#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Created on Fri Nov  7 23:29:08 2025

@author: garvinyim

This package contains a functions to scrape data from the JBCA Glitch Catalogue
(http://www.jb.man.ac.uk/~pulsar/glitches/gTable.html) and the ATNF Glitch and 
Pulsar Catalogues (https://www.atnf.csiro.au/research/pulsar/psrcat/download.html). 
It will return the resultant tables as Pandas DataFrame which can then be 
further processed.

"""

### PREAMBLE ###

import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import tarfile
import re

### FUNCTIONS ###

def read_JBCA_glitch_catalogue(show_table = False):
    
    url = "http://www.jb.man.ac.uk/~pulsar/glitches/gTable.html"
    
    page_html = requests.get(url).text
    soup = BeautifulSoup(page_html, 'html.parser')
    
    table_rows = soup.find_all('tr') # Contains all the table's rows
    
    ## Extracting the header names
    header_row = table_rows[3] # Pulling out the header row
    headings = [] # To hold the names of each column
    for column in header_row:
        if column.string != None and column.string != '\n':
            column_name = column.string
            headings.append(column_name)
    
    ## Remove the first 4 table rows (part of the header) leaving the main table
    table_rows = table_rows[5:]

    ## Main table
    main_table = []
    for row in table_rows:
        row = row.find_all('font')
        row_info = []
        if row[1].text == None or row[1].text == '': # Testing if the row begins with a pulsar name or not
            break
        else:
            for i, column in enumerate(row):
                if i != 0: # Not saving the table index
                    row_info.append(column.text)    
            main_table.append(row_info)
        
    ## Creating a Pandas Dataframe to hold glitch table
    glitch_table = pd.DataFrame(main_table, columns = headings)
    
    if show_table == True:
        print(glitch_table)
            
    return glitch_table

def download_ATNF_catalogues():
    
    url = "https://www.atnf.csiro.au/research/pulsar/psrcat/download.html"
    
    page_html = requests.get(url).text
    soup = BeautifulSoup(page_html, 'html.parser')
    
    ## Getting the URL to download the data
    first_link = soup.a # This contains the most up-to-date ATNF catalogues
    route = '/../' + first_link['href']
    url_for_download = url + route # URL used to download the ATNF files
    
    ## Saving the .tar.gz file locally
    path = os.path.join(os.getcwd(),'psrcat_pkg.tar.gz') # Files will be saved under this name
    r = requests.get(url_for_download)
    with open(path,'wb') as f:
        f.write(r.content)
        
    return None    

def extract_errors(line_list):
    '''
    Parameters
    ----------
    line_list : List representing one row of data in the ATNF catalogue, with 
    each element being a string. Some elements should be numeric and hence, they
    have an error associated. The errors are expressed in bracket notation, so
    the errors are in the last few significant digits. For example:
        '200(5)' ---> 200 +/- 5
        '0.10(2)' ---> 0.10 +/- 0.02
        '24.0(55)' ---> 24.0 +/- 5.5

    Returns
    -------
    new_line : A processed list that now has columns to hold the errors. If there
    are no associated errors, the element is saved as '-'. All the elements are
    saved as strings. Relevant quantities should be later processed into floats.
    '''
    new_line = []
    for i, entry in enumerate(line_list):
        split_entry = entry.split('(') # This does nothing if there is no open bracket
        new_line.append(split_entry[0])
        if len(split_entry) > 1: # If the entry was split, there will be 2 elements
            split_entry[1] = split_entry[1].replace(')', '') # Remove the closing bracket
            ## Counting the number of decimal places
            decimal_places = 0
            if re.search(r'\.', split_entry[0]): # If there is a decimal point
                m = re.search(r'\.', split_entry[0])
                decimal_places = len(split_entry[0]) - m.end()
                multiplier = pow(10, decimal_places)
                split_entry[1] = float(split_entry[1]) / multiplier
            new_line.append(str(split_entry[1]))
        else:
            if i > 1 and i < 7: # Does not create error column for first two and last columns
                new_line.append('-')
    
    return new_line

def read_ATNF_glitch_catalogue(show_table = False):
    
    path = os.path.join(os.getcwd(),'psrcat_pkg.tar.gz') # Path where .tar.gz is
    db_name = 'glitch.db'
    file_path = 'psrcat_tar/' + db_name # Path inside .tar.gz folder where file is
    
    ## Opening .tar.gz folder to save glitch.db locally   
    with tarfile.open(path, 'r:gz') as tar:
        for member in tar.getmembers():
            f = tar.extractfile(member)
            if f is not None and f.name == file_path:
                content = f.read().decode('utf-8') # Need to decode to convert from bytes to text
                with open(db_name, 'w') as db_file:
                    db_file.write(content)
                    
    ## Splitting the text file by newline characters
    split_by_newline = content.split('\n')
    
    ## Dealing with the header
    header = ['Name', 'J2000 Name', 'Glitch Epoch', '+/-', 'dF_F', '+/-', 'dF1_F1', '+/-', 'Q', '+/-', 'T_d', '+/-', 'Ref.']
    split_by_newline = split_by_newline[3:] # Getting rid of the header

    ## Main table
    main_table = []  
    for line in split_by_newline:
        ## If line not an empty string
        if line.strip(): # This strips away any whitespace at the start and end
            line_list = line.split()
            new_line = extract_errors(line_list)
            main_table.append(new_line)
    
    ## Creating a Pandas Dataframe to hold glitch table
    glitch_table = pd.DataFrame(main_table, columns = header)
      
    if show_table == True:
        print(glitch_table)
           
    return glitch_table

def read_ATNF_pulsar_catalogue(show_table = False):
    
    path = os.path.join(os.getcwd(),'psrcat_pkg.tar.gz') # Path where .tar.gz is
    db_name = 'psrcat.db'
    file_path = 'psrcat_tar/' + db_name # Path inside .tar.gz folder where file is
    
    ## Opening .tar.gz folder to save glitch.db locally   
    with tarfile.open(path, 'r:gz') as tar:
        for member in tar.getmembers():
            f = tar.extractfile(member)
            if f is not None and f.name == file_path:
                content = f.read().decode('utf-8') # Need to decode to convert from bytes to text
                with open(db_name, 'w') as db_file:
                    db_file.write(content)
    
    ## Splitting the text file by newline characters
    split_by_newline = content.split('\n')
    
    ## Extracting the names of pulsar features (162 in total, but only 69 shown on website)
    features = []
    for line in split_by_newline:
        if line != '' and line[0] != '@' and line[0] != '#':
            split_line = line.split()
            features.append(split_line[0])
    features = sorted(set(features))
    
    
    ## Create a dictionary to hold data for each pulsar
    pulsar = dict.fromkeys(features) # Creates a valueless dictionary with 'features' as keys 
    
    ## Create a list to hold pulsar dictionaries
    list_of_pulsars = []
    
    for line in split_by_newline:
        if line != '' and line[0] != '#':
            if line[0] == '@': # An @ character symbolises the start of the next pulsar
                list_of_pulsars.append(pulsar)
                pulsar = dict.fromkeys(features) # Reset the dictionary
            else: # Still collecting data for the same pulsar
                split_line = line.split()
                key = split_line[0]
                value = split_line[1]
                pulsar[key] = value
    
    ## Creating a Pandas Dataframe to hold pulsar table
    pulsar_table = pd.DataFrame(list_of_pulsars)
    
    if show_table == True:
        print(pulsar_table)
        
    return pulsar_table


### MAIN ###

if __name__ == "__main__": # This ensures the below code only runs when used as a script and not as a module (being used in another program)

    read_JBCA_glitch_catalogue(show_table = True)
    
    ## Download the ATNF files if they are not in the current working directory
    if not os.path.exists('psrcat_pkg.tar.gz'):
        download_ATNF_catalogues()
        
    read_ATNF_glitch_catalogue(show_table = True)
    
    read_ATNF_pulsar_catalogue(show_table = True)
