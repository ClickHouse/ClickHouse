#!/bin/bash

# Install

echo "deb https://dev.monetdb.org/downloads/deb/ $(lsb_release -cs) monetdb" | sudo tee /etc/apt/sources.list.d/monetdb.list

sudo wget --output-document=/etc/apt/trusted.gpg.d/monetdb.gpg https://www.monetdb.org/downloads/MonetDB-GPG-KEY.gpg
sudo apt-get update
sudo apt-get install monetdb5-sql monetdb-client

sudo monetdbd create /var/lib/monetdb
sudo monetdbd start /var/lib/monetdb

sudo monetdb create test
sudo monetdb release test
