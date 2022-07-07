#!/bin/bash

sudo apt-get update
sudo apt install openjdk-11-jdk jq -y
sudo update-alternatives --config java

# Install

PINOT_VERSION=0.10.0

wget https://downloads.apache.org/pinot/apache-pinot-$PINOT_VERSION/apache-pinot-$PINOT_VERSION-bin.tar.gz
tar -zxvf apache-pinot-$PINOT_VERSION-bin.tar.gz

./apache-pinot-$PINOT_VERSION-bin/bin/pinot-admin.sh QuickStart -type batch &
sleep 30
./apache-pinot-$PINOT_VERSION-bin/bin/pinot-admin.sh AddTable -tableConfigFile offline_table.json -schemaFile schema.json -exec

# Load the data

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz

# Pinot was unable to load data as a single file wihout any errors returned. We have to split the data
split -d --additional-suffix .tsv --verbose -n l/100 hits.tsv parts

# Pinot can't load value '"tatuirovarki_redmond' so we need to fix this row to make it work
sed parts93.tsv -e 's "tatuirovarki_redmond tatuirovarki_redmond g' -i

# Fix path to local directory
sed splitted.yaml 's PWD_DIR_PLACEHOLDER '$PWD' g' -i
sed local.yaml 's PWD_DIR_PLACEHOLDER '$PWD' g' -i

# Load data
./apache-pinot-$PINOT_VERSION-bin/bin/pinot-admin.sh LaunchDataIngestionJob -jobSpecFile splitted.yaml

# After upload it shows 94465149 rows instead of 99997497 in the dataset

# Run the queries
./run.sh

# stop Druid services
kill %1

du -bcs ./batch
