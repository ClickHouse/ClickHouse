#!/bin/bash

sudo apt-get update
sudo apt-get install -y docker.io

sudo docker run --name vertica -p 5433:5433 -p 5444:5444 --mount type=volume,source=vertica-data,target=/data --name vertica_ce vertica/vertica-ce

sudo docker exec vertica /opt/vertica/bin/vsql -U dbadmin -c "$(cat create.sql)"

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'
gzip -d hits.csv.gz

sudo docker cp hits.tsv vertica:/hits.csv

time sudo docker exec 4e9120959a41 /opt/vertica/bin/vsql -U dbadmin -c "COPY hits FROM '/hits.csv'"
