#!/bin/bash

sudo apt-get update
sudo apt-get install -y docker.io

sudo docker run -p 5433:5433 -p 5444:5444 --volume $(pwd):/workdir --mount type=volume,source=vertica-data,target=/data --name vertica_ce vertica/vertica-ce

sudo docker exec vertica_ce /opt/vertica/bin/vsql -U dbadmin -c "$(cat create.sql)"

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
gzip -d hits.tsv.gz

time sudo docker exec vertica_ce /opt/vertica/bin/vsql -U dbadmin -c "COPY hits FROM LOCAL '/workdir/hits.tsv' DELIMITER E'\\t' NULL E'\\001' DIRECT"
