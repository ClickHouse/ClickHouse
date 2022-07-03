#!/bin/bash

wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg
sudo apt-get update && sudo apt-get install -y apt-transport-https
echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list
sudo apt-get update && sudo apt-get install -y elasticsearch

sudo systemctl start elasticsearch.service
sudo /usr/share/elasticsearch/bin/elasticsearch-reset-password -u elastic

# Example:
# User: elastic
# Password: C0Qq9kNYMUunKTXMDOUZ

export PASSWORD='...'

curl -k -XGET 'https://localhost:9200' -u "elastic:${PASSWORD}"

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.json.gz'
gzip -d hits.json.gz

# Prevent 'curl' from OOM.

split -l 1000000000 hits.json hits_
for table in hits_*; do mv ${table} ${table}.json; done

time for table in hits_*; do curl -k -H "Transfer-Encoding: chunked" -XPOST -u "elastic:${PASSWORD}" 'https://localhost:9200/_bulk' -T ${table}; done
