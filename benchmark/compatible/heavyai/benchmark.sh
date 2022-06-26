#!/bin/bash

# Install

sudo apt update
sudo apt install default-jre-headless
sudo apt install apt-transport-https
sudo useradd -U -m heavyai
sudo curl https://releases.heavy.ai/GPG-KEY-heavyai | sudo apt-key add -
echo "deb https://releases.heavy.ai/os/apt/ stable cpu" | sudo tee /etc/apt/sources.list.d/heavyai.list
sudo apt update
sudo apt install heavyai

export HEAVYAI_USER=heavyai
export HEAVYAI_GROUP=heavyai
export HEAVYAI_STORAGE=/var/lib/heavyai
export HEAVYAI_PATH=/opt/heavyai
export HEAVYAI_LOG=/var/lib/heavyai/data/mapd_log

cd $HEAVYAI_PATH/systemd
./install_heavy_systemd.sh

# Press Enter multiple times.

sudo systemctl start heavydb
sudo systemctl enable heavydb

# Load the data

wget 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'
gzip -d hits.csv.gz
chmod 777 ~ hits.csv

sudo bash -c "echo 'allowed-import-paths = [\"/home/ubuntu/\"]' > /var/lib/heavyai/heavy.conf_"
sudo bash -c "cat /var/lib/heavyai/heavy.conf >> /var/lib/heavyai/heavy.conf_"
sudo bash -c "mv /var/lib/heavyai/heavy.conf_ /var/lib/heavyai/heavy.conf && chown heavyai /var/lib/heavyai/heavy.conf"
sudo systemctl restart heavydb

/opt/heavyai/bin/heavysql -t -p HyperInteractive < create.sql
time /opt/heavyai/bin/heavysql -t -p HyperInteractive <<< "COPY hits FROM '$(pwd)/hits.csv' WITH (HEADER = 'false');"

# Loaded: 99997497 recs, Rejected: 0 recs in 572.633000 secs

./run.sh 2>&1 | tee log.txt

du -bcs /var/lib/heavyai/

cat log.txt | grep -P 'Total time|null' | sed -r -e 's/^.*Total time: ([0-9]+) ms$/\1/' | awk '{ if ($1 == "null") { print } else { print $1 / 1000 } }' |
  awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'
