#!/bin/bash

# Install

sudo apt-get update
sudo apt install gnupg postgresql-common apt-transport-https lsb-release wget
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
sudo bash -c 'echo "deb https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -c -s) main" > /etc/apt/sources.list.d/timescaledb.list'
wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | sudo apt-key add -
sudo apt-get update
sudo apt install timescaledb-2-postgresql-14
sudo systemctl restart postgresql

sudo su postgres -c psql -c "CREATE DATABASE test"
sudo su postgres -c psql test -c "CREATE EXTENSION IF NOT EXISTS timescaledb"
