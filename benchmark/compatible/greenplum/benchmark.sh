#!/bin/bash

# NOTE: it requires Ubuntu 18.04
# Greenplum does not install on any newer system.

sudo apt update
sudo apt install -y software-properties-common
sudo add-apt-repository ppa:greenplum/db
sudo apt update
sudo apt install greenplum-db-6
source /opt/greenplum-db-*.0/greenplum_path.sh
cp $GPHOME/docs/cli_help/gpconfigs/gpinitsystem_singlenode .
echo localhost > ./hostlist_singlenode
sed -i "s/MASTER_HOSTNAME=[a-z_]*/MASTER_HOSTNAME=$(hostname)/" gpinitsystem_singlenode
sudo mkdir /gpmaster
sudo chmod 777 /gpmaster
gpinitsystem -c gpinitsystem_singlenode
