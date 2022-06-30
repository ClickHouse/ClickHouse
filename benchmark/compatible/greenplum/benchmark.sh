#!/bin/bash

# NOTE: it requires Ubuntu 18.04
# Greenplum does not install on any newer system.

echo "This script must be run from gpadmin user. Press enter to continue."
read
sudo apt update
sudo apt install -y software-properties-common
sudo add-apt-repository ppa:greenplum/db
sudo apt update
sudo apt install greenplum-db-6
sudo rm -rf /gpmaster /gpdata*
ssh-keygen -t rsa -b 4096
cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys
mod 600 ~/.ssh/authorized_keys
sudo echo "# kernel.shmall = _PHYS_PAGES / 2 # See Shared Memory Pages
kernel.shmall = 197951838
# kernel.shmmax = kernel.shmall * PAGE_SIZE 
kernel.shmmax = 810810728448
kernel.shmmni = 4096
vm.overcommit_memory = 2 # See Segment Host Memory
vm.overcommit_ratio = 95 # See Segment Host Memory

net.ipv4.ip_local_port_range = 10000 65535 # See Port Settings
kernel.sem = 500 2048000 200 4096
kernel.sysrq = 1
kernel.core_uses_pid = 1
kernel.msgmnb = 65536
kernel.msgmax = 65536
kernel.msgmni = 2048
net.ipv4.tcp_syncookies = 1
net.ipv4.conf.default.accept_source_route = 0
net.ipv4.tcp_max_syn_backlog = 4096
net.ipv4.conf.all.arp_filter = 1
net.core.netdev_max_backlog = 10000
net.core.rmem_max = 2097152
net.core.wmem_max = 2097152
vm.swappiness = 10
vm.zone_reclaim_mode = 0
vm.dirty_expire_centisecs = 500
vm.dirty_writeback_centisecs = 100
vm.dirty_background_ratio = 0 # See System Memory
vm.dirty_ratio = 0
vm.dirty_background_bytes = 1610612736
vm.dirty_bytes = 4294967296" |sudo tee -a  /etc/sysctl.conf
sudo sysctl -p

echo "* soft nofile 524288
* hard nofile 524288
* soft nproc 131072
* hard nproc 131072" |sudo tee -a  /etc/security/limits.conf
echo "RemoveIPC=no" |sudo tee -a  /etc/systemd/logind.conf
echo "Now you need to reboot the machine. Press Enter if you already rebooted, or reboot now and run the script once again"
read
source /opt/greenplum-db-*.0/greenplum_path.sh
cp $GPHOME/docs/cli_help/gpconfigs/gpinitsystem_singlenode .
echo localhost > ./hostlist_singlenode
sed -i "s/MASTER_HOSTNAME=[a-z_]*/MASTER_HOSTNAME=$(hostname)/" gpinitsystem_singlenode
sed -i "s@declare -a DATA_DIRECTORY=(/gpdata1 /gpdata2)@declare -a DATA_DIRECTORY=(/gpdata1 /gpdata2 /gpdata3 /gpdata4 /gpdata5 /gpdata6 /gpdata7 /gpdata8 /gpdata9 /gpdata10 /gpdata11 /gpdata12 /gpdata13 /gpdata14)@" gpinitsystem_singlenode
sudo mkdir /gpmaster /gpdata1 /gpdata2 /gpdata3 /gpdata4 /gpdata5 /gpdata6 /gpdata7 /gpdata8 /gpdata9 /gpdata10 /gpdata11 /gpdata12 /gpdata13 /gpdata14
sudo chmod 777 /gpmaster /gpdata1 /gpdata2 /gpdata3 /gpdata4 /gpdata5 /gpdata6 /gpdata7 /gpdata8 /gpdata9 /gpdata10 /gpdata11 /gpdata12 /gpdata13 /gpdata14
gpinitsystem -ac gpinitsystem_singlenode
export MASTER_DATA_DIRECTORY=/gpmaster/gpsne-1/
#wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
#gzip -d hits.tsv.gz
chmod 777 ~ hits.tsv
psql -d postgres -f create.sql
nohup gpfdist &
time psql -d postgres -t -c '\timing' -c "insert into hits select * from hits_ext;"
du -sh /gpdata*
./run.sh 2>&1 | tee log.txt
cat log.txt | grep -oP 'Time: \d+\.\d+ ms' | sed -r -e 's/Time: ([0-9]+\.[0-9]+) ms/\1/' |awk '{ if (i % 3 == 0) { printf "[" }; printf $1 / 1000; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'
