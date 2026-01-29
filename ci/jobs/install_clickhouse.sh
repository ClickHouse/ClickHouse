#!/bin/bash

set -eo pipefail
set -vx

ch_path=/usr/bin
tz=Europe/Berlin
config_install_opts=--fast-test

cd $(dirname "$0")/../..

/usr/bin/clickhouse-server stop ||:
pkill -9 clickhouse ||:
rm -rf /var/lib/clickhouse/
find /var/log -type f -delete

apt-get update
apt-get install -y curl git python3 python3-jinja2 python3-numpy python3-pandas expect brotli bzip2

curl -o /usr/bin/clickhouse -z /usr/bin/clickhouse https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/amd64/clickhouse

rm -rf /etc/clickhouse-client/* /etc/clickhouse-server/*
# google *.proto files
mkdir -p /usr/share/clickhouse/ && ln -sf /usr/local/include /usr/share/clickhouse/protos
ln -sf "$ch_path"/clickhouse "$ch_path"/clickhouse-server
ln -sf "$ch_path"/clickhouse "$ch_path"/clickhouse-client
ln -sf "$ch_path"/clickhouse "$ch_path"/clickhouse-compressor
ln -sf "$ch_path"/clickhouse "$ch_path"/clickhouse-local
ln -sf "$ch_path"/clickhouse "$ch_path"/clickhouse-disks
ln -sf "$ch_path"/clickhouse "$ch_path"/clickhouse-obfuscator
ln -sf "$ch_path"/clickhouse "$ch_path"/clickhouse-format
ln -sf "$ch_path"/clickhouse "$ch_path"/ch
ln -sf /usr/bin/clickhouse-odbc-bridge "$ch_path"/clickhouse-odbc-bridge
cp programs/server/config.xml programs/server/users.xml /etc/clickhouse-server/
./tests/config/install.sh /etc/clickhouse-server /etc/clickhouse-client "$config_install_opts"

clickhouse-server --version
sed -i 's|>/test/chroot|>ci/tmp/chroot|' /etc/clickhouse-server**/config.d/*.xml

cp /usr/share/zoneinfo/$tz /etc/localtime
sudo clickhouse-server --config-file /etc/clickhouse-server/config.xml start 2> server.log &
disown
