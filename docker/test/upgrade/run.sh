#!/bin/bash
# shellcheck disable=SC2094
# shellcheck disable=SC2086
# shellcheck disable=SC2024

# Avoid overlaps with previous runs
dmesg --clear

set -x

# we mount tests folder from repo to /usr/share
ln -s /usr/share/clickhouse-test/ci/stress.py /usr/bin/stress
ln -s /usr/share/clickhouse-test/clickhouse-test /usr/bin/clickhouse-test
ln -s /usr/share/clickhouse-test/ci/download_release_packages.py /usr/bin/download_release_packages
ln -s /usr/share/clickhouse-test/ci/get_previous_release_tag.py /usr/bin/get_previous_release_tag

# Stress tests and upgrade check uses similar code that was placed
# in a separate bash library. See tests/ci/stress_tests.lib
source /usr/share/clickhouse-test/ci/attach_gdb.lib
source /usr/share/clickhouse-test/ci/stress_tests.lib

azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --debug /azurite_log &
./setup_minio.sh stateless # to have a proper environment

echo "Get previous release tag"
previous_release_tag=$(dpkg --info package_folder/clickhouse-client*.deb | grep "Version: " | awk '{print $2}' | cut -f1 -d'+' | get_previous_release_tag)
echo $previous_release_tag

echo "Clone previous release repository"
git clone https://github.com/ClickHouse/ClickHouse.git --no-tags --progress --branch=$previous_release_tag --no-recurse-submodules --depth=1 previous_release_repository

echo "Download clickhouse-server from the previous release"
mkdir previous_release_package_folder

echo $previous_release_tag | download_release_packages && echo -e "Download script exit code$OK" >> /test_output/test_results.tsv \
    || echo -e "Download script failed$FAIL" >> /test_output/test_results.tsv

# Check if we cloned previous release repository successfully
if ! [ "$(ls -A previous_release_repository/tests/queries)" ]
then
    echo -e 'failure\tFailed to clone previous release tests' > /test_output/check_status.tsv
    exit
elif ! [ "$(ls -A previous_release_package_folder/clickhouse-common-static_*.deb && ls -A previous_release_package_folder/clickhouse-server_*.deb)" ]
then
    echo -e 'failure\tFailed to download previous release packages' > /test_output/check_status.tsv
    exit
fi

echo -e "Successfully cloned previous release tests$OK" >> /test_output/test_results.tsv
echo -e "Successfully downloaded previous release packages$OK" >> /test_output/test_results.tsv

# Make upgrade check more funny by forcing Ordinary engine for system database
mkdir -p /var/lib/clickhouse/metadata
echo "ATTACH DATABASE system ENGINE=Ordinary" > /var/lib/clickhouse/metadata/system.sql

# Install previous release packages
install_packages previous_release_package_folder

# Initial run without S3 to create system.*_log on local file system to make it
# available for dump via clickhouse-local
configure

# it contains some new settings, but we can safely remove it
rm /etc/clickhouse-server/config.d/merge_tree.xml
rm /etc/clickhouse-server/config.d/enable_wait_for_shutdown_replicated_tables.xml
rm /etc/clickhouse-server/users.d/nonconst_timezone.xml

start
stop
mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.initial.log

# Start server from previous release
# Let's enable S3 storage by default
export USE_S3_STORAGE_FOR_MERGE_TREE=1
# Previous version may not be ready for fault injections
export ZOOKEEPER_FAULT_INJECTION=0
configure

# force_sync=false doesn't work correctly on some older versions
sudo cat /etc/clickhouse-server/config.d/keeper_port.xml \
  | sed "s|<force_sync>false</force_sync>|<force_sync>true</force_sync>|" \
  > /etc/clickhouse-server/config.d/keeper_port.xml.tmp
sudo mv /etc/clickhouse-server/config.d/keeper_port.xml.tmp /etc/clickhouse-server/config.d/keeper_port.xml

# But we still need default disk because some tables loaded only into it
sudo cat /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml \
  | sed "s|<main><disk>s3</disk></main>|<main><disk>s3</disk></main><default><disk>default</disk></default>|" \
  > /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml.tmp
mv /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml.tmp /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml
sudo chown clickhouse /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml
sudo chgrp clickhouse /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml

# it contains some new settings, but we can safely remove it
rm /etc/clickhouse-server/config.d/merge_tree.xml
rm /etc/clickhouse-server/config.d/enable_wait_for_shutdown_replicated_tables.xml
rm /etc/clickhouse-server/users.d/nonconst_timezone.xml

start

clickhouse-client --query="SELECT 'Server version: ', version()"

mkdir tmp_stress_output

stress --test-cmd="/usr/bin/clickhouse-test --queries=\"previous_release_repository/tests/queries\""  --upgrade-check --output-folder tmp_stress_output --global-time-limit=1200 \
    && echo -e "Test script exit code$OK" >> /test_output/test_results.tsv \
    || echo -e "Test script failed$FAIL script exit code: $?" >> /test_output/test_results.tsv

rm -rf tmp_stress_output

# We experienced deadlocks in this command in very rare cases. Let's debug it:
timeout 10m clickhouse-client --query="SELECT 'Tables count:', count() FROM system.tables" ||
(
    echo "thread apply all backtrace (on select tables count)" >> /test_output/gdb.log
    timeout 30m gdb -batch -ex 'thread apply all backtrace' -p "$(cat /var/run/clickhouse-server/clickhouse-server.pid)" | ts '%Y-%m-%d %H:%M:%S' >> /test_output/gdb.log
    clickhouse stop --force
)

# Use bigger timeout for previous version and disable additional hang check
stop 300 false
mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.stress.log

# Install and start new server
install_packages package_folder
export ZOOKEEPER_FAULT_INJECTION=1
configure

# Just in case previous version left some garbage in zk
sudo cat /etc/clickhouse-server/config.d/lost_forever_check.xml \
  | sed "s|>1<|>0<|g" \
  > /etc/clickhouse-server/config.d/lost_forever_check.xml.tmp
sudo mv /etc/clickhouse-server/config.d/lost_forever_check.xml.tmp /etc/clickhouse-server/config.d/lost_forever_check.xml
rm /etc/clickhouse-server/config.d/filesystem_caches_path.xml

start 500
clickhouse-client --query "SELECT 'Server successfully started', 'OK', NULL, ''" >> /test_output/test_results.tsv \
    || (rg --text "<Error>.*Application" /var/log/clickhouse-server/clickhouse-server.log > /test_output/application_errors.txt \
    && echo -e "Server failed to start (see application_errors.txt and clickhouse-server.clean.log)$FAIL$(trim_server_logs application_errors.txt)" \
    >> /test_output/test_results.tsv)

# Remove file application_errors.txt if it's empty
[ -s /test_output/application_errors.txt ] || rm /test_output/application_errors.txt

clickhouse-client --query="SELECT 'Server version: ', version()"

# Let the server run for a while before checking log.
sleep 60

stop
mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.upgrade.log

# Error messages (we should ignore some errors)
# FIXME https://github.com/ClickHouse/ClickHouse/issues/38643 ("Unknown index: idx.")
# FIXME https://github.com/ClickHouse/ClickHouse/issues/39174 ("Cannot parse string 'Hello' as UInt64")
# FIXME Not sure if it's expected, but some tests from stress test may not be finished yet when we restarting server.
#       Let's just ignore all errors from queries ("} <Error> TCPHandler: Code:", "} <Error> executeQuery: Code:")
# FIXME https://github.com/ClickHouse/ClickHouse/issues/39197 ("Missing columns: 'v3' while processing query: 'v3, k, v1, v2, p'")
# FIXME https://github.com/ClickHouse/ClickHouse/issues/39174 - bad mutation does not indicate backward incompatibility
echo "Check for Error messages in server log:"
rg -Fav -e "Code: 236. DB::Exception: Cancelled merging parts" \
           -e "Code: 236. DB::Exception: Cancelled mutating parts" \
           -e "REPLICA_IS_ALREADY_ACTIVE" \
           -e "REPLICA_ALREADY_EXISTS" \
           -e "ALL_REPLICAS_LOST" \
           -e "DDLWorker: Cannot parse DDL task query" \
           -e "RaftInstance: failed to accept a rpc connection due to error 125" \
           -e "UNKNOWN_DATABASE" \
           -e "NETWORK_ERROR" \
           -e "UNKNOWN_TABLE" \
           -e "ZooKeeperClient" \
           -e "KEEPER_EXCEPTION" \
           -e "DirectoryMonitor" \
           -e "TABLE_IS_READ_ONLY" \
           -e "Code: 1000, e.code() = 111, Connection refused" \
           -e "UNFINISHED" \
           -e "NETLINK_ERROR" \
           -e "Renaming unexpected part" \
           -e "PART_IS_TEMPORARILY_LOCKED" \
           -e "and a merge is impossible: we didn't find" \
           -e "found in queue and some source parts for it was lost" \
           -e "is lost forever." \
           -e "Unknown index: idx." \
           -e "Cannot parse string 'Hello' as UInt64" \
           -e "} <Error> TCPHandler: Code:" \
           -e "} <Error> executeQuery: Code:" \
           -e "Missing columns: 'v3' while processing query: 'v3, k, v1, v2, p'" \
           -e "The set of parts restored in place of" \
           -e "(ReplicatedMergeTreeAttachThread): Initialization failed. Error" \
           -e "Code: 269. DB::Exception: Destination table is myself" \
           -e "Coordination::Exception: Connection loss" \
           -e "MutateFromLogEntryTask" \
           -e "No connection to ZooKeeper, cannot get shared table ID" \
           -e "Session expired" \
           -e "TOO_MANY_PARTS" \
           -e "Authentication failed" \
           -e "Cannot flush" \
           -e "Container already exists" \
           -e "doesn't have metadata version on disk" \
    clickhouse-server.upgrade.log \
    | grep -av -e "_repl_01111_.*Mapping for table with UUID" \
    | zgrep -Fa "<Error>" > /test_output/upgrade_error_messages.txt \
    && echo -e "Error message in clickhouse-server.log (see upgrade_error_messages.txt)$FAIL$(head_escaped /test_output/upgrade_error_messages.txt)" \
        >> /test_output/test_results.tsv \
    || echo -e "No Error messages after server upgrade$OK" >> /test_output/test_results.tsv

# Remove file upgrade_error_messages.txt if it's empty
[ -s /test_output/upgrade_error_messages.txt ] || rm /test_output/upgrade_error_messages.txt

# Grep logs for sanitizer asserts, crashes and other critical errors
check_logs_for_critical_errors

tar -chf /test_output/coordination.tar /var/lib/clickhouse/coordination ||:

collect_query_and_trace_logs

mv /var/log/clickhouse-server/stderr.log /test_output/

# Write check result into check_status.tsv
# Try to choose most specific error for the whole check status
clickhouse-local --structure "test String, res String, time Nullable(Float32), desc String" -q "SELECT 'failure', test FROM table WHERE res != 'OK' order by
(test like '%Sanitizer%') DESC,
(test like '%Killed by signal%') DESC,
(test like '%gdb.log%') DESC,
(test ilike '%possible deadlock%') DESC,
(test like '%start%') DESC,
(test like '%dmesg%') DESC,
(test like '%OOM%') DESC,
(test like '%Signal 9%') DESC,
(test like '%Fatal message%') DESC,
(test like '%Error message%') DESC,
(test like '%previous release%') DESC,
rowNumberInAllBlocks()
LIMIT 1" < /test_output/test_results.tsv > /test_output/check_status.tsv || echo "failure\tCannot parse test_results.tsv" > /test_output/check_status.tsv
[ -s /test_output/check_status.tsv ] || echo -e "success\tNo errors found" > /test_output/check_status.tsv

# But OOMs in stress test are allowed
if rg 'OOM in dmesg|Signal 9' /test_output/check_status.tsv
then
    sed -i 's/failure/success/' /test_output/check_status.tsv
fi

collect_core_dumps
