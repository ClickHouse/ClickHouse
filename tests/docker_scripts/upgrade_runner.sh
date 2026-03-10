#!/bin/bash
# shellcheck disable=SC2094
# shellcheck disable=SC2086
# shellcheck disable=SC2024

# Avoid overlaps with previous runs
dmesg --clear

set -ex

# we mount tests folder from repo to /usr/share
ln -s /repo/ci/jobs/scripts/stress/stress.py /usr/bin/stress
ln -s /repo/tests/clickhouse-test /usr/bin/clickhouse-test
ln -s /repo/tests/ci/download_release_packages.py /usr/bin/download_release_packages
ln -s /repo/tests/ci/get_previous_release_tag.py /usr/bin/get_previous_release_tag

# Stress tests and upgrade check uses similar code that was placed
# in a separate bash library. See tests/ci/stress_tests.lib
# shellcheck source=../stateless/attach_gdb.lib
source /repo/tests/docker_scripts/attach_gdb.lib
# shellcheck source=../stateless/stress_tests.lib
source /repo/tests/docker_scripts/stress_tests.lib

azurite-rs --host 0.0.0.0 --blob-port 10000 --debug > /azurite_log 2>&1 &
cd /repo && python3 /repo/ci/jobs/scripts/clickhouse_proc.py start_minio stateless || ( echo "Failed to start minio" && exit 1 ) # to have a proper environment

echo "Get previous release tag"
PACKAGES_DIR=/repo/ci/tmp
# shellcheck disable=SC2016
previous_release_tag=$(dpkg-deb --showformat='${Version}' --show $PACKAGES_DIR/clickhouse-client*.deb | get_previous_release_tag)
if [ $? -ne 0 ]; then
    echo "Failed to get previous release tag"
    exit 1
fi
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
    exit 1
elif ! [ "$(ls -A previous_release_package_folder/clickhouse-common-static_*.deb && ls -A previous_release_package_folder/clickhouse-server_*.deb)" ]
then
    echo -e 'failure\tFailed to download previous release packages' > /test_output/check_status.tsv
    exit 1
fi

echo -e "Successfully cloned previous release tests$OK" >> /test_output/test_results.tsv
echo -e "Successfully downloaded previous release packages$OK" >> /test_output/test_results.tsv

# Make upgrade check more funny by forcing Ordinary engine for system database
mkdir -p /var/lib/clickhouse/metadata
echo "ATTACH DATABASE system ENGINE=Ordinary" > /var/lib/clickhouse/metadata/system.sql

# Install previous release packages
install_packages previous_release_package_folder

# NOTE: we need to run clickhouse-local under script to get settings without any adjustments, like clickhouse-local does in case of stdout is not a tty
function save_settings_clean()
{
  local out=$1 && shift
  script -q -c "clickhouse-local --implicit-select 0 -q \"select * from system.settings into outfile '$out'\"" --log-out /dev/null
}

function save_mergetree_settings_clean()
{
  local out=$1 && shift
  script -q -c "clickhouse-local --implicit-select 0 -q \"select * from system.merge_tree_settings into outfile '$out'\"" --log-out /dev/null
}

# We save the (numeric) version of the old server to compare setting changes between the 2
# We do this since we are testing against the latest release, not taking into account release candidates, so we might
# be testing current master (24.6) against the latest stable release (24.4)
function save_major_version()
{
  local out=$1 && shift
  clickhouse-local -q "SELECT a[1]::UInt64 * 100 + a[2]::UInt64 as v FROM (Select splitByChar('.', version()) as a) into outfile '$out'"
}

save_settings_clean 'old_settings.native'
save_mergetree_settings_clean 'old_merge_tree_settings.native'
save_major_version 'old_version.native'
old_major_version=$(clickhouse-local -q "select a[1] || '.' || a[2] from (select splitByChar('.', version()) as a)")

configure_opts=(
    # Let's enable S3 storage by default
    --s3-storage
)
if [ $((RANDOM % 2)) -eq 0 ]; then
    configure_opts+=(--encrypted-storage)
fi

# Start server from previous release
configure "${configure_opts[@]}"

# But we still need default disk because some tables loaded only into it
sudo sed -i "s|<main><disk>s3</disk></main>|<main><disk>s3</disk></main><default><disk>default</disk></default>|" /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml
sudo chown clickhouse /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml
sudo chgrp clickhouse /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml

start_server || (echo "Failed to start server" && exit 1)

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
stop_server 300 false || (echo "Failed to stop server" && exit 1)
mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.stress.log

# Install and start new server
install_packages $PACKAGES_DIR
configure "${configure_opts[@]}"

# Check that all new/changed setting were added in settings changes history.
# Some settings can be different for builds with sanitizers, so we check
# Also the automatic value of 'max_threads' and similar was displayed as "'auto(...)'" in previous versions instead of "auto(...)".
# settings changes only for non-sanitizer builds.
IS_SANITIZED=$(clickhouse-local --query "SELECT value LIKE '%-fsanitize=%' FROM system.build_options WHERE name = 'CXX_FLAGS'")
if [ "${IS_SANITIZED}" -eq "0" ]
then
  save_settings_clean 'new_settings.native'
  save_mergetree_settings_clean 'new_merge_tree_settings.native'
  clickhouse-local -nmq "
  CREATE TABLE old_settings AS file('old_settings.native');
  CREATE TABLE old_merge_tree_settings AS file('old_merge_tree_settings.native');
  CREATE TABLE old_version AS file('old_version.native');
  CREATE TABLE new_settings AS file('new_settings.native');
  CREATE TABLE new_merge_tree_settings AS file('new_merge_tree_settings.native');

  SELECT
      name,
      new_settings.value AS new_value,
      old_settings.value AS old_value
  FROM new_settings
  LEFT JOIN old_settings ON new_settings.name = old_settings.name
  WHERE (old_value IS NULL OR new_value != old_value)
      AND (name NOT IN (
      SELECT arrayJoin(tupleElement(changes, 'name'))
      FROM
      (
          SELECT *, splitByChar('.', version) AS version_array FROM system.settings_changes WHERE type = 'Session'
      )
      WHERE (version_array[1]::UInt64 * 100 + version_array[2]::UInt64) > (SELECT v FROM old_version LIMIT 1)
  ))
  SETTINGS join_use_nulls = 1
  INTO OUTFILE 'changed_settings.txt'
  FORMAT PrettyCompactNoEscapes;

  SELECT
      name,
      new_merge_tree_settings.value AS new_value,
      old_merge_tree_settings.value AS old_value
  FROM new_merge_tree_settings
  LEFT JOIN old_merge_tree_settings ON new_merge_tree_settings.name = old_merge_tree_settings.name
  WHERE (old_value IS NULL OR new_value != old_value)
      AND (name NOT IN (
      SELECT arrayJoin(tupleElement(changes, 'name'))
      FROM
      (
          SELECT *, splitByChar('.', version) AS version_array FROM system.settings_changes WHERE type = 'MergeTree'
      )
      WHERE (version_array[1]::UInt64 * 100 + version_array[2]::UInt64) > (SELECT v FROM old_version LIMIT 1)
  ))
  SETTINGS join_use_nulls = 1
  INTO OUTFILE 'changed_merge_tree_settings.txt'
  FORMAT PrettyCompactNoEscapes;

  SELECT name
  FROM new_settings
  WHERE (name NOT IN (
      SELECT name
      FROM old_settings
  )) AND (name NOT IN (
      SELECT arrayJoin(tupleElement(changes, 'name'))
      FROM
      (
          SELECT *, splitByChar('.', version) AS version_array FROM system.settings_changes WHERE type = 'Session'
      )
      WHERE (version_array[1]::UInt64 * 100 + version_array[2]::UInt64) > (SELECT v FROM old_version LIMIT 1)
  ))
  INTO OUTFILE 'new_settings.txt'
  FORMAT PrettyCompactNoEscapes;

  SELECT name
  FROM new_merge_tree_settings
  WHERE (name NOT IN (
      SELECT name
      FROM old_merge_tree_settings
  )) AND (name NOT IN (
      SELECT arrayJoin(tupleElement(changes, 'name'))
      FROM
      (
          SELECT *, splitByChar('.', version) AS version_array FROM system.settings_changes WHERE type = 'MergeTree'
      )
      WHERE (version_array[1]::UInt64 * 100 + version_array[2]::UInt64) > (SELECT v FROM old_version LIMIT 1)
  ))
  INTO OUTFILE 'new_merge_tree_settings.txt'
  FORMAT PrettyCompactNoEscapes;
  "

  if [ -s changed_settings.txt ]
  then
      mv changed_settings.txt /test_output/
      echo -e "Changed settings are not reflected in the settings changes history (see changed_settings.txt)$FAIL$(head_escaped /test_output/changed_settings.txt)" >> /test_output/test_results.tsv
  else
      echo -e "There are no changed settings or they are reflected in settings changes history$OK" >> /test_output/test_results.tsv
  fi

  if [ -s changed_merge_tree_settings.txt ]
  then
      mv changed_merge_tree_settings.txt /test_output/
      echo -e "Changed MergeTree settings are not reflected in the settings changes history (see changed_merge_tree_settings.txt)$FAIL$(head_escaped /test_output/changed_merge_tree_settings.txt)" >> /test_output/test_results.tsv
  else
      echo -e "There are no changed MergeTree settings or they are reflected in settings changes history$OK" >> /test_output/test_results.tsv
  fi

  if [ -s new_settings.txt ]
  then
      mv new_settings.txt /test_output/
      echo -e "New settings are not reflected in settings changes history (see new_settings.txt)$FAIL$(head_escaped /test_output/new_settings.txt)" >> /test_output/test_results.tsv
  else
      echo -e "There are no new settings or they are reflected in settings changes history$OK" >> /test_output/test_results.tsv
  fi

  if [ -s new_merge_tree_settings.txt ]
  then
      mv new_merge_tree_settings.txt /test_output/
      echo -e "New MergeTree settings are not reflected in settings changes history (see new_merge_tree_settings.txt)$FAIL$(head_escaped /test_output/new_merge_tree_settings.txt)" >> /test_output/test_results.tsv
  else
      echo -e "There are no new MergeTree settings or they are reflected in settings changes history$OK" >> /test_output/test_results.tsv
  fi

fi

# Just in case previous version left some garbage in zk
sudo sed -i "s|>1<|>0<|g" /etc/clickhouse-server/config.d/lost_forever_check.xml
rm -f /etc/clickhouse-server/config.d/filesystem_caches_path.xml

# Set compatibility setting to previous version, so we won't fail due to known backward incompatible changes.
echo "<clickhouse>
    <profiles>
        <default>
            <compatibility>$old_major_version</compatibility>
            <!-- Allow loading projections with positional arguments (e.g., GROUP BY 1, 2) created by the old server. -->
            <enable_positional_arguments_for_projections>1</enable_positional_arguments_for_projections>
        </default>
    </profiles>
</clickhouse>" > /etc/clickhouse-server/users.d/compatibility.xml

cat /etc/clickhouse-server/users.d/compatibility.xml

# List of allowed reasons why the server cannot start up
# ADD ENTRIES HERE ONLY IF YOU ARE CERTAIN THEY DO NOT INTRODUCE BACKWARD-INCOMPATIBLE CHANGES
# 1. Lazy database engine has been removed in a backward-incompatible manner
check_allow_list() {
    local log="/var/log/clickhouse-server/clickhouse-server.log"
    if [ -f "$log" ] && rg -q "Unknown database engine: Lazy" "$log"; then
        # cleanup errors
        echo -e "Found allow-listed error in logs. Suppressing failure"$OK > /test_output/test_results.tsv
        # Finishing tests because following checks would fail
        exit 0
    fi
}

start_server || check_allow_list || (echo "Failed to start server" && exit 1)

clickhouse-client --query "SELECT 'Server successfully started', 'OK', NULL, ''" >> /test_output/test_results.tsv \
    || (rg --text "<Error>.*Application" /var/log/clickhouse-server/clickhouse-server.log > /test_output/application_errors.txt \
    && echo -e "Server failed to start (see application_errors.txt and clickhouse-server.clean.log)$FAIL$(trim_server_logs application_errors.txt)" \
    >> /test_output/test_results.tsv)

# Remove file application_errors.txt if it's empty
[ -s /test_output/application_errors.txt ] || rm -f /test_output/application_errors.txt

clickhouse-client --query="SELECT 'Server version: ', version()"

# Let the server run for a while before checking log.
sleep 60

stop_server || (echo "Failed to stop server" && exit 1)
mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.upgrade.log
cp /var/log/clickhouse-server/clickhouse-server.upgrade.log /test_output/clickhouse-server.upgrade.log

# Error messages (we should ignore some errors)
# FIXME https://github.com/ClickHouse/ClickHouse/issues/38643 ("Unknown index: idx.")
# FIXME Not sure if it's expected, but some tests from stress test may not be finished yet when we restarting server.
#       Let's just ignore all errors from queries ("} <Error> TCPHandler: Code:", "} <Error> executeQuery: Code:")
# FIXME https://github.com/ClickHouse/ClickHouse/issues/39197 ("Missing columns: 'v3' while processing query: 'v3, k, v1, v2, p'")
# FIXME https://github.com/ClickHouse/ClickHouse/issues/39174 - bad mutation does not indicate backward incompatibility:
#       stress tests may leave behind intentionally-broken mutations that retry after upgrade.
#       `CANNOT_PARSE_TEXT` errors come from:
#       - 00834_kill_mutation{,_replicated_zookeeper}: `DELETE WHERE toUInt32(s) = 1` on String data ('a', 'b')
#       - 01414_mutations_and_errors_zookeeper: `MODIFY COLUMN value UInt64` on String data ('Hello')
#       `MutateFromLogEntryTask` is also excluded for the same reason, but only catches the first log line;
#       the wrapping `MergeTreeBackgroundExecutor` line also needs to be excluded.
# `NO_SUCH_INTERSERVER_IO_ENDPOINT` is expected during upgrades because replicated tables try to fetch parts
# from replicas that are being restarted and whose interserver endpoints are temporarily unavailable.
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
           -e "DistributedInsertQueue" \
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
           -e "Cannot parse string 'Hello' as UInt32" \
           -e "Cannot parse string \'Hello\' as UInt32" \
           -e "Cannot parse string \\'Hello\\' as UInt32" \
           -e "Cannot parse string \'a\' as UInt32" \
           -e "Cannot parse string \'b\' as UInt32" \
           -e "Cannot parse string 'a' as UInt32" \
           -e "Cannot parse string 'b' as UInt32" \
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
           -e "Unknown codec family: ZSTD_QAT" \
           -e "Unknown codec family: DEFLATE_QPL" \
           -e "Bad get: has String, requested UInt64. (BAD_GET" \
           -e "Disk does not support stat. (NOT_IMPLEMENTED" \
           -e "QUALIFY clause is not supported in the old analyzer" \
           -e "Cannot attach table \`test_7\`" \
           -e "Cannot open file /var/lib/clickhouse/access/" \
           -e "NO_SUCH_INTERSERVER_IO_ENDPOINT" \
           -e "Mapping for table with UUID=1f474183-1403-4282-9309-21f6e3518dab already exists" \
           -e "Cannot parse projection test_projection" \
           -e "Key expressions cannot contain subqueries" \
    /test_output/clickhouse-server.upgrade.log \
    | grep -av -e "_repl_01111_.*Mapping for table with UUID" \
    | grep -Fa "<Error>" > /test_output/upgrade_error_messages.txt || true

if [ -s /test_output/upgrade_error_messages.txt ]; then
    echo -e "Error message in clickhouse-server.log (see upgrade_error_messages.txt)$FAIL$(head_escaped /test_output/upgrade_error_messages.txt)" >> /test_output/test_results.tsv
else
    echo -e "No Error messages after server upgrade$OK" >> /test_output/test_results.tsv
fi

# Remove file upgrade_error_messages.txt if it's empty
[ -s /test_output/upgrade_error_messages.txt ] || rm -f /test_output/upgrade_error_messages.txt

# Grep logs for sanitizer asserts, crashes and other critical errors
check_logs_for_critical_errors

tar -chf /test_output/coordination.tar /var/lib/clickhouse/coordination ||:

collect_query_and_trace_logs

mv /var/log/clickhouse-server/stderr.log /test_output/

collect_core_dumps
