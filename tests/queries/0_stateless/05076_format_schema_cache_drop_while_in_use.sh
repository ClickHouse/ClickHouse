#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Protobuf support
# Tag no-parallel: enables a server-wide failpoint and clears the server-wide schema cache
#
# A schema given by `format_schema_source` is published into the on-disk schema cache and read back
# from there by the Protobuf importer. `SYSTEM DROP FORMAT SCHEMA CACHE` must keep a file that a
# query has published and not read back yet, and must still remove it once the query is over. It must
# equally keep the staging file of a publish that has not renamed it into place yet.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FAILPOINT=format_schema_cache_pause_before_read
FAILPOINT_PUBLISH=format_schema_cache_pause_before_publish
# Unique per run, so the schema is not cached already and really has to be published.
MESSAGE="M05076_${CLICKHOUSE_DATABASE}"
CACHE_DIR="$CLICKHOUSE_SCHEMA_FILES/__cache__"

count_cached() { ls -1 "$CACHE_DIR" 2>/dev/null | wc -l; }
# Counts staging files only, so a published schema cannot satisfy the assert on its own.
count_temp() { ls -1 "$CACHE_DIR"/*.tmp 2>/dev/null | wc -l; }

# Defense-in-depth: an early failure must not leave a server-wide failpoint enabled for the tests
# that run after this one.
disable_failpoints() {
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT_PUBLISH}" 2>/dev/null
}
trap disable_failpoints EXIT

# The schema is resolved on the server, so the format has to run there: a client-side
# `SELECT ... FORMAT Protobuf` would publish into the client's own working directory instead.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE dest (s String) ENGINE = File(ProtobufSingle)
SETTINGS format_schema_source = 'string',
         format_schema = 'syntax = \"proto3\"; message ${MESSAGE} { string s = 1; }',
         format_schema_message_name = '${MESSAGE}';
"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP FORMAT SCHEMA CACHE"
echo "cached files at start: $(count_cached)"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${FAILPOINT}"

# Publishes the schema file, then pauses before the importer reads it back.
${CLICKHOUSE_CLIENT} --query "INSERT INTO dest VALUES ('hello')" &
query_pid=$!

${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${FAILPOINT} PAUSE"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP FORMAT SCHEMA CACHE"
echo "cached files kept while in use: $(count_cached)"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT}"
if wait "$query_pid"; then
    echo "query succeeded: 1"
else
    echo "query succeeded: 0"
fi

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP FORMAT SCHEMA CACHE"
echo "cached files after the query: $(count_cached)"

# The drop must also keep the staging file that a publish is about to rename into place. A schema of
# its own gives this arm a cache file that has never existed, so the insert publishes whatever the
# drops above did or did not remove.
MESSAGE_PUBLISH="M05076p_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE dest_publish (s String) ENGINE = File(ProtobufSingle)
SETTINGS format_schema_source = 'string',
         format_schema = 'syntax = \"proto3\"; message ${MESSAGE_PUBLISH} { string s = 1; }',
         format_schema_message_name = '${MESSAGE_PUBLISH}';
"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${FAILPOINT_PUBLISH}"

# Writes the staging file in full, then pauses before it becomes reachable under its final name.
${CLICKHOUSE_CLIENT} --query "INSERT INTO dest_publish VALUES ('world')" &
publish_pid=$!

${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${FAILPOINT_PUBLISH} PAUSE"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP FORMAT SCHEMA CACHE"
echo "staged files kept while publishing: $(count_temp)"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT_PUBLISH}"
if wait "$publish_pid"; then
    echo "publish succeeded: 1"
else
    echo "publish succeeded: 0"
fi

# A schema given by a query is resolved through a code path of its own, keyed by the querying user, so
# it is kept in use independently of a schema given as a string. The drop above leaves the arm before
# this one published, so the cache has to be emptied before this arm counts it.
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP FORMAT SCHEMA CACHE"

MESSAGE_QUERY="M05076q_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE dest_query (s String) ENGINE = File(ProtobufSingle)
SETTINGS format_schema_source = 'query',
         format_schema = 'SELECT ''syntax = \"proto3\"; message ${MESSAGE_QUERY} { string s = 1; }''',
         format_schema_message_name = '${MESSAGE_QUERY}';
"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${FAILPOINT}"

# Publishes the schema file, then pauses before the importer reads it back.
${CLICKHOUSE_CLIENT} --query "INSERT INTO dest_query VALUES ('hello')" &
query_source_pid=$!

${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${FAILPOINT} PAUSE"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP FORMAT SCHEMA CACHE"
echo "query-source cached files kept while in use: $(count_cached)"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT}"
if wait "$query_source_pid"; then
    echo "query-source query succeeded: 1"
else
    echo "query-source query succeeded: 0"
fi

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP FORMAT SCHEMA CACHE"
echo "query-source cached files after the query: $(count_cached)"
