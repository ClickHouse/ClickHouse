#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Protobuf support
# Tag no-parallel: enables a server-wide failpoint and clears the server-wide schema cache
#
# A schema given by `format_schema_source` is published into the on-disk schema cache and read back
# from there by the Protobuf importer. `SYSTEM DROP FORMAT SCHEMA CACHE` must keep a file that a
# query has published and not read back yet, and must still remove it once the query is over.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FAILPOINT=format_schema_cache_pause_before_read
# Unique per run, so the schema is not cached already and really has to be published.
MESSAGE="M05076_${CLICKHOUSE_DATABASE}"
CACHE_DIR="$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.server_settings WHERE name = 'format_schema_path'")/__cache__"

count_cached() { ls -1 "$CACHE_DIR" 2>/dev/null | wc -l; }

# Defense-in-depth: an early failure (e.g. a `SYSTEM WAIT FAILPOINT ... PAUSE` timeout) must not
# leave the server-wide failpoint enabled for the tests that run after this one.
trap '${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT '"${FAILPOINT}"'" 2>/dev/null' EXIT

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
