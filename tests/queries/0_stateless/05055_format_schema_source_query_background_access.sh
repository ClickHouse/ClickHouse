#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Protobuf support

# The schema query of `format_schema_source='query'` is user-controlled, and a context without a
# user has full access, so it must be refused in background tasks.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The schema is cached on disk by the hash of the schema query, so the query has to be unique per
# run - otherwise a re-run would reuse the cache and never execute the query at all.
BG_MESSAGE="M_bg_${CLICKHOUSE_DATABASE}"
FG_MESSAGE="M_fg_${CLICKHOUSE_DATABASE}"

START=$(${CLICKHOUSE_CLIENT} --query "SELECT toString(now64(6))")

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE dest_bg (s String) ENGINE = File(ProtobufSingle)
SETTINGS format_schema_source = 'query',
         format_schema = 'SELECT ''syntax = \"proto3\"; message ${BG_MESSAGE} { string s = 1; }''',
         format_schema_message_name = '${BG_MESSAGE}';

/* Only the min thresholds are reachable, so the flush happens in the background pool. */
CREATE TABLE buf (s String) ENGINE = Buffer(currentDatabase(), dest_bg, 1, 1, 3600, 0, 1000000000, 0, 1000000000);
INSERT INTO buf VALUES ('hello');
"

refused=0
for _ in {1..60}
do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
    # The logger is checked too because this query is itself written to the text log, so otherwise
    # an iteration would match the previous one instead of the background exception.
    refused=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count() > 0 FROM system.text_log
        WHERE event_date >= toDate('${START}')
          AND event_time_microseconds >= toDateTime64('${START}', 6)
          AND logger_name LIKE '%StorageBuffer%'
          AND message LIKE '%can only be executed on behalf of a user%'")
    [ "$refused" = "1" ] && break
    sleep 0.5
done

echo "background flush refused: ${refused}"
echo "row still in buffer: $(${CLICKHOUSE_CLIENT} --query "
    SELECT total_bytes > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'buf'")"

# The same schema query still works in a query running on behalf of a user.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE dest_fg (s String) ENGINE = File(ProtobufSingle)
SETTINGS format_schema_source = 'query',
         format_schema = 'SELECT ''syntax = \"proto3\"; message ${FG_MESSAGE} { string s = 1; }''',
         format_schema_message_name = '${FG_MESSAGE}';
INSERT INTO dest_fg VALUES ('hello');
"
echo "foreground rows: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM dest_fg")"
