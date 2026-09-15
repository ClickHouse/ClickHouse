#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The reported shape, in the variant where the combining merge runs BEFORE the rows expire: the
# merged row satisfies the TTL `WHERE` but its TTL is still in the future, so the merge must keep the
# row and record it as expirable at that time. Once it does expire, background TTL selection has to
# pick the part up on its own - there is no `OPTIMIZE` after the combining merge, so nothing else can
# delete the row, and a part reporting the source parts' "nothing to expire" is never selected.

# Far enough ahead that the combining merge below cannot race the expiry even on a loaded machine.
# A Unix timestamp, not a datetime string: stateless tests randomize `session_timezone`, and a bare
# string would be parsed - and the metadata compared - in whatever zone each client session got.
EXPIRY=$(( $(date '+%s') + 20 ))

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS ttl_where_expires_later;

    CREATE TABLE ttl_where_expires_later
    (
        key UInt64,
        occurrences SimpleAggregateFunction(sum, Int64),
        expiry SimpleAggregateFunction(max, DateTime)
    )
    ENGINE = AggregatingMergeTree
    ORDER BY key
    TTL expiry DELETE WHERE occurrences = 0
    SETTINGS min_bytes_for_wide_part = 0, merge_with_ttl_timeout = 0;

    -- Keep the parts apart so the combining merge is the OPTIMIZE below and not a background merge.
    SYSTEM STOP MERGES ttl_where_expires_later;

    INSERT INTO ttl_where_expires_later VALUES (1, -1, toDateTime($EXPIRY));
    INSERT INTO ttl_where_expires_later VALUES (1, +1, toDateTime($EXPIRY));

    SYSTEM START MERGES ttl_where_expires_later;
    OPTIMIZE TABLE ttl_where_expires_later FINAL;
"

# The merge summed the two rows to 0, so the WHERE matches now, but the TTL has not passed yet.
$CLICKHOUSE_CLIENT -q "SELECT 'not expired yet', count() FROM ttl_where_expires_later"

# The merged part must advertise that row as expirable at its own TTL, which only holds if the merge
# re-evaluated the rows-WHERE TTL on its output.
$CLICKHOUSE_CLIENT -q "
    SELECT 'ttl info from merge output',
        rows_where_ttl_info.min = [toDateTime($EXPIRY)] AND rows_where_ttl_info.max = [toDateTime($EXPIRY)]
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'ttl_where_expires_later' AND active
"

for _ in {1..180}
do
    count=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ttl_where_expires_later")
    if [[ "$count" == "0" ]]; then
        break
    fi
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "SELECT 'deleted in background', count() FROM ttl_where_expires_later"

# A single part is never picked by the regular merge selector, so the deletion can only have come
# from a TTL merge that the selector chose by itself. Every row of the part is expired here, so the
# selector takes the drop variant rather than rewriting the part; accept either.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS part_log"
$CLICKHOUSE_CLIENT -q "
    SELECT 'ttl merge happened', countIf(merge_reason IN ('TTLDeleteMerge', 'TTLDropMerge')) >= 1
    FROM system.part_log
    WHERE event_date >= yesterday() AND database = currentDatabase()
      AND table = 'ttl_where_expires_later' AND event_type = 'MergeParts'
"

$CLICKHOUSE_CLIENT -q "DROP TABLE ttl_where_expires_later"
