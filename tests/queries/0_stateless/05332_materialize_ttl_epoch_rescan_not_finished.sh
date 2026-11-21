#!/usr/bin/env bash

# A part that holds a row whose rows TTL computes to exactly the epoch is rescanned by `MATERIALIZE TTL`
# even when its stored bounds are fully expired. `TTLDeleteAlgorithm` seeds `ttl_finished` from those old
# bounds, so if a row that survives the rescan has a live TTL under the new expression, `ttl_finished`
# must be cleared: otherwise the part's TTL is considered done, the merge selector never picks it for a
# TTL merge, and the row is never removed after it expires.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_ttl_epoch_rescan;
    CREATE TABLE t_ttl_epoch_rescan (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
        TTL d - INTERVAL 1 DAY
        SETTINGS min_bytes_for_full_part_storage = 0, merge_with_ttl_timeout = 0, materialize_ttl_recalculate_only = 0;
    SYSTEM STOP MERGES t_ttl_epoch_rescan;
    -- The first row's TTL is exactly the epoch, the second one's is expired.
    INSERT INTO t_ttl_epoch_rescan VALUES ('1970-01-02 00:00:00'), (now());
    ALTER TABLE t_ttl_epoch_rescan MODIFY TTL d + INTERVAL 10 SECOND SETTINGS materialize_ttl_after_modify = 0, alter_sync = 2;
    SYSTEM START MERGES t_ttl_epoch_rescan;
    -- The rescan removes the first row and keeps the second one, whose TTL is now in the future.
    ALTER TABLE t_ttl_epoch_rescan MATERIALIZE TTL SETTINGS mutations_sync = 2;
"

# Once the second row expires, a background TTL merge must remove it.
for _ in {1..600}
do
    [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM t_ttl_epoch_rescan") == 0 ]] && break
    sleep 0.5
done

$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t_ttl_epoch_rescan;
    DROP TABLE t_ttl_epoch_rescan;
"
