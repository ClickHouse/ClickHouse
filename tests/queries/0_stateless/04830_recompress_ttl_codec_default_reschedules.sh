#!/usr/bin/env bash
# Tags: long

# An exact `TTL ... RECOMPRESS CODEC(Default)` entry resolves through the normal default codec
# selection, and the background recompression selector must schedule a recompression-only merge
# whenever a part's codec differs from the currently resolved default — e.g. a part written under
# `LZ4` before `default_compression_codec` was changed to `ZSTD(3)` — instead of treating the
# `Default` alias as "never changes the codec" and skipping the part forever.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_04830_recompress_default"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS ${TABLE};
CREATE TABLE ${TABLE} (dt DateTime, x UInt64)
ENGINE = MergeTree ORDER BY tuple()
TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(Default)
SETTINGS default_compression_codec = 'LZ4', merge_with_recompression_ttl_timeout = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
SYSTEM STOP TTL MERGES ${TABLE};
INSERT INTO ${TABLE} SELECT now() - INTERVAL 1 DAY, number FROM numbers(10000);
SELECT 'before', default_compression_codec FROM system.parts WHERE database = currentDatabase() AND table = '${TABLE}' AND active;
ALTER TABLE ${TABLE} MODIFY SETTING default_compression_codec = 'ZSTD(3)';
SYSTEM START TTL MERGES ${TABLE};
"

# The recompression merge is scheduled by the background selector; wait for it.
codec=""
for _ in {1..600}; do
    codec=$(${CLICKHOUSE_CLIENT} -q "SELECT default_compression_codec FROM system.parts WHERE database = currentDatabase() AND table = '${TABLE}' AND active")
    if [ "$codec" == "ZSTD(3)" ]; then
        break
    fi
    sleep 0.3
done

echo "after	$codec"
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(x) FROM ${TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${TABLE}"
