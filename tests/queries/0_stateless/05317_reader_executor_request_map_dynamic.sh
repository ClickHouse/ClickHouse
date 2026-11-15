#!/usr/bin/env bash
# Tags: no-object-storage, no-distributed-cache, no-encrypted-storage, no-parallel-replicas
# The test finds the files of the column by their names in the logged path, which object storage replaces with a random key.
# The executor falls back to the legacy read path for the distributed cache and for decryption, so nothing is announced.
# Parallel replicas learn their ranges from the coordinator only after the readers exist.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_dynamic (k UInt64, d Dynamic) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1,
        min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
    INSERT INTO t_dynamic SELECT number, if(number % 2, number::Dynamic, toString(number)::Dynamic) FROM numbers(100000);
    OPTIMIZE TABLE t_dynamic FINAL;
"

# The wide reader creates the streams of `d` while it reads the prefix of the column, after the map was announced.
logs=$($CLICKHOUSE_CLIENT --send_logs_level=test --use_reader_executor=1 --max_threads=1 \
    --remote_filesystem_read_method=read --local_filesystem_read_method=pread --enable_filesystem_cache=0 \
    --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0 \
    -q "SELECT sum(length(toString(d))) FROM t_dynamic WHERE k < 5000 OR k >= 90000" 2>&1 >/dev/null)

for substream in d.variant_discr d.String d.UInt64
do
    # A case-insensitive disk stores the file under the hash of the stream name.
    file_name=$($CLICKHOUSE_CLIENT -q "
        SELECT filenames[indexOf(substreams, '$substream')] FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_dynamic' AND column = 'd' AND active")
    echo "$substream: $(echo "$logs" | grep -o "Request map of [^ ]*/$file_name\.bin: [0-9]* bytes in [0-9]* ranges" | grep -o '[0-9]* ranges$' | sort -u)"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE t_dynamic"
