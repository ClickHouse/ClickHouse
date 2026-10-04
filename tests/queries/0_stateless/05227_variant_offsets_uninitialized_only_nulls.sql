-- Tags: memory-engine
-- Offsets of NULL rows in a Variant column are meaningless, but ColumnVariant::compress hands the
-- whole offsets buffer to LZ4, which reads every byte. Only a sanitizer build can observe this.
-- Sizing is load bearing: ColumnVector::compress skips buffers below 4096 bytes, so the block that
-- reaches the compressing sink must keep at least 512 rows. The insert block sizes are pinned so
-- that neither randomized settings nor squashing can split or re-create the block.

DROP TABLE IF EXISTS t_variant_src;
DROP TABLE IF EXISTS t_variant_dst;
DROP TABLE IF EXISTS t_json_src;
DROP TABLE IF EXISTS t_json_dst;

CREATE TABLE t_variant_src (n UInt64, v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO t_variant_src SELECT number, if(number % 2 = 0, NULL, number)::Variant(UInt64, String) FROM numbers(4096)
SETTINGS max_block_size = 4096, min_insert_block_size_rows = 4096, min_insert_block_size_bytes = 0;

CREATE TABLE t_variant_dst (v Variant(UInt64, String)) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_variant_dst SELECT v FROM t_variant_src WHERE n % 2 = 0
SETTINGS min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

SELECT count(), countIf(v IS NULL) FROM t_variant_dst;

-- The stored table must stay far below its raw offsets buffer of total_rows * 8 bytes. If this
-- line reddens, the block no longer reaches the compressing sink and the MSan oracle above is dead.
SELECT total_bytes < total_rows * 8 FROM system.tables WHERE database = currentDatabase() AND name = 't_variant_dst';

CREATE TABLE t_json_src (n UInt64, j JSON) ENGINE = Memory;
INSERT INTO t_json_src SELECT number, ('{"a":' || if(number % 2 = 0, 'null', toString(number)) || '}')::JSON FROM numbers(4096)
SETTINGS max_block_size = 4096, min_insert_block_size_rows = 4096, min_insert_block_size_bytes = 0;

CREATE TABLE t_json_dst (j JSON) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_json_dst SELECT j FROM t_json_src WHERE n % 2 = 0
SETTINGS min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

SELECT count(), countIf(j.a IS NULL) FROM t_json_dst;
SELECT total_bytes < total_rows * 8 FROM system.tables WHERE database = currentDatabase() AND name = 't_json_dst';

DROP TABLE t_variant_src;
DROP TABLE t_variant_dst;
DROP TABLE t_json_src;
DROP TABLE t_json_dst;
