-- add_minmax_index_for_numeric_columns=0: Changes data_skipping_indices
DROP TABLE IF EXISTS test_table;

-- packed_skip_index_max_bytes=0: this test asserts per-substream absolute byte sizes
-- reported by system.data_skipping_indices. Packed substreams are bundled into a single
-- archive file, so the per-substream size accounting is layout-dependent and would change.
-- default_compression_codec='LZ4': skip index substreams are compressed with the default
-- codec, so the compressed size depends on it; pin it (the reference uses the LZ4 default).
CREATE TABLE test_table
(
    key UInt64,
    value String,
    INDEX value_index value TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key SETTINGS compress_marks=false, add_minmax_index_for_numeric_columns=0, packed_skip_index_max_bytes=0, default_compression_codec='LZ4';

INSERT INTO test_table VALUES (0, 'Value');
SELECT * FROM system.data_skipping_indices WHERE database = currentDatabase();

ALTER TABLE test_table DROP INDEX value_index;
ALTER TABLE test_table ADD INDEX value_index value TYPE minmax GRANULARITY 1;
ALTER TABLE test_table MATERIALIZE INDEX value_index SETTINGS mutations_sync=1;

SELECT * FROM system.data_skipping_indices WHERE database = currentDatabase();

DROP TABLE test_table;
