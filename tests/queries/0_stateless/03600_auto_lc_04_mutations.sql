DROP TABLE IF EXISTS kek;

CREATE TABLE kek (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS
    statistics_compact_format = 1,
    use_statistics_for_serialization_info = 1,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    max_uniq_number_for_low_cardinality = 1000,
    statistics_types = 'defaults, uniq';

INSERT INTO kek SELECT number, number FROM numbers(10000);

SELECT serialization_kind, statistics, estimated_cardinality, estimated_defaults
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'kek' AND column = 'b' AND active;

SET mutations_sync = 2;
ALTER TABLE kek UPDATE b = 0 WHERE 1;

SELECT serialization_kind, statistics, estimated_cardinality, estimated_defaults
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'kek' AND column = 'b' AND active;

OPTIMIZE TABLE kek FINAL;

SELECT serialization_kind, statistics, estimated_cardinality, estimated_defaults
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'kek' AND column = 'b' AND active;

DROP TABLE IF EXISTS kek;
