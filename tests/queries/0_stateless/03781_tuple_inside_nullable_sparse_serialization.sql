SET enable_multiple_prewhere_read_steps = 0;

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS tuple_test;

CREATE TABLE tuple_test
(
    tup Nullable(Tuple(u UInt64, s String))
)
ENGINE = MergeTree
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.0,
    min_bytes_for_wide_part = 0,
    nullable_serialization_version = 'allow_sparse';

INSERT INTO tuple_test (tup) VALUES (NULL);

SELECT tup FROM tuple_test
WHERE tup IS NULL AND tup.s = 'a';
