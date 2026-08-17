-- The default value of a Variant is NULL, so count must skip its default rows.

SET allow_experimental_variant_type = 1;

DROP TABLE IF EXISTS variant_count_sparse;
DROP TABLE IF EXISTS variant_count_dense;

CREATE TABLE variant_count_sparse
(
    id UInt64,
    v Variant(UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;

CREATE TABLE variant_count_dense AS variant_count_sparse
ENGINE = MergeTree
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO variant_count_sparse SELECT
    number,
    if(number % 5 = 0, CAST(number AS Variant(UInt64)), CAST(NULL AS Variant(UInt64)))
FROM numbers(100);

INSERT INTO variant_count_dense SELECT * FROM variant_count_sparse;

OPTIMIZE TABLE variant_count_sparse FINAL;
OPTIMIZE TABLE variant_count_dense FINAL;

SELECT
    (SELECT count(v) FROM variant_count_sparse) AS sparse_count,
    (SELECT count(v) FROM variant_count_dense) AS dense_count,
    sparse_count = dense_count;

DROP TABLE variant_count_sparse;
DROP TABLE variant_count_dense;
