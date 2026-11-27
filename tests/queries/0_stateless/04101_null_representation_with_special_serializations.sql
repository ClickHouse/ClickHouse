-- Test that format_tsv_null_representation is respected when columns use special
-- serializations (SerializationReplicated, SerializationSparse).

SET format_tsv_null_representation = '<NULL>';
SET join_use_nulls = 1;
SET allow_special_serialization_kinds_in_output_formats = 1;
SET enable_lazy_columns_replication = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t_sparse;

CREATE TABLE t1 (key UInt64, val Nullable(String)) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t2 (key UInt64, val Nullable(String)) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t_sparse (id UInt64, val Nullable(String))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, nullable_serialization_version = 'allow_sparse';

INSERT INTO t1 VALUES (1, 'a'), (2, NULL), (3, 'c');
INSERT INTO t2 VALUES (1, 'x'), (3, NULL), (4, 'z');
INSERT INTO t_sparse SELECT number, if(number % 5 = 0, toString(number), NULL) FROM numbers(10);

-- Test SerializationReplicated
SELECT t1.val, t2.val FROM t1 FULL OUTER JOIN t2 ON t1.key = t2.key ORDER BY coalesce(t1.key, t2.key) FORMAT TabSeparatedRaw;

-- Test SerializationSparse
SELECT val FROM t_sparse ORDER BY id FORMAT TabSeparatedRaw;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t_sparse;
