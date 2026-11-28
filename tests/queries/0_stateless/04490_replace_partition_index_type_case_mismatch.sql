-- Index TYPE name is case-insensitive, so SET(0) and set(0) are the same index.
-- REPLACE PARTITION FROM must not report "different secondary indices" for them.

DROP TABLE IF EXISTS dst_idx_case;
DROP TABLE IF EXISTS src_idx_case;

CREATE TABLE dst_idx_case (id UInt64, INDEX idx id TYPE SET(0) GRANULARITY 4) ENGINE = MergeTree ORDER BY id PARTITION BY id % 2;
CREATE TABLE src_idx_case (id UInt64, INDEX idx id TYPE set(0) GRANULARITY 4) ENGINE = MergeTree ORDER BY id PARTITION BY id % 2;

INSERT INTO src_idx_case VALUES (0), (2);

ALTER TABLE dst_idx_case REPLACE PARTITION 0 FROM src_idx_case;
SELECT count() FROM dst_idx_case;

-- Type name is canonicalized to lower case in the stored definition.
SELECT type FROM system.data_skipping_indices WHERE table = 'src_idx_case' AND database = currentDatabase();

DROP TABLE dst_idx_case;
DROP TABLE src_idx_case;

-- Data type names inside CAST are aliases/case-insensitive (INT == Int32), so keys and index
-- expressions using them must also compare equal. REPLACE PARTITION FROM must not report
-- "different ordering" / "different secondary indices" for them.

DROP TABLE IF EXISTS dst_cast_type;
DROP TABLE IF EXISTS src_cast_type;

CREATE TABLE dst_cast_type (x String, INDEX i CAST(x AS Int32) TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY CAST(x AS Int32) PARTITION BY tuple();
CREATE TABLE src_cast_type (x String, INDEX i CAST(x AS INT) TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY CAST(x AS INT) PARTITION BY tuple();

INSERT INTO src_cast_type VALUES ('1'), ('2');

ALTER TABLE dst_cast_type REPLACE PARTITION tuple() FROM src_cast_type;
SELECT count() FROM dst_cast_type;

DROP TABLE dst_cast_type;
DROP TABLE src_cast_type;

-- The whole type-conversion family stores the target type as a string literal, so accurateCast and
-- its siblings must be canonicalized too, not only CAST.

DROP TABLE IF EXISTS dst_acc_cast;
DROP TABLE IF EXISTS src_acc_cast;

CREATE TABLE dst_acc_cast (x String, INDEX i accurateCast(x, 'Int32') TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY accurateCast(x, 'Int32') PARTITION BY tuple();
CREATE TABLE src_acc_cast (x String, INDEX i accurateCast(x, 'INT') TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY accurateCast(x, 'INT') PARTITION BY tuple();

INSERT INTO src_acc_cast VALUES ('1'), ('2');

ALTER TABLE dst_acc_cast REPLACE PARTITION tuple() FROM src_acc_cast;
SELECT count() FROM dst_acc_cast;

DROP TABLE dst_acc_cast;
DROP TABLE src_acc_cast;

-- defaultValueOfTypeName also resolves its only argument - a string literal - through
-- DataTypeFactory, so its type spelling must be canonicalized too.

DROP TABLE IF EXISTS dst_dvton;
DROP TABLE IF EXISTS src_dvton;

CREATE TABLE dst_dvton (x Int32)
    ENGINE = MergeTree ORDER BY x + defaultValueOfTypeName('Int32') PARTITION BY tuple();
CREATE TABLE src_dvton (x Int32)
    ENGINE = MergeTree ORDER BY x + defaultValueOfTypeName('INT') PARTITION BY tuple();

INSERT INTO src_dvton VALUES (1), (2);

ALTER TABLE dst_dvton REPLACE PARTITION tuple() FROM src_dvton;
SELECT count() FROM dst_dvton;

DROP TABLE dst_dvton;
DROP TABLE src_dvton;

-- JSONExtract takes the type as its last, variadic argument, JSONExtractKeysAndValues likewise.

DROP TABLE IF EXISTS dst_json;
DROP TABLE IF EXISTS src_json;

CREATE TABLE dst_json (j String)
    ENGINE = MergeTree ORDER BY length(JSONExtractKeysAndValues(j, 'Int32')) PARTITION BY tuple();
CREATE TABLE src_json (j String)
    ENGINE = MergeTree ORDER BY length(JSONExtractKeysAndValues(j, 'INT')) PARTITION BY tuple();

INSERT INTO src_json VALUES ('{"a": 1}'), ('{"a": 2}');

ALTER TABLE dst_json REPLACE PARTITION tuple() FROM src_json;
SELECT count() FROM dst_json;

DROP TABLE dst_json;
DROP TABLE src_json;
