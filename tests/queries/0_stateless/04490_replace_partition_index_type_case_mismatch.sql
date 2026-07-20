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
