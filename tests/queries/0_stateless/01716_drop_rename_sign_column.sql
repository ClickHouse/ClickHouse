DROP TABLE IF EXISTS signed_table;

CREATE TABLE signed_table (
    k UInt32,
    v String,
    s Int8
) ENGINE CollapsingMergeTree(s) ORDER BY k;

INSERT INTO signed_table(k, v, s) VALUES (1, 'a', 1);

ALTER TABLE signed_table DROP COLUMN s; --{serverError 524}
ALTER TABLE signed_table RENAME COLUMN s TO s1; --{serverError 524}

DROP TABLE IF EXISTS signed_table;
