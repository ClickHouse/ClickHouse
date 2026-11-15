DROP TABLE IF EXISTS signed_table;

CREATE TABLE signed_table (
    k UInt32,
    v String,
    s Int8
) ENGINE CollapsingMergeTree(s) ORDER BY k;

INSERT INTO signed_table(k, v, s) VALUES (1, 'a', 1);

ALTER TABLE signed_table DROP COLUMN s; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}
ALTER TABLE signed_table RENAME COLUMN s TO s1; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

DROP TABLE IF EXISTS signed_table;
