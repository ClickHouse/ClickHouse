DROP TABLE IF EXISTS constant_sort_key_single;
-- `identity` prevents constant folding, so the sorting expression remains `ColumnConst`.
CREATE TABLE constant_sort_key_single (payload UInt64)
ENGINE = MergeTree
ORDER BY identity(1);

INSERT INTO constant_sort_key_single SELECT 3 - number FROM numbers(3);
SELECT payload FROM constant_sort_key_single;
DROP TABLE constant_sort_key_single;

DROP TABLE IF EXISTS constant_sort_key_multiple;
CREATE TABLE constant_sort_key_multiple (payload UInt64)
ENGINE = MergeTree
ORDER BY (identity(1), identity(2), identity(3), identity(4));

INSERT INTO constant_sort_key_multiple SELECT 3 - number FROM numbers(3);
SELECT payload FROM constant_sort_key_multiple;
DROP TABLE constant_sort_key_multiple;

DROP TABLE IF EXISTS constant_sort_key_suffix;
CREATE TABLE constant_sort_key_suffix (payload UInt64)
ENGINE = MergeTree
ORDER BY (identity(1), identity(2), identity(3), payload);

INSERT INTO constant_sort_key_suffix SELECT 3 - number FROM numbers(3);
SELECT payload FROM constant_sort_key_suffix;
DROP TABLE constant_sort_key_suffix;
