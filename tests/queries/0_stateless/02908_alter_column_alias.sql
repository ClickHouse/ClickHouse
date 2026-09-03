CREATE TABLE t (
    c0 DateTime,
    c1 DateTime,
    a DateTime alias toStartOfFifteenMinutes(c0)
) ENGINE = MergeTree() ORDER BY tuple();

ALTER TABLE t MODIFY COLUMN a DateTime ALIAS c1;
SHOW CREATE t;
