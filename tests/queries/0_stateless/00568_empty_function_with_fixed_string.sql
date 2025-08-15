SELECT toFixedString('', 4) AS str, empty(str) AS is_empty;
SELECT toFixedString('\0abc', 4) AS str, empty(str) AS is_empty;

DROP TABLE IF EXISTS defaulted;
CREATE TABLE defaulted (v6 FixedString(16)) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO defaulted SELECT toFixedString('::0', 16) FROM numbers(32768);
SELECT count(), notEmpty(v6) e FROM defaulted GROUP BY e;
DROP TABLE defaulted;
