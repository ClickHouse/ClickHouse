DROP TABLE IF EXISTS _02481_mergetree;
DROP TABLE IF EXISTS _02481_merge;

CREATE TABLE _02481_mergetree(x UInt64, y UInt64, arr Array(String)) ENGINE = MergeTree ORDER BY x SAMPLE BY x;

CREATE TABLE _02481_merge(x UInt64, y UInt64, arr Array(String)) ENGINE = Merge(currentDatabase(), '^(_02481_mergetree)$');

INSERT INTO _02481_mergetree SELECT number, number + 1, [1,2] FROM system.numbers LIMIT 100000;

SELECT count() FROM _02481_mergetree SAMPLE 1 / 2 ARRAY JOIN arr WHERE x != 0;
SELECT count() FROM _02481_merge SAMPLE 1 / 2 ARRAY JOIN arr WHERE x != 0;

DROP TABLE _02481_mergetree;
DROP TABLE _02481_merge;
