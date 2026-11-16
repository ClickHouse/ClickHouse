-- The `Merge` children disagree about `x`: it is physical in one child and an `ALIAS` in the
-- other. A JOIN has to resolve both `key` and `x` through `StorageMerge`, exercising the batched
-- identifier resolution and the subsequent alias-expression rewrite.
SET enable_analyzer = 1;

CREATE TABLE t04903_physical (key UInt8, x UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t04903_alias (key UInt8, y UInt8, x UInt8 ALIAS y IN (1, 2, 3)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t04903_physical VALUES (0, 7);
INSERT INTO t04903_alias (key, y) VALUES (1, 2), (2, 9);

SELECT arraySort(groupArray(r.x))
FROM numbers(3) AS l
INNER JOIN merge(currentDatabase(), '^t04903_(physical|alias)$') AS r ON l.number = r.key;

DROP TABLE t04903_physical;
DROP TABLE t04903_alias;
