-- `variantElement` over a `Merge` table must see the `Variant` column that `Merge` derives as the
-- common type of the children, wherever in the query the call appears.

DROP TABLE IF EXISTS t_05237_seeds_a;
DROP TABLE IF EXISTS t_05237_seeds_b;

CREATE TABLE t_05237_seeds_a ORDER BY id AS SELECT number AS id, if(number % 2, NULL, 'unseeded') AS seed FROM numbers(4);
CREATE TABLE t_05237_seeds_b ORDER BY id AS SELECT number + 10 AS id, toNullable(toUInt8(number)) AS seed FROM numbers(4);

SELECT id, seed, variantType(seed) AS vt, variantElement(seed, 'UInt8') AS v FROM merge('t_05237_seeds_.*') ORDER BY id;

SELECT '--- the same value, everywhere else in the query ---';

SELECT id, variantElement(seed, 'UInt8') FROM merge('t_05237_seeds_.*') ORDER BY id;
SELECT count() FROM merge('t_05237_seeds_.*') WHERE variantElement(seed, 'UInt8') >= 2;
SELECT count() FROM (SELECT variantElement(seed, 'UInt8') AS v FROM merge('t_05237_seeds_.*')) WHERE v >= 2;
SELECT countIf(variantElement(seed, 'UInt8') >= 2) FROM merge('t_05237_seeds_.*');

SELECT '--- without the rewrite to a subcolumn ---';

SELECT count() FROM merge('t_05237_seeds_.*') WHERE variantElement(seed, 'UInt8') >= 2 SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '--- the subcolumn spelled out ---';

SELECT id, seed.UInt8, seed.String FROM merge('t_05237_seeds_.*') ORDER BY id;
SELECT id, seed, seed.UInt8 FROM merge('t_05237_seeds_.*') ORDER BY id;

SELECT '--- a child with an ALIAS column takes another code path ---';

DROP TABLE IF EXISTS t_05237_alias_a;
DROP TABLE IF EXISTS t_05237_alias_b;

CREATE TABLE t_05237_alias_a (id UInt64, seed Nullable(String), next UInt64 ALIAS id + 1) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05237_alias_b (id UInt64, seed Nullable(UInt8), next UInt64 ALIAS id + 1) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05237_alias_a SELECT number, if(number % 2, NULL, 'unseeded') FROM numbers(4);
INSERT INTO t_05237_alias_b SELECT number + 10, toUInt8(number) FROM numbers(4);

SELECT id, next, variantElement(seed, 'UInt8') FROM merge('t_05237_alias_.*') ORDER BY id;

SELECT '--- a column no child declares is still filled with the default ---';

DROP TABLE IF EXISTS t_05237_partial_a;
DROP TABLE IF EXISTS t_05237_partial_b;

CREATE TABLE t_05237_partial_a (id UInt64, seed Nullable(String), only_here UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05237_partial_b (id UInt64, seed Nullable(UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05237_partial_a VALUES (1, 'unseeded', 42);
INSERT INTO t_05237_partial_b VALUES (2, 7);

SELECT id, only_here, variantElement(seed, 'UInt8') FROM merge('t_05237_partial_.*') ORDER BY id;

DROP TABLE t_05237_seeds_a;
DROP TABLE t_05237_seeds_b;
DROP TABLE t_05237_alias_a;
DROP TABLE t_05237_alias_b;
DROP TABLE t_05237_partial_a;
DROP TABLE t_05237_partial_b;
