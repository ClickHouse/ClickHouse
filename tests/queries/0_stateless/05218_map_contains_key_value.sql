-- Function mapContainsKeyValue(map, key, value): does the map hold an entry with this key and this value?

SELECT 'constant maps';
SELECT mapContainsKeyValue(map('k1', 'v1', 'k2', 'v2'), 'k1', 'v1');
SELECT mapContainsKeyValue(map('k1', 'v1', 'k2', 'v2'), 'k2', 'v2');
SELECT mapContainsKeyValue(map('k1', 'v1', 'k2', 'v2'), 'k1', 'v2');
SELECT mapContainsKeyValue(map('k1', 'v1', 'k2', 'v2'), 'k3', 'v1');
SELECT mapContainsKeyValue(map('k1', 'v1'), 'k1', '');
SELECT mapContainsKeyValue(map('k1', ''), 'k1', '');
SELECT mapContainsKeyValue(map('', 'v1'), '', 'v1');
SELECT mapContainsKeyValue(map(), 'k1', 'v1');
SELECT toTypeName(mapContainsKeyValue(map('k1', 'v1'), 'k1', 'v1'));

SELECT 'repeated keys';
SELECT mapContainsKeyValue(map('k', 'v1', 'k', 'v2'), 'k', 'v1'), mapContainsKeyValue(map('k', 'v1', 'k', 'v2'), 'k', 'v2');
SELECT map('k', 'v1', 'k', 'v2')['k'] = 'v1', map('k', 'v1', 'k', 'v2')['k'] = 'v2';

SELECT 'columns';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, m Map(String, String), k String, v String) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, map('a', '1', 'b', '2'), 'a', '1'), (2, map('a', '1', 'b', '2'), 'b', '1'), (3, map('a', '1', 'a', '2'), 'a', '2'), (4, map(), 'a', '1'), (5, map('a', ''), 'a', '');
SELECT id, mapContainsKeyValue(m, k, v), mapContainsKeyValue(m, 'a', '1') FROM tab ORDER BY id;
DROP TABLE tab;

SELECT 'types';
SELECT mapContainsKeyValue(map('k', 1), 'k', 1);
SELECT mapContainsKeyValue(map('k', 1), 'k', 1.0);
SELECT mapContainsKeyValue(map('k', 1), 'k', 2);
SELECT mapContainsKeyValue(map(1, 'v'), 1, 'v');
SELECT mapContainsKeyValue(map('k', ['a', 'b']), 'k', ['a', 'b']);
SELECT mapContainsKeyValue(CAST(map('k', 'v') AS Map(LowCardinality(String), LowCardinality(String))), 'k', 'v');
SELECT mapContainsKeyValue(CAST(map('k', 'v') AS Map(LowCardinality(String), LowCardinality(String))), toLowCardinality('k'), 'v');
-- `=` compares a FixedString through the String supertype, which drops the zero padding.
SELECT mapContainsKeyValue(map('k', 'v'), toFixedString('k', 3), 'v');

SELECT 'nulls';
SELECT mapContainsKeyValue(map('k', 'v'), 'k', NULL);
SELECT mapContainsKeyValue(map('k', 'v'), NULL, 'v');
SELECT toTypeName(mapContainsKeyValue(map('k', 'v'), 'k', NULL));
SELECT mapContainsKeyValue(CAST(map('k', NULL) AS Map(String, Nullable(String))), 'k', NULL);
SELECT mapContainsKeyValue(CAST(map('k', NULL) AS Map(String, Nullable(String))), 'k', 'v');
SELECT mapContainsKeyValue(CAST(map('k', 'v') AS Map(String, Nullable(String))), 'k', 'v');
SELECT mapContainsKeyValue(CAST(map('k', 'v') AS Map(String, Nullable(String))), 'k', NULL);

-- A typed `NULL` keeps its null bit inside the constant column. Read through a null map only, it looks
-- non-null, and the comparison then runs on the type default behind it, which can equal a stored value.
SELECT mapContainsKeyValue(map('k', ''), 'k', CAST(NULL AS Nullable(String)));
SELECT mapContainsKeyValue(map('k', 'v'), 'k', CAST(NULL AS Nullable(String)));
SELECT mapContainsKeyValue(CAST(map('k', NULL) AS Map(String, Nullable(String))), 'k', CAST(NULL AS Nullable(String)));
SELECT mapContainsKeyValue(CAST(map('k', NULL) AS Map(String, Nullable(String))), 'k', materialize(CAST(NULL AS Nullable(String))));
SELECT mapContainsKeyValue(CAST(map('k', 'v') AS Map(String, Nullable(String))), 'k', CAST('v' AS Nullable(String)));
SELECT mapContainsKeyValue(CAST(map('k', '') AS Map(String, Nullable(String))), 'k', CAST(NULL AS Nullable(String)));

SELECT 'documentation';
-- Argument types are rendered into links: an unknown name makes every read of these tables throw.
SELECT name, is_aggregate FROM system.functions WHERE name = 'mapContainsKeyValue';
SELECT count() FROM system.documentation WHERE type = 'Function' AND name = 'mapContainsKeyValue';

SELECT 'errors';
SELECT mapContainsKeyValue(map('k', 1), 'k', 'v'); -- { serverError NO_COMMON_TYPE }
SELECT mapContainsKeyValue('not a map', 'k', 'v'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapContainsKeyValue(map('k', 'v'), 'k'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapContainsKeyValue(map('k', 'v'), 'k', 'v', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
