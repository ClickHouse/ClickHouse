DROP TABLE IF EXISTS map_kc;

CREATE TABLE map_kc (id UInt32, m Map(String, Nullable(UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';

INSERT INTO map_kc VALUES (1, {'a': 1, 'b': 2}), (2, {'a': 10, 'c': 30});
INSERT INTO map_kc VALUES (3, {'b': 200, 'd': NULL});

SELECT id, m FROM map_kc ORDER BY id;
SELECT id, m['a'] FROM map_kc ORDER BY id;
SELECT id, m['d'] FROM map_kc ORDER BY id;
SELECT mapContains(m, 'c') FROM map_kc ORDER BY id;
SELECT mapContains(m, 'zzz') FROM map_kc ORDER BY id;
SELECT mapKeys(m) FROM map_kc ORDER BY id;
SELECT id, m['missing'] FROM map_kc ORDER BY id;

-- present NULL vs absent key for Nullable(V)
SELECT id, m['d'], mapContains(m, 'd') FROM map_kc ORDER BY id;

INSERT INTO map_kc VALUES (4, {'a': 1, 'a': 2}); -- {serverError BAD_ARGUMENTS}
INSERT INTO map_kc VALUES (4, {'': 1}); -- {serverError BAD_ARGUMENTS}

OPTIMIZE TABLE map_kc FINAL;

SELECT id, m FROM map_kc ORDER BY id;
SELECT mapKeys(m) FROM map_kc ORDER BY id;
SELECT sum(m['a']), sum(m['b']), countIf(mapContains(m, 'c')) FROM map_kc;

DROP TABLE map_kc;
