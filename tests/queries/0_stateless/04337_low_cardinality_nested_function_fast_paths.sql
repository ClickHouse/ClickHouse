DROP TABLE IF EXISTS t_nested_lc_fast_paths;

CREATE TABLE t_nested_lc_fast_paths
(
    id UInt64,
    arr_lc Array(LowCardinality(String)),
    map_key_lc Map(LowCardinality(String), String),
    map_value_lc Map(String, LowCardinality(String))
)
ENGINE = Memory;

INSERT INTO t_nested_lc_fast_paths VALUES
    (1, ['alpha', 'beta', 'gamma'], {'alpha' : 'one', 'beta' : 'two', 'other' : 'zzz'}, {'k1' : 'alpha', 'k2' : 'beta', 'other' : 'zzz'}),
    (2, ['beta', 'delta'], {'beta' : 'twenty', 'delta' : 'four'}, {'k1' : 'beta', 'k4' : 'delta'}),
    (3, [], {}, {}),
    (4, ['alpha', 'alphabet'], {'alpha' : 'again', 'alphabet' : 'letters'}, {'k1' : 'alpha', 'k5' : 'alphabet'});

SELECT 'arrayElement Array(LowCardinality)';
SELECT id, arr_lc[1], arr_lc[2], arr_lc[-1], arr_lc[99]
FROM t_nested_lc_fast_paths
ORDER BY id;

SELECT 'mapContainsKeyLike and mapExtractKeyLike';
SELECT
    id,
    mapContainsKeyLike(map_key_lc, 'alpha'),
    mapContainsKeyLike(map_key_lc, 'alp%'),
    mapSort(mapExtractKeyLike(map_key_lc, 'alp%'))
FROM t_nested_lc_fast_paths
ORDER BY id;

SELECT 'mapContainsValueLike and mapExtractValueLike';
SELECT
    id,
    mapContainsValueLike(map_value_lc, 'alpha'),
    mapContainsValueLike(map_value_lc, 'alp%'),
    mapSort(mapExtractValueLike(map_value_lc, 'alp%'))
FROM t_nested_lc_fast_paths
ORDER BY id;

SELECT 'constant map fallbacks';
SELECT number, map(toLowCardinality('alpha'), 'one')['alpha']
FROM numbers(2)
ORDER BY number;
SELECT number, toTypeName(value), value
FROM
(
    SELECT number, materialize(CAST(map('alpha', 'one'), 'Map(LowCardinality(String), LowCardinality(String))'))['alpha'] AS value
    FROM numbers(2)
)
ORDER BY number;
SELECT number, mapContainsKeyLike(map(toLowCardinality('alpha'), 'one'), 'alp%'), mapSort(mapExtractKeyLike(map(toLowCardinality('alpha'), 'one'), 'alp%'))
FROM numbers(2)
ORDER BY number;
SELECT number, mapContainsKeyLike(materialize(CAST(map('alpha', 'one'), 'Map(LowCardinality(String), String)')), toLowCardinality('alp%')), mapSort(mapExtractKeyLike(materialize(CAST(map('alpha', 'one'), 'Map(LowCardinality(String), String)')), toLowCardinality('alp%')))
FROM numbers(2)
ORDER BY number;
SELECT number, mapContainsValueLike(map('k1', toLowCardinality('alpha')), 'alp%'), mapSort(mapExtractValueLike(map('k1', toLowCardinality('alpha')), 'alp%'))
FROM numbers(2)
ORDER BY number;

SELECT 'nullable argument fallbacks';
SELECT materialize(CAST(['x'], 'Array(LowCardinality(String))'))[toNullable(1)], toTypeName(materialize(CAST(['x'], 'Array(LowCardinality(String))'))[toNullable(1)]);
SELECT materialize(CAST(['x'], 'Array(LowCardinality(String))'))[CAST(NULL, 'Nullable(UInt8)')], toTypeName(materialize(CAST(['x'], 'Array(LowCardinality(String))'))[CAST(NULL, 'Nullable(UInt8)')]);
SELECT mapContainsKeyLike(materialize(CAST(map('alpha', 'one'), 'Map(LowCardinality(String), String)')), toNullable('alp%')), mapSort(mapExtractKeyLike(materialize(CAST(map('alpha', 'one'), 'Map(LowCardinality(String), String)')), toNullable('alp%')));
SELECT mapContainsKeyLike(materialize(CAST(map('alpha', 'one'), 'Map(LowCardinality(String), String)')), CAST(NULL, 'Nullable(String)')), mapSort(mapExtractKeyLike(materialize(CAST(map('alpha', 'one'), 'Map(LowCardinality(String), String)')), CAST(NULL, 'Nullable(String)')));
SELECT mapContainsValueLike(materialize(CAST(map('k1', 'alpha'), 'Map(String, LowCardinality(String))')), toNullable('alp%')), mapSort(mapExtractValueLike(materialize(CAST(map('k1', 'alpha'), 'Map(String, LowCardinality(String))')), toNullable('alp%')));
SELECT mapContainsValueLike(materialize(CAST(map('k1', 'alpha'), 'Map(String, LowCardinality(String))')), CAST(NULL, 'Nullable(String)')), mapSort(mapExtractValueLike(materialize(CAST(map('k1', 'alpha'), 'Map(String, LowCardinality(String))')), CAST(NULL, 'Nullable(String)')));

SELECT materialize(CAST(['x'], 'Array(LowCardinality(String))'))[0]; -- { serverError ZERO_ARRAY_OR_TUPLE_INDEX }

DROP TABLE t_nested_lc_fast_paths;
