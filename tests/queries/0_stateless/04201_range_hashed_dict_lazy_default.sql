-- Test: exercises `dictGetOrDefault` with `RANGE_HASHED` dictionary, verifying that
-- the default expression remains LAZY (not evaluated) when the value is found.
-- Covers: src/Functions/FunctionsExternalDictionaries.h:338 — the loop
--   `for (size_t i = 0; i != number_of_arguments - 1; ++i)`
-- which inserts indices 0..N-2 into `arguments_with_disabled_lazy_execution`,
-- leaving the LAST argument (default expression) lazy. If the loop boundary
-- regressed to `i != number_of_arguments`, the throwing default would be
-- eagerly evaluated and the query would fail with ILLEGAL_DIVISION.

DROP DICTIONARY IF EXISTS range_lazy_default_dict;
DROP TABLE IF EXISTS range_lazy_default_src;

CREATE TABLE range_lazy_default_src (id UInt64, s Date, e Date, v UInt64) ENGINE = TinyLog;
INSERT INTO range_lazy_default_src VALUES (1, toDate('2024-01-01'), toDate('2024-12-31'), 100);

CREATE DICTIONARY range_lazy_default_dict
(
    id UInt64,
    s Date,
    e Date,
    v UInt64 DEFAULT 0
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'range_lazy_default_src'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(RANGE_HASHED())
RANGE(MIN s MAX e);

-- Key=1 IS in range → returns 100. The throwing default `intDiv(1, materialize(0))`
-- must NOT be evaluated. `materialize` defeats constant folding.
SELECT dictGetOrDefault('range_lazy_default_dict', 'v', toUInt64(1), toDate('2024-06-15'), intDiv(1, materialize(0)));

DROP DICTIONARY range_lazy_default_dict;
DROP TABLE range_lazy_default_src;
