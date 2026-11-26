-- Tags: no-replicated-database, no-parallel-replicas
-- no-parallel, no-parallel-replicas: Dictionary is not created in parallel replicas.

-- { echo }

DROP DICTIONARY IF EXISTS dict;

CREATE DICTIONARY dict
(
  id Int64,
  f  Int64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(QUERY 'SELECT 1 id, 2 f'))
LAYOUT(flat())
lifetime(0);

SELECT
    dictGet(dict, 'f', dummy),
   (dictGet(dict, 'f', dummy) = 1),
    count()
FROM system.one
GROUP BY all
SETTINGS optimize_inverse_dictionary_lookup=0;

SELECT
    dictGet(dict, 'f', dummy),
   (dictGet(dict, 'f', dummy) = 1),
    count()
FROM system.one
GROUP BY all
SETTINGS optimize_inverse_dictionary_lookup=1;


SELECT
    toYear(d),
    (toYear(d) = 2024),
    count()
FROM values('d Date', ('2024-01-02'))
GROUP BY ALL
SETTINGS optimize_time_filter_with_preimage = 0;

SELECT
    toYear(d),
    (toYear(d) = 2024),
    count()
FROM values('d Date', ('2024-01-02'))
GROUP BY ALL
SETTINGS optimize_time_filter_with_preimage = 1;


SELECT tuple(dummy), (tuple(dummy) = tuple(1)), count()
FROM system.one
GROUP BY ALL
SETTINGS optimize_injective_functions_in_group_by=0;

SELECT tuple(dummy), (tuple(dummy) = tuple(1)), count()
FROM system.one
GROUP BY ALL
SETTINGS optimize_injective_functions_in_group_by=1;
