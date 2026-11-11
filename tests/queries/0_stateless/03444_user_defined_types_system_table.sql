-- Tags: no-parallel
-- Tag no-parallel: user-defined types live in a single process-wide namespace.
DROP TYPE IF EXISTS SystemTestType1;
DROP TYPE IF EXISTS SystemTestType2;
DROP TYPE IF EXISTS SystemTestType3;

CREATE TYPE SystemTestType1 AS UInt64;
CREATE TYPE SystemTestType2 AS Array(String);
CREATE TYPE SystemTestType3(K, V) AS Map(K, V);

SELECT name, base_type, type_parameters, create_query FROM system.user_defined_types WHERE name LIKE 'SystemTestType%' ORDER BY name;

SHOW TYPES;

DROP TYPE SystemTestType2;
SELECT name FROM system.user_defined_types WHERE name = 'SystemTestType2';

DROP TYPE SystemTestType1;
DROP TYPE SystemTestType3;
SELECT name FROM system.user_defined_types WHERE name LIKE 'SystemTestType%';
