DROP TYPE IF EXISTS SystemTestType1;
DROP TYPE IF EXISTS SystemTestType2;

CREATE TYPE SystemTestType1 AS UInt64 INPUT 'toUInt64(value)' OUTPUT 'toString(value)' DEFAULT '42';
CREATE TYPE SystemTestType2 AS Array(String);

SELECT name FROM system.tables WHERE database = 'udt' AND name = 'user_defined_types';

SELECT name, base_type_ast_string FROM udt.user_defined_types ORDER BY name;

SELECT 
    name, 
    base_type_ast_string,
    input_expression,
    output_expression,
    default_expression,
    length(create_query_string) > 0 as has_create_query
FROM udt.user_defined_types 
WHERE name IN ('SystemTestType1', 'SystemTestType2')
ORDER BY name;

SELECT name FROM udt.user_defined_types ORDER BY name;

SHOW TYPES;

DROP TYPE SystemTestType2;
SELECT name FROM udt.user_defined_types WHERE name = 'SystemTestType2';

DROP TYPE SystemTestType1;
SELECT name FROM udt.user_defined_types WHERE name LIKE 'SystemTestType%';
