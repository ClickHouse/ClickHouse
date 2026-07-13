SET enable_analyzer = 1;
DROP TABLE IF EXISTS test_multiple_array_join;

CREATE TABLE test_multiple_array_join (
    id UInt64,
    person Nested (
        name String,
        surname String
    ),
    properties Nested (
        key String,
        value String
    )
) Engine=MergeTree ORDER BY id;
 
INSERT INTO test_multiple_array_join VALUES (1, ['Thomas', 'Michel'], ['Aquinas', 'Foucault'], ['profession', 'alive'], ['philosopher', 'no']);
INSERT INTO test_multiple_array_join VALUES (2, ['Thomas', 'Nicola'], ['Edison', 'Tesla'], ['profession', 'alive'], ['inventor', 'no']);

SELECT *
FROM test_multiple_array_join
ARRAY JOIN person
ARRAY JOIN properties
ORDER BY ALL;

DROP TABLE test_multiple_array_join;
