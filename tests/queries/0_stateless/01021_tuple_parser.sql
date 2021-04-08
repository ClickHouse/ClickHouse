SELECT toTypeName((1,)), (1,);

SET enable_debug_queries = 1;

ANALYZE SELECT (1,);

DROP TABLE IF EXISTS tuple_values;

CREATE TABLE tuple_values (t Tuple(int)) ENGINE = Memory;

INSERT INTO tuple_values VALUES ((1)), ((2,));

DROP TABLE tuple_values;
