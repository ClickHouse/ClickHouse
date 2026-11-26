SET convert_query_to_cnf = 0;

DROP TABLE IF EXISTS n;
DROP TABLE IF EXISTS r;

CREATE TABLE n (k UInt32) ENGINE = Memory;
CREATE TABLE r (k UInt32, name String) ENGINE = Memory;

SET enable_optimize_predicate_expression = 0;

EXPLAIN SYNTAX SELECT * FROM n, r WHERE n.k = r.k AND r.name = 'A';
EXPLAIN SYNTAX SELECT * FROM n, r WHERE n.k = r.k AND r.name LIKE 'A%';
EXPLAIN SYNTAX SELECT * FROM n, r WHERE n.k = r.k AND r.name NOT LIKE 'A%';

DROP TABLE n;
DROP TABLE r;
