-- https://github.com/ClickHouse/ClickHouse/issues/69598
-- A parametrized view called inside a scalar subquery in a WITH clause used to fail
-- with UNKNOWN_IDENTIFIER ("Identifier 'default.test_pv.number' cannot be resolved").

DROP VIEW IF EXISTS test_pv;
CREATE VIEW test_pv AS SELECT number FROM numbers({limit:UInt64});

WITH (SELECT sum(number) FROM test_pv(limit = 10)) AS sm SELECT sm;

DROP VIEW test_pv;
