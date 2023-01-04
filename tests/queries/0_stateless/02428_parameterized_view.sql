DROP VIEW IF EXISTS test_02428_pv1;
DROP VIEW IF EXISTS test_02428_pv2;
DROP VIEW IF EXISTS test_02428_pv3;
DROP VIEW IF EXISTS test_02428_pv4;
DROP VIEW IF EXISTS test_02428_pv5;
DROP VIEW IF EXISTS test_02428_pv6;
DROP VIEW IF EXISTS test_02428_pv7;
DROP VIEW IF EXISTS test_02428_v1;
DROP TABLE IF EXISTS test_02428_Catalog;
DROP TABLE IF EXISTS db_02428.pv1;
DROP TABLE IF EXISTS db_02428.Catalog;
DROP DATABASE IF EXISTS db_02428;

CREATE TABLE test_02428_Catalog (Name String, Price UInt64, Quantity UInt64) ENGINE = Memory;

INSERT INTO test_02428_Catalog VALUES ('Pen', 10, 3);
INSERT INTO test_02428_Catalog VALUES ('Book', 50, 2);
INSERT INTO test_02428_Catalog VALUES ('Paper', 20, 1);

CREATE VIEW test_02428_pv1 AS SELECT * FROM test_02428_Catalog WHERE Price={price:UInt64};
SELECT Price FROM test_02428_pv1(price=20);
SELECT Price FROM `test_02428_pv1`(price=20);

set param_p=10;
SELECT Price FROM test_02428_pv1;  -- { serverError UNKNOWN_QUERY_PARAMETER}
SELECT Price FROM test_02428_pv1(price={p:UInt64});

set param_l=1;
SELECT Price FROM test_02428_pv1(price=50) LIMIT ({l:UInt64});

DETACH TABLE test_02428_pv1;
ATTACH TABLE test_02428_pv1;

EXPLAIN SYNTAX SELECT * from test_02428_pv1(price=10);

INSERT INTO test_02428_pv1 VALUES ('Bag', 50, 2); -- { serverError NOT_IMPLEMENTED}

SELECT Price FROM pv123(price=20); -- { serverError UNKNOWN_FUNCTION }

CREATE VIEW test_02428_v1 AS SELECT * FROM test_02428_Catalog WHERE Price=10;

SELECT Price FROM test_02428_v1(price=10);  -- { serverError UNKNOWN_FUNCTION }

CREATE VIEW test_02428_pv2 AS SELECT * FROM test_02428_Catalog WHERE Price={price:UInt64} AND Quantity={quantity:UInt64};
SELECT Price FROM test_02428_pv2(price=50,quantity=2);

SELECT Price FROM test_02428_pv2(price=50); -- { serverError UNKNOWN_QUERY_PARAMETER}

CREATE VIEW test_02428_pv3 AS SELECT * FROM test_02428_Catalog WHERE Price={price:UInt64} AND Quantity=3;
SELECT Price FROM test_02428_pv3(price=10);

CREATE VIEW test_02428_pv4 AS SELECT * FROM test_02428_Catalog WHERE Price={price:UInt64} AND Quantity={price:UInt64}; -- {serverError DUPLICATE_COLUMN}

CREATE DATABASE db_02428;

CREATE TABLE db_02428.Catalog (Name String, Price UInt64, Quantity UInt64) ENGINE = Memory;

INSERT INTO db_02428.Catalog VALUES ('Pen', 10, 3);
INSERT INTO db_02428.Catalog VALUES ('Book', 50, 2);
INSERT INTO db_02428.Catalog VALUES ('Paper', 20, 1);

CREATE VIEW db_02428.pv1 AS SELECT * FROM db_02428.Catalog WHERE Price={price:UInt64};
SELECT Price FROM db_02428.pv1(price=20);
SELECT Price FROM `db_02428.pv1`(price=20); -- { serverError UNKNOWN_FUNCTION }

INSERT INTO test_02428_Catalog VALUES ('Book2', 30, 8);
INSERT INTO test_02428_Catalog VALUES ('Book3', 30, 8);

CREATE VIEW test_02428_pv5 AS SELECT Price FROM test_02428_Catalog WHERE {price:UInt64} HAVING Quantity in (SELECT {quantity:UInt64}) LIMIT {limit:UInt64};
SELECT Price FROM test_02428_pv5(price=30, quantity=8,limit=1);

CREATE VIEW test_02428_pv6 AS SELECT Price+{price:UInt64} FROM test_02428_Catalog GROUP BY Price+{price:UInt64} ORDER BY Price+{price:UInt64};
SELECT * FROM test_02428_pv6(price=10);

CREATE VIEW test_02428_pv7 AS SELECT Price/{price:UInt64} FROM test_02428_Catalog ORDER BY Price;
SELECT * FROM test_02428_pv7(price=10);

DROP VIEW test_02428_pv1;
DROP VIEW test_02428_pv2;
DROP VIEW test_02428_pv3;
DROP VIEW test_02428_pv5;
DROP VIEW test_02428_pv6;
DROP VIEW test_02428_pv7;
DROP VIEW test_02428_v1;
DROP TABLE test_02428_Catalog;
DROP TABLE db_02428.pv1;
DROP TABLE db_02428.Catalog;
DROP DATABASE db_02428;