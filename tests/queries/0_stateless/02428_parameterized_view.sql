DROP TABLE IF EXISTS v1;
DROP TABLE IF EXISTS v2;
DROP TABLE IF EXISTS v3;
DROP TABLE IF EXISTS Catalog;
DROP TABLE IF EXISTS system.v1;
DROP TABLE IF EXISTS system.Catalog;

CREATE TABLE Catalog (Name String, Price UInt64, Quantity UInt64) ENGINE = Memory;

INSERT INTO Catalog VALUES ('Pen', 10, 3);
INSERT INTO Catalog VALUES ('Book', 50, 2);
INSERT INTO Catalog VALUES ('Paper', 20, 1);

CREATE VIEW v1 AS SELECT * FROM Catalog WHERE Price={price:UInt64};
SELECT Price FROM v1(price=20);
SELECT Price FROM `v1`(price=20);

set param_p=10;
SELECT Price FROM v1;  -- { serverError UNKNOWN_QUERY_PARAMETER}
SELECT Price FROM v1(price={p:UInt64});

set param_l=1;
SELECT Price FROM v1(price=50) LIMIT ({l:UInt64});

DETACH TABLE v1;
ATTACH TABLE v1;

EXPLAIN SYNTAX SELECT * from v1(price=10);

INSERT INTO v1 VALUES ('Bag', 50, 2); -- { serverError NOT_IMPLEMENTED}

SELECT Price FROM v123(price=20); -- { serverError UNKNOWN_FUNCTION }

CREATE VIEW v10 AS SELECT * FROM Catalog WHERE Price=10;

SELECT Price FROM v10(price=10);  -- { serverError UNKNOWN_FUNCTION }

CREATE VIEW v2 AS SELECT * FROM Catalog WHERE Price={price:UInt64} AND Quantity={quantity:UInt64};
SELECT Price FROM v2(price=50,quantity=2);

SELECT Price FROM v2(price=50); -- { serverError UNKNOWN_QUERY_PARAMETER}

CREATE VIEW v3 AS SELECT * FROM Catalog WHERE Price={price:UInt64} AND Quantity=3;
SELECT Price FROM v3(price=10);

CREATE VIEW v4 AS SELECT * FROM Catalog WHERE Price={price:UInt64} AND Quantity={price:UInt64}; -- {serverError BAD_ARGUMENTS}

CREATE TABLE system.Catalog (Name String, Price UInt64, Quantity UInt64) ENGINE = Memory;

INSERT INTO system.Catalog VALUES ('Pen', 10, 3);
INSERT INTO system.Catalog VALUES ('Book', 50, 2);
INSERT INTO system.Catalog VALUES ('Paper', 20, 1);

CREATE VIEW system.v1 AS SELECT * FROM system.Catalog WHERE Price={price:UInt64};
SELECT Price FROM system.v1(price=20);
SELECT Price FROM `system.v1`(price=20); -- { serverError UNKNOWN_FUNCTION }

INSERT INTO Catalog VALUES ('Book2', 30, 8);
INSERT INTO Catalog VALUES ('Book3', 30, 8);

CREATE VIEW v5 AS SELECT Price FROM Catalog WHERE {price:UInt64} HAVING Quantity in (SELECT {quantity:UInt64}) LIMIT {limit:UInt64};
SELECT Price FROM v5(price=30, quantity=8,limit=1);

DROP TABLE v1;
DROP TABLE v2;
DROP TABLE v3;
DROP TABLE Catalog;
DROP TABLE system.v1;
DROP TABLE system.Catalog;