DROP VIEW IF EXISTS pv1;
DROP VIEW IF EXISTS pv2;
DROP VIEW IF EXISTS pv3;
DROP VIEW IF EXISTS pv4;
DROP VIEW IF EXISTS pv5;
DROP VIEW IF EXISTS pv6;
DROP VIEW IF EXISTS v1;
DROP TABLE IF EXISTS Catalog;
DROP TABLE IF EXISTS system.pv1;
DROP TABLE IF EXISTS system.Catalog;

CREATE TABLE Catalog (Name String, Price UInt64, Quantity UInt64) ENGINE = Memory;

INSERT INTO Catalog VALUES ('Pen', 10, 3);
INSERT INTO Catalog VALUES ('Book', 50, 2);
INSERT INTO Catalog VALUES ('Paper', 20, 1);

CREATE VIEW pv1 AS SELECT * FROM Catalog WHERE Price={price:UInt64};
SELECT Price FROM pv1(price=20);
SELECT Price FROM `pv1`(price=20);

set param_p=10;
SELECT Price FROM pv1;  -- { serverError UNKNOWN_QUERY_PARAMETER}
SELECT Price FROM pv1(price={p:UInt64});

set param_l=1;
SELECT Price FROM pv1(price=50) LIMIT ({l:UInt64});

DETACH TABLE pv1;
ATTACH TABLE pv1;

EXPLAIN SYNTAX SELECT * from pv1(price=10);

INSERT INTO pv1 VALUES ('Bag', 50, 2); -- { serverError NOT_IMPLEMENTED}

SELECT Price FROM pv123(price=20); -- { serverError UNKNOWN_FUNCTION }

CREATE VIEW v1 AS SELECT * FROM Catalog WHERE Price=10;

SELECT Price FROM v1(price=10);  -- { serverError UNKNOWN_FUNCTION }

CREATE VIEW pv2 AS SELECT * FROM Catalog WHERE Price={price:UInt64} AND Quantity={quantity:UInt64};
SELECT Price FROM pv2(price=50,quantity=2);

SELECT Price FROM pv2(price=50); -- { serverError UNKNOWN_QUERY_PARAMETER}

CREATE VIEW pv3 AS SELECT * FROM Catalog WHERE Price={price:UInt64} AND Quantity=3;
SELECT Price FROM pv3(price=10);

CREATE VIEW pv4 AS SELECT * FROM Catalog WHERE Price={price:UInt64} AND Quantity={price:UInt64}; -- {serverError DUPLICATE_COLUMN}

CREATE TABLE system.Catalog (Name String, Price UInt64, Quantity UInt64) ENGINE = Memory;

INSERT INTO system.Catalog VALUES ('Pen', 10, 3);
INSERT INTO system.Catalog VALUES ('Book', 50, 2);
INSERT INTO system.Catalog VALUES ('Paper', 20, 1);

CREATE VIEW system.pv1 AS SELECT * FROM system.Catalog WHERE Price={price:UInt64};
SELECT Price FROM system.pv1(price=20);
SELECT Price FROM `system.pv1`(price=20); -- { serverError UNKNOWN_FUNCTION }

INSERT INTO Catalog VALUES ('Book2', 30, 8);
INSERT INTO Catalog VALUES ('Book3', 30, 8);

CREATE VIEW pv5 AS SELECT Price FROM Catalog WHERE {price:UInt64} HAVING Quantity in (SELECT {quantity:UInt64}) LIMIT {limit:UInt64};
SELECT Price FROM pv5(price=30, quantity=8,limit=1);

CREATE VIEW pv6 AS SELECT Price+{price:UInt64} FROM Catalog GROUP BY Price+{price:UInt64} ORDER BY Price+{price:UInt64};
SELECT * FROM pv6(price=10);

DROP VIEW pv1;
DROP VIEW pv2;
DROP VIEW pv3;
DROP VIEW pv5;
DROP VIEW pv6;
DROP VIEW v1;
DROP TABLE Catalog;
DROP TABLE system.pv1;
DROP TABLE system.Catalog;