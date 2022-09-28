DROP TABLE IF EXISTS v1;
DROP TABLE IF EXISTS Catalog;

CREATE TABLE Catalog (Name String, Price UInt64, Quantity UInt64) ENGINE = Memory;

INSERT INTO Catalog VALUES ('Pen', 10, 3);
INSERT INTO Catalog VALUES ('Book', 50, 2);
INSERT INTO Catalog VALUES ('Paper', 20, 1);

CREATE VIEW v1 AS SELECT * FROM Catalog WHERE Price={price:UInt64};
SELECT Price FROM v1(price=20);

SELECT Price FROM v123(price=20); -- { serverError UNKNOWN_FUNCTION }

CREATE VIEW v10 AS SELECT * FROM Catalog WHERE Price=10;
SELECT Price FROM v10(price=10);  -- { serverError UNKNOWN_FUNCTION }


CREATE VIEW v2 AS SELECT * FROM Catalog WHERE Price={price:UInt64} AND Quantity={quantity:UInt64};
SELECT Price FROM v2(price=50,quantity=2);

SELECT Price FROM v2(price=50); -- { serverError UNKNOWN_QUERY_PARAMETER}

CREATE VIEW v3 AS SELECT * FROM Catalog WHERE Price={price:UInt64} AND Quantity=3;
SELECT Price FROM v3(price=10);

CREATE VIEW v4 AS SELECT * FROM Catalog WHERE Price={price:UInt64} AND Quantity={price:UInt64}; -- {serverError BAD_ARGUMENTS}

DROP TABLE v1;
DROP TABLE v2;
DROP TABLE v3;
DROP TABLE Catalog;
