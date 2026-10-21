-- An unqualified column name that is ambiguous between three or more joined tables
-- resolves to the alias with the same name under prefer_column_name_to_alias = 1,
-- as the old analyzer did.

SET enable_analyzer = 1;
SET prefer_column_name_to_alias = 1;

DROP TABLE IF EXISTS ranged_items;
DROP TABLE IF EXISTS product;
DROP TABLE IF EXISTS store;

CREATE TABLE ranged_items (SNO Int32, skuID Int32) ENGINE = Memory;
CREATE TABLE product (PIN Int32) ENGINE = Memory;
CREATE TABLE store (SNO Int32) ENGINE = Memory;
INSERT INTO ranged_items VALUES (1, 10), (2, 20), (2, 20);
INSERT INTO product VALUES (10), (20);
INSERT INTO store VALUES (1), (2);

-- comma join, alias name used in ORDER BY
SELECT ranged_items.SNO AS SNO, count() AS c
FROM ranged_items, product, store
WHERE ranged_items.skuID = product.PIN AND ranged_items.SNO = store.SNO
GROUP BY ranged_items.SNO ORDER BY SNO;

-- comma join, alias name used in GROUP BY and ORDER BY
SELECT ranged_items.SNO AS SNO, count() AS c
FROM ranged_items, product, store
WHERE ranged_items.skuID = product.PIN AND ranged_items.SNO = store.SNO
GROUP BY SNO ORDER BY SNO;

-- alias points at the last table
SELECT store.SNO + 10 AS SNO, count() AS c
FROM ranged_items, product, store
WHERE ranged_items.skuID = product.PIN AND ranged_items.SNO = store.SNO
GROUP BY SNO ORDER BY SNO;

-- alias name used in WHERE and inside a SELECT expression
SELECT ranged_items.SNO AS SNO, SNO * 100 AS SNO100
FROM ranged_items, product, store
WHERE ranged_items.skuID = product.PIN AND ranged_items.SNO = store.SNO AND SNO = 2
ORDER BY SNO100;

-- without an alias the identifier is still ambiguous
SELECT ranged_items.SNO AS s, count()
FROM ranged_items, product, store
WHERE ranged_items.skuID = product.PIN AND ranged_items.SNO = store.SNO
GROUP BY SNO; -- { serverError AMBIGUOUS_IDENTIFIER }

-- an alias whose expression is the ambiguous identifier itself cannot take over
SELECT SNO AS SNO
FROM ranged_items, product, store
WHERE ranged_items.skuID = product.PIN AND ranged_items.SNO = store.SNO; -- { serverError AMBIGUOUS_IDENTIFIER }

-- three tables: the alias is used
SELECT ranged_items.SNO + 10 AS SNO, SNO AS x
FROM ranged_items, product, store
WHERE ranged_items.skuID = product.PIN AND ranged_items.SNO = store.SNO
ORDER BY x;

-- two tables: the left table's column wins, the alias is not used
SELECT ranged_items.SNO + 10 AS SNO, SNO AS x
FROM ranged_items, store
WHERE ranged_items.SNO = store.SNO
ORDER BY x;

DROP TABLE ranged_items;
DROP TABLE product;
DROP TABLE store;

DROP TABLE IF EXISTS drl;
DROP TABLE IF EXISTS ap;
DROP TABLE IF EXISTS ab;

CREATE TABLE drl (BuyerID String, PublisherID Int32) ENGINE = Memory;
CREATE TABLE ap (PublisherID Int32, PublisherName String) ENGINE = Memory;
CREATE TABLE ab (BuyerID Int32, BuyerName String) ENGINE = Memory;
INSERT INTO drl VALUES ('1', 100), ('2', 200);
INSERT INTO ap VALUES (100, 'pubA'), (200, 'pubB');
INSERT INTO ab VALUES (1, 'buyer 1'), (2, 'buyer 2');

-- LEFT JOIN chain, alias name used in WHERE
SELECT drl.BuyerID AS BuyerID, ab.BuyerName AS BuyerName
FROM drl LEFT JOIN ap ON drl.PublisherID = ap.PublisherID
         LEFT JOIN ab ON drl.BuyerID = toString(ab.BuyerID)
WHERE BuyerID IN ('1');

-- ambiguity in the nested JOIN, the column of the outer right table must not win over the alias
SELECT ab0.BuyerName AS BuyerName, ab2.BuyerName
FROM ab AS ab0 LEFT JOIN ab AS ab1 ON ab0.BuyerID = ab1.BuyerID
               LEFT JOIN ab AS ab2 ON ab0.BuyerID = ab2.BuyerID + 1
WHERE BuyerName = 'buyer 2';

-- alias name used in an ON clause resolves to the alias as well
SELECT drl.PublisherID AS PublisherID, ap.PublisherName
FROM drl LEFT JOIN ab ON drl.BuyerID = toString(ab.BuyerID)
         LEFT JOIN ap ON PublisherID = ap.PublisherID
ORDER BY PublisherID;

DROP TABLE drl;
DROP TABLE ap;
DROP TABLE ab;
