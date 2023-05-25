-- Tags: no-parallel

DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl(a String, b UInt32, c Float64, d Int64, e UInt8) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO tbl SELECT number, number * 2, number * 3, number * 4, number * 5 FROM system.numbers LIMIT 10;

SET mutations_sync = 1;

-- Change the types of columns by adding a temporary column and updating and dropping.
-- Alters should be executed in sequential order.
ALTER TABLE tbl ADD COLUMN xi Int64;
ALTER TABLE tbl UPDATE xi = a WHERE 1;
ALTER TABLE tbl DROP COLUMN a;
ALTER TABLE tbl ADD COLUMN a Int64;
ALTER TABLE tbl UPDATE a = xi WHERE 1;
ALTER TABLE tbl DROP COLUMN xi;

ALTER TABLE tbl ADD COLUMN xi String;
ALTER TABLE tbl UPDATE xi = b WHERE 1;
ALTER TABLE tbl DROP COLUMN b;
ALTER TABLE tbl ADD COLUMN b String;
ALTER TABLE tbl UPDATE b = xi WHERE 1;
ALTER TABLE tbl DROP COLUMN xi;

ALTER TABLE tbl ADD COLUMN xi UInt8;
ALTER TABLE tbl UPDATE xi = c WHERE 1;
ALTER TABLE tbl DROP COLUMN c;
ALTER TABLE tbl ADD COLUMN c UInt8;
ALTER TABLE tbl UPDATE c = xi WHERE 1;
ALTER TABLE tbl DROP COLUMN xi;

ALTER TABLE tbl ADD COLUMN xi Float64;
ALTER TABLE tbl UPDATE xi = d WHERE 1;
ALTER TABLE tbl DROP COLUMN d;
ALTER TABLE tbl ADD COLUMN d Float64;
ALTER TABLE tbl UPDATE d = xi WHERE 1;
ALTER TABLE tbl DROP COLUMN xi;

ALTER TABLE tbl ADD COLUMN xi UInt32;
ALTER TABLE tbl UPDATE xi = e WHERE 1;
ALTER TABLE tbl DROP COLUMN e;
ALTER TABLE tbl ADD COLUMN e UInt32;
ALTER TABLE tbl UPDATE e = xi WHERE 1;
ALTER TABLE tbl DROP COLUMN xi;

SELECT * FROM tbl FORMAT TabSeparatedWithNamesAndTypes;

DROP TABLE tbl;

-- Do the same thing again but with MODIFY COLUMN.
CREATE TABLE tbl(a String, b UInt32, c Float64, d Int64, e UInt8) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO tbl SELECT number, number * 2, number * 3, number * 4, number * 5 FROM system.numbers LIMIT 10;
ALTER TABLE tbl MODIFY COLUMN a Int64, MODIFY COLUMN b String, MODIFY COLUMN c UInt8, MODIFY COLUMN d Float64, MODIFY COLUMN e UInt32;
SELECT * FROM tbl FORMAT TabSeparatedWithNamesAndTypes;

DROP TABLE tbl;
