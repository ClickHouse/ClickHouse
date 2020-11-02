DROP TABLE IF EXISTS table_from_remote;
DROP TABLE IF EXISTS table_from_select;
DROP TABLE IF EXISTS table_from_numbers;

CREATE TABLE table_from_remote AS remote('localhost', 'system', 'numbers');

SHOW CREATE TABLE table_from_remote;

ALTER TABLE table_from_remote ADD COLUMN col UInt8;

SHOW CREATE TABLE table_from_remote;

CREATE TABLE table_from_numbers AS numbers(1000);

SHOW CREATE TABLE table_from_numbers;

ALTER TABLE table_from_numbers ADD COLUMN col UInt8; --{serverError 48}

SHOW CREATE TABLE table_from_numbers;

CREATE TABLE table_from_select ENGINE = MergeTree() ORDER BY tuple() AS SELECT number from system.numbers LIMIT 1;

SHOW CREATE TABLE table_from_select;

ALTER TABLE table_from_select ADD COLUMN col UInt8;

SHOW CREATE TABLE table_from_select;

DROP TABLE IF EXISTS table_from_remote;
DROP TABLE IF EXISTS table_from_select;
DROP TABLE IF EXISTS table_from_numbers;
