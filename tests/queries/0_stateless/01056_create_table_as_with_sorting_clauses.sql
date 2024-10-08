DROP TABLE IF EXISTS x;
DROP TABLE IF EXISTS x_as;

SELECT '-------------- Test copy sorting clauses from source table --------------';
CREATE TABLE x (`CounterID` UInt32, `EventDate` Date, `UserID` UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID);
CREATE TABLE x_as AS x ENGINE = MergeTree SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SHOW CREATE TABLE x FORMAT TSVRaw;
SELECT '-------------------------------------------------------------------------';
SHOW CREATE TABLE x_as FORMAT TSVRaw;

DROP TABLE x;
DROP TABLE x_as;

SELECT '-------------- Test copy sorting clauses from destination table (source table without the same type clauses) --------------';
CREATE TABLE x (`CounterID` UInt32, `EventDate` Date, `UserID` UInt64) ENGINE = MergeTree PRIMARY KEY (CounterID, EventDate, intHash32(UserID));
CREATE TABLE x_as AS x ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SHOW CREATE TABLE x FORMAT TSVRaw;
SELECT '-------------------------------------------------------------------------';
SHOW CREATE TABLE x_as FORMAT TSVRaw;

DROP TABLE x;
DROP TABLE x_as;

SELECT '-------------- Test copy sorting clauses from destination table (source table with the same type clauses) --------------';
CREATE TABLE x (`CounterID` UInt32, `EventDate` Date, `UserID` UInt64) ENGINE = MergeTree ORDER BY (CounterID);
CREATE TABLE x_as AS x ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SHOW CREATE TABLE x FORMAT TSVRaw;
SELECT '-------------------------------------------------------------------------';
SHOW CREATE TABLE x_as FORMAT TSVRaw;

DROP TABLE x;
DROP TABLE x_as;

SELECT '-------------- Test compatibility with allow_deprecated_syntax_for_merge_tree (source table is old syntax)  --------------';
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE x (`CounterID` UInt32, `EventDate` Date, `UserID` UInt64) ENGINE = MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192);
CREATE TABLE x_as AS x;

SHOW CREATE TABLE x FORMAT TSVRaw;
SELECT '-------------------------------------------------------------------------';
SHOW CREATE TABLE x_as FORMAT TSVRaw;

DROP TABLE x;
DROP TABLE x_as;

SELECT '-------------- Test compatibility with allow_deprecated_syntax_for_merge_tree (source table is new syntax)  --------------';
CREATE TABLE x (`CounterID` UInt32, `EventDate` Date, `UserID` UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID);
CREATE TABLE x_as AS x ENGINE = MergeTree SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SHOW CREATE TABLE x FORMAT TSVRaw;
SELECT '-------------------------------------------------------------------------';
SHOW CREATE TABLE x_as FORMAT TSVRaw;

DROP TABLE x;
DROP TABLE x_as;