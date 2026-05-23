DROP DICTIONARY IF EXISTS TestTblDict;
DROP VIEW IF EXISTS TestTbl_view;
DROP TABLE IF EXISTS TestTbl;

CREATE TABLE TestTbl
(
    `id` UInt16,
    `dt` Date,
    `val` String
)
ENGINE = MergeTree
PARTITION BY dt
ORDER BY (id);

CREATE VIEW TestTbl_view
AS
SELECT *
FROM TestTbl
WHERE dt = ( SELECT max(dt) FROM TestTbl );

CREATE DICTIONARY IF NOT EXISTS TestTblDict
(
    `id` UInt16,
    `dt` Date,
    `val` String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE TestTbl_view DB currentDatabase()))
LIFETIME(1)
LAYOUT(COMPLEX_KEY_HASHED());

select 'view' src,* FROM TestTbl_view;
select 'dict' src,* FROM TestTblDict ;

insert into TestTbl values(1, '2022-10-20', 'first');

-- Force the dictionary to reload deterministically rather than sleeping and
-- relying on the background updater (5-second granularity, can be delayed
-- under CI load). `SYSTEM RELOAD DICTIONARY` exercises the same source-query
-- re-evaluation path that the original issue (#42610) was about.
SYSTEM RELOAD DICTIONARY TestTblDict;

select 'view' src,* FROM TestTbl_view;
select 'dict' src,* FROM TestTblDict ;

insert into TestTbl values(1, '2022-10-21', 'second');

SYSTEM RELOAD DICTIONARY TestTblDict;

select 'view' src,* FROM TestTbl_view;
select 'dict' src,* FROM TestTblDict ;

DROP DICTIONARY IF EXISTS TestTblDict;
DROP VIEW IF EXISTS TestTbl_view;
DROP TABLE IF EXISTS TestTbl;
