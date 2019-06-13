--create database arr_tests;
--use arr_tests;
DROP TABLE IF EXISTS arr_tests_visits;

CREATE TABLE arr_tests_visits
(
    CounterID        UInt32,
    StartDate        Date,
    Sign             Int8,
    VisitID          UInt64,
    UserID           UInt64,
    VisitVersion     UInt16,
    `Adfox.BannerID` Array(UInt64),
    `Adfox.Load`     Array(UInt8),
    `Adfox.PuidKey`  Array(Array(UInt8)),
    `Adfox.PuidVal`  Array(Array(UInt32))
) ENGINE = MergeTree() PARTITION BY toMonday(StartDate) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192;

truncate table arr_tests_visits;
insert into arr_tests_visits (CounterID, StartDate, Sign, VisitID, UserID, VisitVersion, `Adfox.BannerID`, `Adfox.Load`, `Adfox.PuidKey`, `Adfox.PuidVal`)
values (1, toDate('2019-06-06'), 1, 1, 1, 1, [1], [1], [[]], [[]]),       (1, toDate('2019-06-06'), -1, 1, 1, 1, [1], [1], [[]], [[]]),       (1, toDate('2019-06-06'), 1, 1, 1, 2, [1,2], [1,1], [[],[1,2,3,4]], [[],[1001, 1002, 1003, 1004]]),       (1, toDate('2019-06-06'), 1, 2, 1, 1, [3], [1], [[3,4,5]], [[2001, 2002, 2003]]),       (1, toDate('2019-06-06'), 1, 3, 2, 1, [4, 5], [1, 0], [[5,6],[]], [[3001, 3002],[]]),       (1, toDate('2019-06-06'), 1, 4, 2, 1, [5, 5, 6], [1, 0, 0], [[1,2], [1, 2], [3]], [[1001, 1002],[1002, 1003], [2001]]);

select CounterID, StartDate, Sign, VisitID, UserID, VisitVersion, BannerID, Load, PuidKeyArr, PuidValArr, arrayEnumerateUniqRanked(PuidKeyArr, PuidValArr) as uniqAdfoxPuid
    from arr_tests_visits
    array join
         Adfox.BannerID as BannerID,
         Adfox.Load as Load,
         Adfox.PuidKey as PuidKeyArr,
         Adfox.PuidVal as PuidValArr;


SELECT
    CounterID,
    StartDate,
    Sign,
    VisitID,
    UserID,
    VisitVersion,
    BannerID,
    Load,
    PuidKeyArr,
    PuidValArr,
    arrayEnumerateUniqRanked(PuidKeyArr, PuidValArr) AS uniqAdfoxPuid
FROM arr_tests_visits
ARRAY JOIN
    Adfox.BannerID AS BannerID,
    Adfox.Load AS Load,
    Adfox.PuidKey AS PuidKeyArr,
    Adfox.PuidVal AS PuidValArr;


DROP TABLE IF NOT EXISTS arr_tests_visits;

