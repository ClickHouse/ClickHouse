DROP TABLE IF EXISTS test;
CREATE TABLE test (a UInt8, b UInt8, c UInt16 ALIAS a + b) ENGINE = MergeTree ORDER BY a;

SELECT b FROM test PREWHERE c = 1;

DROP TABLE test;

drop table if exists audience_local;
create table audience_local
(
 Date Date,
 AudienceType Enum8('other' = 0, 'client' = 1, 'group' = 2),
 UMA UInt64,
 APIKey String,
 TrialNameID UInt32,
 TrialGroupID UInt32,
 AppVersion String,
 Arch Enum8('other' = 0, 'x32' = 1, 'x64' = 2),
 UserID UInt32,
 GroupID UInt8,
 OSName Enum8('other' = 0, 'Android' = 1, 'iOS' = 2, 'macOS' = 3, 'Windows' = 4, 'Linux' = 5),
 Channel Enum8('other' = 0, 'Canary' = 1, 'Dev' = 2, 'Beta' = 3, 'Stable' = 4),
 Hits UInt64,
 Sum Int64,
 Release String alias splitByChar('-', AppVersion)[1]
)
engine = SummingMergeTree
PARTITION BY (toISOYear(Date), toISOWeek(Date))
ORDER BY (AudienceType, UMA, APIKey, Date, TrialNameID, TrialGroupID, AppVersion, Arch, UserID, GroupID, OSName, Channel)
SETTINGS index_granularity = 8192;

SELECT DISTINCT UserID
FROM audience_local
PREWHERE Date = toDate('2019-07-25') AND Release = '17.11.0.542';

drop table if exists audience_local;
