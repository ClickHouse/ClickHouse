-- Tags: distributed

DROP TABLE IF EXISTS click_storage;
DROP TABLE IF EXISTS click_storage_dst;

CREATE TABLE click_storage ( `PhraseID` UInt64, `PhraseProcessedID` UInt64 ALIAS if(PhraseID > 5, PhraseID, 0) ) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO click_storage SELECT number AS PhraseID from numbers(10);

CREATE TABLE click_storage_dst ( `PhraseID` UInt64, `PhraseProcessedID` UInt64 ) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'click_storage');

SET prefer_localhost_replica = 1;
SELECT materialize(PhraseProcessedID)　FROM click_storage_dst;

SET prefer_localhost_replica = 0;
SELECT materialize(PhraseProcessedID)　FROM click_storage_dst;

DROP TABLE IF EXISTS click_storage;
DROP TABLE IF EXISTS click_storage_dst;
