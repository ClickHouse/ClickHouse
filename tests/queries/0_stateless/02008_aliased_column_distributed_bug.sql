DROP TABLE IF EXISTS click_storage;
DROP TABLE IF EXISTS click_storage_dst;

CREATE TABLE click_storage ( `PhraseID` UInt64, `PhraseProcessedID` UInt64 ALIAS if(PhraseID > 0, PhraseID, 0) ) ENGINE = MergeTree() ORDER BY tuple();

CREATE TABLE click_storage_dst ( `PhraseID` UInt64, `PhraseProcessedID` UInt64 ) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'click_storage');

SELECT materialize(PhraseProcessedID)　FROM click_storage_dst;

DROP TABLE IF EXISTS click_storage;
DROP TABLE IF EXISTS click_storage_dst;
