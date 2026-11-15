SET param_CurrentStart='2025-02-09', param_CurrentEnd='2025-02-11';
SET prefer_localhost_replica = 0;

SELECT *
FROM cluster(test_shard_localhost, view(
    SELECT toDate({CurrentStart:String}), dummy::Date x from system.one
    order by x
    ASC WITH FILL FROM toDate({CurrentStart:String})
    TO toDate({CurrentEnd:String}) + toIntervalDay(1) STEP toIntervalDay(1)

));
