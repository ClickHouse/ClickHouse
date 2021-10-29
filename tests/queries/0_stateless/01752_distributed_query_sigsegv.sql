-- Tags: distributed

-- this is enough to trigger the regression
SELECT throwIf(dummy = 0) FROM remote('127.1', system.one); -- { serverError 395 }

-- these are just in case
SELECT throwIf(dummy = 0) FROM remote('127.{1,2}', system.one); -- { serverError 395 }
SELECT throwIf(dummy = 0) FROM remote('127.{1,2}', system.one) SETTINGS prefer_localhost_replica=0; -- { serverError 395 }
SELECT throwIf(dummy = 0) FROM remote('127.{1,2}', system.one) SETTINGS prefer_localhost_replica=0, distributed_group_by_no_merge=1; -- { serverError 395 }
SELECT throwIf(dummy = 0) FROM remote('127.{1,2}', system.one) SETTINGS prefer_localhost_replica=0, distributed_group_by_no_merge=2; -- { serverError 395 }
