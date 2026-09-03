-- Tags: distributed

-- this is enough to trigger the regression
SELECT throwIf(dummy = 0) FROM remote('127.1', system.one); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- these are just in case
SELECT throwIf(dummy = 0) FROM remote('127.{1,2}', system.one); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT throwIf(dummy = 0) FROM remote('127.{1,2}', system.one) SETTINGS prefer_localhost_replica=0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT throwIf(dummy = 0) FROM remote('127.{1,2}', system.one) SETTINGS prefer_localhost_replica=0, distributed_group_by_no_merge=1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT throwIf(dummy = 0) FROM remote('127.{1,2}', system.one) SETTINGS prefer_localhost_replica=0, distributed_group_by_no_merge=2; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
