-- Tags: no-fasttest
-- - no-fasttest -- compiled w/o datasketches

-- Regression for very high error in uniqTheta() due to optimization for u8 keys
select throwIf(stddevSampStable(theta)/avg(theta)>0.1) from (select number%3 key, uniqTheta(generateUUIDv4(number)) theta FROM numbers_mt(9160000) GROUP BY key with totals SETTINGS max_threads = 8) format Null;
