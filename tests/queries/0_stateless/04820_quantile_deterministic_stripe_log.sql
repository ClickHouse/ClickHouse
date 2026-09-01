-- A fresh table pins the current state version of `quantileDeterministic` into its metadata, but the
-- engines that persist their data through `NativeWriter` with revision 0 (`StripeLog` data files,
-- `Set`/`Join` backups) used to force the version back to 0 on write, so a stored state lost its
-- skip degree on every round trip through the data file and a merge over a lopsided split was
-- split-dependent again. At revision 0 there is no peer to negotiate a version with: the stream is
-- self-describing, and the version pinned on the type must survive into it.

DROP TABLE IF EXISTS quantile_deterministic_stripe_log;

CREATE TABLE quantile_deterministic_stripe_log
(
    split UInt8,
    part UInt8,
    state AggregateFunction(medianDeterministic, UInt64, UInt64)
)
ENGINE = StripeLog;

-- A very lopsided split: without the skip degree in the data file it used to give a different answer.
INSERT INTO quantile_deterministic_stripe_log SELECT 2, 0, medianDeterministicState(number, number) FROM numbers(990000);
INSERT INTO quantile_deterministic_stripe_log SELECT 2, 1, medianDeterministicState(number, number) FROM numbers(990000, 10000);

-- One piece of every size at once.
INSERT INTO quantile_deterministic_stripe_log SELECT 4, 0, medianDeterministicState(number, number) FROM numbers(1);
INSERT INTO quantile_deterministic_stripe_log SELECT 4, 1, medianDeterministicState(number, number) FROM numbers(1, 999);
INSERT INTO quantile_deterministic_stripe_log SELECT 4, 2, medianDeterministicState(number, number) FROM numbers(1000, 9000);
INSERT INTO quantile_deterministic_stripe_log SELECT 4, 3, medianDeterministicState(number, number) FROM numbers(10000, 990000);

-- The pinned state version reaches the stored column.
SELECT DISTINCT toTypeName(state) FROM quantile_deterministic_stripe_log;

-- Split 0 is the value a single state over all the rows gives; every split must match it.
SELECT split, median
FROM
(
    SELECT 0 AS split, medianDeterministic(number, number) AS median FROM numbers(1000000)
    UNION ALL
    SELECT split, medianDeterministicMerge(state) AS median FROM quantile_deterministic_stripe_log GROUP BY split
)
ORDER BY split;

DROP TABLE quantile_deterministic_stripe_log;
