-- The smallest and largest integers a Time64(0) column can store must render as the saturated time.

-- Pinned at its own default on purpose, not redundantly: the stress runner injects a random
-- compatibility='NN.N', which reverts this setting for versions before 25.12 and rejects the type.
SET enable_time_time64_type = 1;

SELECT * FROM format(Values, 'x Time64(0)', '(-9223372036854775808)');
SELECT * FROM format(Values, 'x Time64(0)', '(9223372036854775807)');
