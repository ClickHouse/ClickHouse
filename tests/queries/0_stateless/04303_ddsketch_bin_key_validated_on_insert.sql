-- A single DDSketch bin at an impossible key (2^31 - 2^10) must be rejected on INSERT, not stored to break a later merge
-- Payloads use gamma = 2.0, whose valid key range is ~[-1021, 1023]

DROP TABLE IF EXISTS t_ddsketch_bad;
CREATE TABLE t_ddsketch_bad (s AggregateFunction(quantilesDD(0.01, 0.5), Float64)) ENGINE = MergeTree ORDER BY tuple();

-- Bad key: rejected on insert, nothing persisted
INSERT INTO t_ddsketch_bad SELECT unhex('020000000000000040000000000000000001040180F0FFFF0F000000000000F03F030C000002040000000000000000')::AggregateFunction(quantilesDD(0.01, 0.5), Float64); -- { serverError INCORRECT_DATA }
SELECT count() FROM t_ddsketch_bad;

-- In-range key (100): inserts and reads back fine
INSERT INTO t_ddsketch_bad SELECT unhex('0200000000000000400000000000000000010401C801000000000000F03F030C000002040000000000000000')::AggregateFunction(quantilesDD(0.01, 0.5), Float64);
SELECT count() FROM t_ddsketch_bad;
SELECT quantilesDDMerge(0.01, 0.5)(s)[1] > 0 FROM t_ddsketch_bad;

-- Read path rejects the same bad state too
SELECT quantilesDDMerge(0.01, 0.5)(d) FROM (SELECT unhex('020000000000000040000000000000000001040180F0FFFF0F000000000000F03F030C000002040000000000000000')::AggregateFunction(quantilesDD(0.01, 0.5), Float64) AS d); -- { serverError INCORRECT_DATA }

-- Corrupted mapping with a huge (but finite) offset
SELECT quantilesDDMerge(0.01, 0.5)(d) FROM (SELECT unhex('02000000000000004000C84E676DC1AB4301040100000000000000F03F030C000002040000000000000000')::AggregateFunction(quantilesDD(0.01, 0.5), Float64) AS d); -- { serverError INCORRECT_DATA }

DROP TABLE t_ddsketch_bad;
