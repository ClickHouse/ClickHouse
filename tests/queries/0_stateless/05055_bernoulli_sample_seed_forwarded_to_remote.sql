-- Tags: no-random-settings

-- `bernoulli_sample_seed = 0` draws one random seed for the whole query. The seed lives in the
-- initiator's query context, which remote nodes do not share - they only receive the settings.
-- The initiator therefore freezes the drawn seed into the setting, so a remote read samples the
-- same rows as a local read of the same table. Without that, every node draws its own seed and
-- the two sides of the comparison below select different rows.

SET allow_experimental_bernoulli_sample = 1;
SET max_insert_threads = 1;

DROP TABLE IF EXISTS t_bernoulli_seed_remote;
CREATE TABLE t_bernoulli_seed_remote (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_bernoulli_seed_remote SELECT number FROM numbers(100000);

SELECT 'random seed is shared with a remote read';
SELECT
    (SELECT count() FROM t_bernoulli_seed_remote SAMPLE 0.1)
    =
    (SELECT count() FROM remote('127.0.0.1', currentDatabase(), t_bernoulli_seed_remote) SAMPLE 0.1)
SETTINGS bernoulli_sample_seed = 0;

SELECT 'the same holds for two remote reads';
SELECT
    (SELECT count() FROM remote('127.0.0.1', currentDatabase(), t_bernoulli_seed_remote) SAMPLE 0.1)
    =
    (SELECT count() FROM remote('127.0.0.2', currentDatabase(), t_bernoulli_seed_remote) SAMPLE 0.1)
SETTINGS bernoulli_sample_seed = 0;

SELECT 'an explicit seed is still honoured across the same pair of reads';
SELECT
    (SELECT count() FROM t_bernoulli_seed_remote SAMPLE 0.1)
    =
    (SELECT count() FROM remote('127.0.0.1', currentDatabase(), t_bernoulli_seed_remote) SAMPLE 0.1)
SETTINGS bernoulli_sample_seed = 42;

DROP TABLE t_bernoulli_seed_remote;
