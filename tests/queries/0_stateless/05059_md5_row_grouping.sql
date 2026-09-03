-- Tags: no-openssl-fips
-- Test that `MD5` over a variable-length `String` column is unaffected by the order the rows are hashed in.

-- Rows are grouped by `MD5` block count inside a window before the kernel runs, so a digest must not
-- depend on which rows share a batch. `cityHash64` pairs each digest with its own row, so a digest that
-- lands in the wrong row's slot changes the sum; an aggregate over the digests alone would not see it.
SELECT sum(cityHash64(s, MD5(s))) FROM
(
    WITH number % 10 AS bucket,
         multiIf(bucket = 0, 0, bucket <= 6, 1 + number % 40, bucket <= 8, 200 + number % 301, 4000 + number % 201) AS len
    SELECT rightPad(toString(number), len, 'x') AS s FROM numbers(65536)
) SETTINGS max_block_size = 65536;

-- The same rows in blocks too small for the grouping to engage, so the in-order path must agree.
SELECT sum(cityHash64(s, MD5(s))) FROM
(
    WITH number % 10 AS bucket,
         multiIf(bucket = 0, 0, bucket <= 6, 1 + number % 40, bucket <= 8, 200 + number % 301, 4000 + number % 201) AS len
    SELECT rightPad(toString(number), len, 'x') AS s FROM numbers(65536)
) SETTINGS max_block_size = 64;

-- 65001 = 63 * 1024 + 489 and 489 is odd, so the last window is short of a full one and its last batch
-- is short of a full set of lanes at every batch width, covering the trailing partial batch and the
-- scatter that follows it.
SELECT sum(cityHash64(s, MD5(s))) FROM
(
    WITH number % 10 AS bucket,
         multiIf(bucket = 0, 0, bucket <= 6, 1 + number % 40, bucket <= 8, 200 + number % 301, 4000 + number % 201) AS len
    SELECT rightPad(toString(number), len, 'x') AS s FROM numbers(65001)
) SETTINGS max_block_size = 65536;
