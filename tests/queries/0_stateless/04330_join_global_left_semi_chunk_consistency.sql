-- Tags: no-random-settings, no-old-analyzer
-- no-random-settings: keep max_joined_block_size_rows small to force the join-output block split.
-- no-old-analyzer: the mixed equi + inequality JOIN ON is only supported by the new analyzer.

SELECT count()
FROM
(
    SELECT t1.generate_series
    FROM numbers(41712) AS t0
    LEFT JOIN generateSeries(5297, 67368) AS t1
      ON (t0.number <= t1.generate_series) AND (t1.generate_series = t0.number)
    SETTINGS max_joined_block_size_rows = 1000
);

SELECT count()
FROM
(
    SELECT t1d0.generate_series, [100000000000000000000.]
    FROM numbers(41712) AS t0d0
    GLOBAL LEFT JOIN generateSeries(5297, 67368) AS t1d0
      ON (t0d0.number <= t1d0.generate_series) AND (t1d0.generate_series = t0d0.number)
    SEMI LEFT JOIN numbers_mt(14075) AS t2d0 USING (number)
    LIMIT 39 BY ALL
    SETTINGS max_joined_block_size_rows = 1000
);
