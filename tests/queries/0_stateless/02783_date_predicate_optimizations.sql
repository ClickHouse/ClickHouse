CREATE TABLE source
(
    `ts` DateTime('UTC'),
    `n` Int32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY tuple();

INSERT INTO source values ('2021-12-31 23:00:00', 0);

SELECT * FROM source WHERE toYYYYMM(ts) = 202112;
SELECT * FROM source WHERE toYear(ts) = 2021;
