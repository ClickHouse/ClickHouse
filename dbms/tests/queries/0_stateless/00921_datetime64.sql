USE test;

DROP TABLE IF EXISTS A;

CREATE TABLE A(t DateTime64) ENGINE = MergeTree() ORDER BY t;
INSERT INTO A(t) VALUES (1556879125123456789);
INSERT INTO A(t) VALUES ('2019-05-03 11:25:25.123456789');

SELECT toString(t, 'UTC'), toDate(t), toStartOfDay(t), toStartOfQuarter(t), toTime(t), toStartOfMinute(t) FROM A ORDER BY t;

DROP TABLE A;
 -- issue toDate does a reinterpret_cast of the datetime64 which is incorrect
-- for the example above, it returns 2036-08-23 which is 0x5F15 days after epoch
-- the datetime64 is 0x159B2550CB345F15