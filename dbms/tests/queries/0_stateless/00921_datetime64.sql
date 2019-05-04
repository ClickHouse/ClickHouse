USE test;

DROP TABLE IF EXISTS A;
-- DROP TABLE IF EXISTS B;

CREATE TABLE A(t DateTime64, a Float64) ENGINE = MergeTree() ORDER BY t;
-- INSERT INTO A(t,a) VALUES (1,1),(50000,3);
INSERT INTO A(t,a) VALUES ('2019-05-03 11:25:25.123456789',5);
-- INSERT INTO A(t,a) VALUES (1556841600034000001,5);
-- INSERT INTO A(t,a) VALUES (now64(),5);

-- 1556841600034


-- CREATE TABLE B(k UInt32, t DateTime64, b Float64) ENGINE = MergeTree() ORDER BY (k, t);
-- INSERT INTO B(k,t,b) VALUES (2,40000,3);

SELECT toString(t, 'UTC'), toDate(t), toStartOfDay(t), toStartOfQuarter(t), toTime(t), toStartOfMinute(t), a FROM A ORDER BY t;

-- DROP TABLE B;
DROP TABLE A;
