SET enable_time_time64_type = 1;

CREATE OR REPLACE TABLE tx (c0 Time) ENGINE = Memory;
INSERT INTO TABLE tx (c0) VALUES ('-1:00:00');
SELECT c0 FROM tx;

DROP TABLE tx;
