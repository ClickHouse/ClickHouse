DROP TABLE IF EXISTS sometable;

CREATE TABLE sometable (
    date Date,
    time Int64,
    value UInt64
) ENGINE=MergeTree()
ORDER BY time;


INSERT INTO sometable (date, time, value) VALUES ('2019-11-08', 1573185600, 100);

SELECT COUNT() from sometable;

INSERT INTO sometable (date, time, value, time) VALUES ('2019-11-08', 1573185600, 100, 1573185600); -- {serverError 15}

DROP TABLE IF EXISTS sometable;
