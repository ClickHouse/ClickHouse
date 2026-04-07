
DROP TABLE IF EXISTS t1;

CREATE TABLE t1
(
    c1 DateTime DEFAULT now() NOT NULL,
    c2 DateTime DEFAULT now() NOT NULL,
    c3 DateTime DEFAULT now() NOT NULL,
    PRIMARY KEY(c1, c2, c3)
) ENGINE = MergeTree()
ORDER BY (c1, c2, c3);

INSERT INTO t1 (c1,c2,c3) VALUES(now() + INTERVAL '1 day 1 hour 1 minute 1 second', now(), now());

DROP TABLE t1;

CREATE TABLE t1 (n int, dt DateTime) ENGINE=Memory;

SET input_format_values_interpret_expressions=0;
INSERT INTO t1 VALUES (1, toDateTime('2023-07-20 21:53:01') + INTERVAL '1 day 1 hour 1 minute 1 second'), (2, toDateTime('2023-07-20 21:53:01') + INTERVAL '1 day');
INSERT INTO t1 VALUES (3, toDateTime('2023-07-20 21:53:01') + INTERVAL 1 DAY), (4, toDateTime('2023-07-20 21:53:01') + (toIntervalMinute(1), toIntervalSecond(1)));

SELECT * FROM t1 ORDER BY n;

DROP TABLE t1;
