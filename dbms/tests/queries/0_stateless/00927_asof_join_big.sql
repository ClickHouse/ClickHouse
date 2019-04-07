USE test;

DROP TABLE IF EXISTS tvs;
DROP TABLE IF EXISTS trades;
DROP TABLE IF EXISTS keys;
DROP TABLE IF EXISTS tv_times;
DROP TABLE IF EXISTS trade_times;

CREATE TABLE keys(k UInt32) ENGINE = MergeTree() ORDER BY k;
INSERT INTO keys(k) SELECT number FROM system.numbers LIMIT 5000;

CREATE TABLE tv_times(t UInt32) ENGINE = MergeTree() ORDER BY t;
INSERT INTO tv_times(t) SELECT number * 3 FROM system.numbers LIMIT 50000;

CREATE TABLE trade_times(t UInt32) ENGINE = MergeTree() ORDER BY t;
INSERT INTO trade_times(t) SELECT number * 10  FROM system.numbers LIMIT 15000;

CREATE TABLE tvs(k UInt32, t UInt32, tv UInt64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO tvs(k,t,tv) SELECT k, t, t FROM keys CROSS JOIN tv_times;

CREATE TABLE trades(k UInt32, t UInt32, price UInt64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO trades(k,t,price) SELECT k, t, t FROM keys CROSS JOIN trade_times;

SELECT SUM(trades.price - tvs.tv) FROM trades ASOF LEFT JOIN tvs USING(k,t);


DROP TABLE tvs;
DROP TABLE trades;
DROP TABLE keys;
DROP TABLE tv_times;
DROP TABLE trade_times;
