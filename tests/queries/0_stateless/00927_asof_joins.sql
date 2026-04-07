DROP TABLE IF EXISTS md;
DROP TABLE IF EXISTS tv;

CREATE TABLE md(key UInt32, t DateTime, bid Float64, ask Float64) ENGINE = MergeTree() ORDER BY (key, t);
INSERT INTO md(key,t,bid,ask) VALUES (1,20,7,8),(1,5,1,2),(1,10,11,12),(1,15,5,6);
INSERT INTO md(key,t,bid,ask) VALUES (2,20,17,18),(2,5,11,12),(2,10,21,22),(2,15,5,6);

CREATE TABLE tv(key UInt32, t DateTime, tv Float64) ENGINE = MergeTree() ORDER BY (key, t);
INSERT INTO tv(key,t,tv) VALUES (1,5,1.5),(1,6,1.51),(1,10,11.5),(1,11,11.51),(1,15,5.5),(1,16,5.6),(1,20,7.5);
INSERT INTO tv(key,t,tv) VALUES (2,5,2.5),(2,6,2.51),(2,10,12.5),(2,11,12.51),(2,15,6.5),(2,16,5.6),(2,20,8.5);

SELECT tv.key, toString(tv.t, 'UTC'), md.bid, tv.tv, md.ask FROM tv ASOF LEFT JOIN md USING(key,t) ORDER BY (tv.key, tv.t)
;

SELECT '-';

SELECT tv.key, toString(tv.t, 'UTC'), md.bid, tv.tv, md.ask FROM tv ASOF LEFT JOIN md USING(key,t) ORDER BY (tv.key, tv.t)
SETTINGS join_algorithm = 'full_sorting_merge';

DROP TABLE md;
DROP TABLE tv;
