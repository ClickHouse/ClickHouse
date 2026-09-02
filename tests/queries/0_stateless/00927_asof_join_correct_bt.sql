DROP TABLE IF EXISTS A;
DROP TABLE IF EXISTS B;

CREATE TABLE A(k UInt32, t UInt32, a UInt64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO A(k,t,a) VALUES (1,101,1),(1,102,2),(1,103,3),(1,104,4),(1,105,5);

CREATE TABLE B1(k UInt32, t UInt32, b UInt64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO B1(k,t,b) VALUES (1,102,2), (1,104,4);

CREATE TABLE B2(t UInt32, k UInt32, b UInt64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO B2(k,t,b) VALUES (1,102,2), (1,104,4);

CREATE TABLE B3(k UInt32, b UInt64, t UInt32) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO B3(k,t,b) VALUES (1,102,2), (1,104,4);

-- { echoOn }
SELECT A.k, A.t, A.a, B.b, B.t, B.k FROM A ASOF LEFT JOIN B1 B USING(k,t) ORDER BY (A.k, A.t);
SELECT A.k, A.t, A.a, B.b, B.t, B.k FROM A ASOF LEFT JOIN B2 B USING(k,t) ORDER BY (A.k, A.t);
SELECT A.k, A.t, A.a, B.b, B.t, B.k FROM A ASOF LEFT JOIN B3 B USING(k,t) ORDER BY (A.k, A.t);

SET join_algorithm = 'full_sorting_merge';
SELECT A.k, A.t, A.a, B.b, B.t, B.k FROM A ASOF LEFT JOIN B1 B USING(k,t) ORDER BY (A.k, A.t);
SELECT A.k, A.t, A.a, B.b, B.t, B.k FROM A ASOF LEFT JOIN B2 B USING(k,t) ORDER BY (A.k, A.t);
SELECT A.k, A.t, A.a, B.b, B.t, B.k FROM A ASOF LEFT JOIN B3 B USING(k,t) ORDER BY (A.k, A.t);

-- { echoOff }

DROP TABLE B1;
DROP TABLE B2;
DROP TABLE B3;

DROP TABLE A;
