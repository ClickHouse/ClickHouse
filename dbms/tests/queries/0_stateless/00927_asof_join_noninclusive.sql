USE test;

DROP TABLE IF EXISTS A;
DROP TABLE IF EXISTS B;

CREATE TABLE A(k UInt32, t DateTime, a Float64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO A(k,t,a) VALUES (1,1,1),(1,2,2),(1,3,3),(1,4,4),(1,5,5);
INSERT INTO A(k,t,a) VALUES (2,1,1),(2,2,2),(2,3,3),(2,4,4),(2,5,5);

CREATE TABLE B(k UInt32, t DateTime, b Float64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO B(k,t,b) VALUES (1,2,2),(1,4,4);
INSERT INTO B(k,t,b) VALUES (2,3,3);

SELECT A.k, A.t, A.a, B.b, B.t, B.k FROM A ASOF LEFT JOIN B USING(k,t) ORDER BY (A.k, A.t);


DROP TABLE A;
DROP TABLE B;