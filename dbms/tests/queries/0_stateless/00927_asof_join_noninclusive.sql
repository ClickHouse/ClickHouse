DROP TABLE IF EXISTS A;
DROP TABLE IF EXISTS B;

CREATE TABLE A(k UInt32, t DateTime, a Float64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO A(k,t,a) VALUES (1,1,1),(1,2,2),(1,3,3),(1,4,4),(1,5,5);  -- multiple joined values
INSERT INTO A(k,t,a) VALUES (2,1,1),(2,2,2),(2,3,3),(2,4,4),(2,5,5);  -- one joined value
INSERT INTO A(k,t,a) VALUES (3,1,1),(3,2,2),(3,3,3),(3,4,4),(3,5,5);  -- no joined values

CREATE TABLE B(k UInt32, t DateTime, b Float64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO B(k,t,b) VALUES (1,2,2),(1,4,4);
INSERT INTO B(k,t,b) VALUES (2,3,3);

SELECT A.k, toString(A.t, 'UTC'), A.a, B.b, toString(B.t, 'UTC'), B.k FROM A ASOF LEFT JOIN B USING(k,t) ORDER BY (A.k, A.t);

SELECT A.k, toString(A.t, 'UTC'), A.a, B.b, toString(B.t, 'UTC'), B.k FROM A ASOF INNER JOIN B ON A.k == B.k AND A.t >= B.t ORDER BY (A.k, A.t);

SELECT A.k, toString(A.t, 'UTC'), A.a, B.b, toString(B.t, 'UTC'), B.k FROM A ASOF JOIN B USING(k,t) ORDER BY (A.k, A.t);

DROP TABLE A;
DROP TABLE B;
