DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int, INDEX i1 substring(sipHash128(c0), 1, 4) TYPE ngrambf_v1(1, 8, 1, 0));
INSERT INTO TABLE t0 (c0) SELECT number FROM numbers(28);
DROP TABLE t0;
