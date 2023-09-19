DROP TABLE IF EXISTS t1;

CREATE TABLE t1 
(
    a Float64,
    b Int64,
    pk String,
    STATISTIC a, b TYPE tdigest,
) Engine = MergeTree() ORDER BY pk; -- { serverError INCORRECT_QUERY }

SET allow_experimental_statistic = 1;

CREATE TABLE t1 
(
    a Float64,
    b Int64,
    pk String,
    STATISTIC a, a TYPE tdigest,
) Engine = MergeTree() ORDER BY pk; -- { serverError ILLEGAL_STATISTIC }

CREATE TABLE t1 
(
    a Float64,
    b Int64,
    pk String,
    STATISTIC a, pk TYPE tdigest,
) Engine = MergeTree() ORDER BY pk; -- { serverError ILLEGAL_STATISTIC }

CREATE TABLE t1 
(
    a Float64,
    b Int64,
    pk String,
) Engine = MergeTree() ORDER BY pk; 

ALTER TABLE t1 ADD STATISTIC a TYPE tdigest;
ALTER TABLE t1 ADD STATISTIC a TYPE tdigest; -- { serverError INCORRECT_QUERY }
ALTER TABLE t1 ADD STATISTIC pk tdigest; -- { serverError ILLEGAL_STATISTIC }
ALTER TABLE t1 DROP STATISTIC b TYPE tdigest; -- { serverError INCORRECT_QUERY }
ALTER TABLE t1 MATERIALIZE STATISTIC b TYPE tdigest; -- { serverError INCORRECT_QUERY }

DROP TABLE t1;
