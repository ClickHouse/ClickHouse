DROP TABLE IF EXISTS t1;

CREATE TABLE t1 
(
    a Float64 STATISTIC(tdigest),
    b Int64 STATISTIC(tdigest),
    pk String,
) Engine = MergeTree() ORDER BY pk; -- { serverError INCORRECT_QUERY }

SET allow_experimental_statistic = 1;

CREATE TABLE t1 
(
    a Float64 STATISTIC(tdigest),
    b Int64,
    pk String STATISTIC(tdigest),
) Engine = MergeTree() ORDER BY pk; -- { serverError ILLEGAL_STATISTIC }

CREATE TABLE t1 
(
    a Float64 STATISTIC(tdigest, tdigest(10)),
    b Int64,
) Engine = MergeTree() ORDER BY pk; -- { serverError INCORRECT_QUERY }

CREATE TABLE t1 
(
    a Float64 STATISTIC(xyz),
    b Int64,
) Engine = MergeTree() ORDER BY pk; -- { serverError INCORRECT_QUERY }

CREATE TABLE t1 
(
    a Float64,
    b Int64,
    pk String,
) Engine = MergeTree() ORDER BY pk; 

ALTER TABLE t1 ADD STATISTIC a TYPE xyz; -- { serverError INCORRECT_QUERY }
ALTER TABLE t1 ADD STATISTIC a TYPE tdigest;
ALTER TABLE t1 ADD STATISTIC a TYPE tdigest; -- { serverError ILLEGAL_STATISTIC }
ALTER TABLE t1 ADD STATISTIC pk TYPE tdigest; -- { serverError ILLEGAL_STATISTIC }
ALTER TABLE t1 DROP STATISTIC b TYPE tdigest; -- { serverError ILLEGAL_STATISTIC }
ALTER TABLE t1 DROP STATISTIC a TYPE tdigest;
ALTER TABLE t1 DROP STATISTIC a TYPE tdigest; -- { serverError ILLEGAL_STATISTIC }
ALTER TABLE t1 CLEAR STATISTIC a TYPE tdigest; -- { serverError ILLEGAL_STATISTIC }
ALTER TABLE t1 MATERIALIZE STATISTIC b TYPE tdigest; -- { serverError ILLEGAL_STATISTIC }

ALTER TABLE t1 ADD STATISTIC a TYPE tdigest;
ALTER TABLE t1 ADD STATISTIC b TYPE tdigest;
ALTER TABLE t1 MODIFY COLUMN a Float64 TTL toDateTime(b) + INTERVAL 1 MONTH;
ALTER TABLE t1 MODIFY COLUMN a Int64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t1;
