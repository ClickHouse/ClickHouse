-- Tags: no-replicated-database, no-parallel, no-fasttest

SET allow_experimental_analyzer = 1;
SET allow_experimental_live_view = 1;

DROP TABLE IF EXISTS lv;
DROP TABLE IF EXISTS lv2;
DROP TABLE IF EXISTS mt;

CREATE TABLE mt (a Int32) Engine=MergeTree order by tuple();
CREATE LIVE VIEW lv AS SELECT sum(a) AS sum_a FROM mt PREWHERE a > 1;
CREATE LIVE VIEW lv2 AS SELECT sum(number) AS sum_number FROM system.numbers PREWHERE number > 1; -- { serverError 182 }

INSERT INTO mt VALUES (1),(2),(3);

SELECT *, _version FROM lv;
SELECT *, _version FROM lv PREWHERE sum_a > 5; -- { serverError 182 }

INSERT INTO mt VALUES (1),(2),(3);

SELECT *, _version FROM lv;
SELECT *, _version FROM lv PREWHERE sum_a > 10; -- { serverError 182 }

DROP TABLE lv;
DROP TABLE mt;
