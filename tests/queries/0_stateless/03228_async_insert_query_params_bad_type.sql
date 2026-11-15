DROP TABLE IF EXISTS t_async_insert_params;

CREATE TABLE t_async_insert_params (id UInt64) ENGINE = MergeTree ORDER BY tuple();

SET param_p1 = 'Hello';

SET async_insert = 1;
SET wait_for_async_insert = 1;

INSERT INTO t_async_insert_params VALUES ({p1:UInt64}); -- { serverError  BAD_QUERY_PARAMETER }
INSERT INTO t_async_insert_params VALUES ({p1:String}); -- { serverError  TYPE_MISMATCH }

ALTER TABLE t_async_insert_params MODIFY COLUMN id String;

INSERT INTO t_async_insert_params VALUES ({p1:UInt64}); -- { serverError  BAD_QUERY_PARAMETER }
INSERT INTO t_async_insert_params VALUES ({p1:String});

SELECT * FROM t_async_insert_params ORDER BY id;

DROP TABLE t_async_insert_params;
