DROP TABLE IF EXISTS t_reverse_order_virt_col;

CREATE TABLE t_reverse_order_virt_col (`order_0` Decimal(76, 53), `p_time` Date)
ENGINE = MergeTree PARTITION BY toYYYYMM(p_time)
ORDER BY order_0;

INSERT INTO t_reverse_order_virt_col SELECT number, '1984-01-01' FROM numbers(1000000);
SELECT DISTINCT _part FROM (SELECT _part FROM t_reverse_order_virt_col ORDER BY order_0 DESC);

DROP TABLE IF EXISTS t_reverse_order_virt_col;
