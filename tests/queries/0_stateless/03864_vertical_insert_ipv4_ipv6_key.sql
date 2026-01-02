-- Test: IPv4/IPv6 key columns are supported in vertical insert ORDER BY.
DROP TABLE IF EXISTS t_vi_ip_key;

CREATE TABLE t_vi_ip_key
(
    ip4 IPv4,
    ip6 IPv6,
    v UInt8
)
ENGINE = MergeTree
ORDER BY (ip4, ip6)
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_ip_key VALUES
    ('10.0.0.2', '::1', 2),
    ('192.168.0.1', '2001:db8::2', 3),
    ('10.0.0.1', '2001:db8::1', 1);

SELECT ip4, ip6, v FROM t_vi_ip_key ORDER BY ip4, ip6;

DROP TABLE t_vi_ip_key;
