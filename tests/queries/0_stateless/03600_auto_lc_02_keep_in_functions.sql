DROP TABLE IF EXISTS t_auto_lc;

SET allow_experimental_statistics = 1;

CREATE TABLE t_auto_lc (id UInt64, s String STATISTICS(defaults, uniq), lc LowCardinality(String) STATISTICS(defaults, uniq))
ENGINE = MergeTree
ORDER BY id
SETTINGS use_statistics_for_serialization_info = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_auto_lc VALUES (1, 'foo', 'foo'), (2, 'barbar', 'barbar');

SELECT
    s,
    lc,
    length(s) AS s_len,
    length(lc) AS lc_len,
    toColumnTypeName(s),
    toColumnTypeName(lc),
    toColumnTypeName(s_len),
    toColumnTypeName(lc_len)
FROM t_auto_lc
ORDER BY id;

DROP TABLE IF EXISTS t_auto_lc;
