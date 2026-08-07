set receive_timeout = '10', receive_data_timeout_ms = '10000', extremes = '1', allow_suspicious_low_cardinality_types = '1', force_primary_key = '1', join_use_nulls = '1', max_rows_to_read = '2', join_algorithm = 'partial_merge';

SELECT * FROM (SELECT dummy AS val FROM system.one) AS s1 ANY LEFT JOIN (SELECT toLowCardinality(dummy) AS rval FROM system.one) AS s2 ON (val + 9223372036854775806) = (rval * 1);
