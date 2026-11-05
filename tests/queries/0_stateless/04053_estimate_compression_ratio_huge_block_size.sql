SELECT estimateCompressionRatio(9223372036854775806)(*) FROM numbers(1); -- { serverError BAD_QUERY_PARAMETER }
SELECT estimateCompressionRatio(268435457)(*) FROM numbers(1); -- { serverError BAD_QUERY_PARAMETER }
SELECT estimateCompressionRatio(268435456)(*) FROM numbers(100) FORMAT Null;
