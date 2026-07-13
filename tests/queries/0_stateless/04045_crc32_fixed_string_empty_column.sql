-- Regression test: CRC32 on FixedString with empty column (0 rows) caused global-buffer-overflow during header evaluation.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=99724&sha=f4c9f771b011692ba3cc774de981525dfc3d50fb&name_0=PR&name_1=AST%20fuzzer%20%28arm_asan%29
SELECT toLowCardinality(toNullable(7)), CRC32(toFixedString(materialize('string'), 65537));
