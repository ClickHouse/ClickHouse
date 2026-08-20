-- Regression test: TABLE_FUNCTION node must not cause a logical error
-- when it appears as a duplicated alias in expression context.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=98443&sha=04c134a5a17fd8e846c7b14e6c3a17094583fe53&name_0=PR&name_1=BuzzHouse%20%28amd_ubsan%29

SELECT 1 FROM (SELECT 1 FROM (SELECT 1 PREWHERE (SELECT 1 FROM VALUES(NULL) AS t0d2) QUALIFY (SELECT 1 FROM VALUES(NULL) AS t0d2))); -- { serverError ILLEGAL_PREWHERE }
