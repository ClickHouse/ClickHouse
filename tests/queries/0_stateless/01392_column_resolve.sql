-- Tags: no-parallel

DROP DATABASE IF EXISTS test_01392;
CREATE DATABASE test_01392;

CREATE TABLE test_01392.tableConversion (conversionId String, value Nullable(Double)) ENGINE = Log();
CREATE TABLE test_01392.tableClick (clickId String, conversionId String, value Nullable(Double)) ENGINE = Log();
CREATE TABLE test_01392.leftjoin (id String) ENGINE = Log();

INSERT INTO test_01392.tableConversion(conversionId, value) VALUES ('Conversion 1', 1);
INSERT INTO test_01392.tableClick(clickId, conversionId, value) VALUES ('Click 1', 'Conversion 1', 14);
INSERT INTO test_01392.tableClick(clickId, conversionId, value) VALUES ('Click 2', 'Conversion 1', 15);
INSERT INTO test_01392.tableClick(clickId, conversionId, value) VALUES ('Click 3', 'Conversion 1', 16);

SELECT
    conversion.conversionId AS myConversionId,
    click.clickId AS myClickId,
    click.myValue AS myValue
FROM (
    SELECT conversionId, value as myValue
    FROM test_01392.tableConversion
) AS conversion
INNER JOIN (
    SELECT clickId, conversionId, value as myValue
    FROM test_01392.tableClick
) AS click ON click.conversionId = conversion.conversionId
LEFT JOIN (
    SELECT * FROM test_01392.leftjoin
) AS dummy ON (dummy.id = conversion.conversionId)
ORDER BY myValue;

DROP TABLE test_01392.tableConversion;
DROP TABLE test_01392.tableClick;
DROP TABLE test_01392.leftjoin;

DROP DATABASE test_01392;
