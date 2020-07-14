DROP TABLE IF EXISTS tableConversion;
DROP TABLE IF EXISTS tableClick;
DROP TABLE IF EXISTS leftjoin;

CREATE TABLE default.tableConversion (conversionId String, value Nullable(Double)) ENGINE = Log();
CREATE TABLE default.tableClick (clickId String, conversionId String, value Nullable(Double)) ENGINE = Log();
CREATE TABLE default.leftjoin (id String) ENGINE = Log();

INSERT INTO default.tableConversion(conversionId, value) VALUES ('Conversion 1', 1);
INSERT INTO default.tableClick(clickId, conversionId, value) VALUES ('Click 1', 'Conversion 1', 14);
INSERT INTO default.tableClick(clickId, conversionId, value) VALUES ('Click 2', 'Conversion 1', 15);
INSERT INTO default.tableClick(clickId, conversionId, value) VALUES ('Click 3', 'Conversion 1', 16);

SELECT
    conversion.conversionId AS myConversionId,
    click.clickId AS myClickId,
    click.myValue AS myValue
FROM (
    SELECT conversionId, value as myValue
    FROM default.tableConversion
) AS conversion
INNER JOIN (
    SELECT clickId, conversionId, value as myValue
    FROM default.tableClick
) AS click ON click.conversionId = conversion.conversionId
LEFT JOIN (
    SELECT * FROM default.leftjoin
) AS dummy ON (dummy.id = conversion.conversionId)
ORDER BY myValue;

DROP TABLE IF EXISTS tableConversion;
DROP TABLE IF EXISTS tableClick;
DROP TABLE IF EXISTS leftjoin;
