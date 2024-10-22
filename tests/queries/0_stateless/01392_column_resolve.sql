CREATE TABLE tableConversion (conversionId String, value Nullable(Double)) ENGINE = Log();
CREATE TABLE tableClick (clickId String, conversionId String, value Nullable(Double)) ENGINE = Log();
CREATE TABLE leftjoin (id String) ENGINE = Log();

INSERT INTO tableConversion(conversionId, value) VALUES ('Conversion 1', 1);
INSERT INTO tableClick(clickId, conversionId, value) VALUES ('Click 1', 'Conversion 1', 14);
INSERT INTO tableClick(clickId, conversionId, value) VALUES ('Click 2', 'Conversion 1', 15);
INSERT INTO tableClick(clickId, conversionId, value) VALUES ('Click 3', 'Conversion 1', 16);

SELECT
    conversion.conversionId AS myConversionId,
    click.clickId AS myClickId,
    click.myValue AS myValue
FROM (
    SELECT conversionId, value as myValue
    FROM tableConversion
) AS conversion
INNER JOIN (
    SELECT clickId, conversionId, value as myValue
    FROM tableClick
) AS click ON click.conversionId = conversion.conversionId
LEFT JOIN (
    SELECT * FROM leftjoin
) AS dummy ON (dummy.id = conversion.conversionId)
ORDER BY myValue;
