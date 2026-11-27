-- https://github.com/ClickHouse/ClickHouse/issues/60772
SELECT toFixedString(toFixedString(toFixedString(toFixedString(toFixedString(toFixedString('%W', 2), 2), 2),toLowCardinality(toLowCardinality(toNullable(2)))), 2), 2),
       toFixedString(toFixedString('2018-01-02 22:33:44', 19), 19),
       hasSubsequence(toNullable(materialize(toLowCardinality('garbage'))), 'gr')
GROUP BY
    '2018-01-02 22:33:44',
    toFixedString(toFixedString('2018-01-02 22:33:44', 19), 19),
    'gr',
    '2018-01-02 22:33:44'
SETTINGS enable_analyzer = 1;

-- WITH CUBE (note that result is different with the analyzer (analyzer is correct including all combinations)
SELECT
    toFixedString(toFixedString(toFixedString(toFixedString(toFixedString(toFixedString('%W', 2), 2), 2), toLowCardinality(toLowCardinality(toNullable(2)))), 2), 2),
    toFixedString(toFixedString('2018-01-02 22:33:44', 19), 19),
    hasSubsequence(toNullable(materialize(toLowCardinality('garbage'))), 'gr')
GROUP BY
    '2018-01-02 22:33:44',
    toFixedString(toFixedString('2018-01-02 22:33:44', 19), 19),
    'gr',
    '2018-01-02 22:33:44'
WITH CUBE
SETTINGS enable_analyzer = 1;
