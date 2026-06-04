-- Tags: no-fasttest
-- Tag no-fasttest: depends on cld2

-- https://github.com/ClickHouse/ClickHouse/issues/64931
SELECT detectLanguageMixed(materialize('二兎を追う者は一兎をも得ず二兎を追う者は一兎をも得ず A vaincre sans peril, on triomphe sans gloire.'))
GROUP BY
    GROUPING SETS (
        ('a', toUInt256(1)),
        (stringToH3(toFixedString(toFixedString('85283473ffffff', 14), 14))))
SETTINGS allow_experimental_nlp_functions = 1;
