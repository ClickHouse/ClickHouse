SELECT
    encodeURLComponent('кликхаус') AS encoded,
    decodeURLComponent(encoded) = 'кликхаус' AS expected_EQ;

SELECT DISTINCT decodeURLComponent(encodeURLComponent(randomString(100) AS x)) = x FROM numbers(100000);
