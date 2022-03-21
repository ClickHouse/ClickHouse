-- Tags: no-fasttest

SELECT
    number,
    hex(base64Decode(base64Encode(repeat('a', number)))) r
FROM numbers(100);
