SELECT 'valid UTF-8 strings across the SIMD threshold';
SELECT count(), countIf(toValidUTF8(repeat('a', number)) = repeat('a', number)) FROM numbers(301);
SELECT count(), countIf(toValidUTF8(repeat('é', number)) = repeat('é', number)) FROM numbers(151);
SELECT count(), countIf(toValidUTF8(repeat('中文🙂', number)) = repeat('中文🙂', number)) FROM numbers(81);

SELECT 'invalid strings keep the existing replacement behavior';
SELECT countIf(toValidUTF8(concat(repeat('a', number), '\xC2')) = concat(repeat('a', number), '�')) FROM numbers(1, 128);
SELECT countIf(toValidUTF8(concat(repeat('a', number), '\xC2\xFF\x80')) = concat(repeat('a', number), '�')) FROM numbers(1, 128);
SELECT countIf(toValidUTF8(concat(repeat('a', 64), '\xE2\x28\xA1', repeat('b', 64))) = concat(repeat('a', 64), '�(�', repeat('b', 64))) FROM numbers(10);

SELECT 'validation is isolated to each ColumnString row';
SELECT
    countIf(toValidUTF8(value) = concat(repeat('a', 127), '�')),
    countIf(toValidUTF8(value) = concat('�', repeat('b', 127))),
    countIf(toValidUTF8(value) = repeat('é', 64))
FROM
(
    SELECT arrayJoin([
        concat(repeat('a', 127), '\xC2'),
        concat('\x80', repeat('b', 127)),
        repeat('é', 64)
    ]) AS value
);
