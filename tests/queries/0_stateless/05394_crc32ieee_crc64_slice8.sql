-- Slice-by-8 CRC32IEEE and CRC64 must match the previous byte-at-a-time checksums.
-- 7/8/9 bytes: before a chunk, one chunk, one chunk plus a tail byte.
-- 15/16/17 bytes: a chunk plus a 7-byte tail, two chunks, two chunks plus a tail byte.
-- Inputs contain NUL and bytes >= 0x80. The second 8-byte row is the payload from review.
-- Checksums below were computed by the byte-at-a-time implementation, not by this branch.
-- materialize() forces the String and FixedString column paths.

DROP TABLE IF EXISTS 05394_crc_slice8;

CREATE TABLE 05394_crc_slice8 (s String) ENGINE = Memory;

INSERT INTO 05394_crc_slice8 VALUES
    (unhex('00FF807F0100FE')),
    (unhex('00FF807F0100FE83')),
    (unhex('00FF807F0100FE83D6')),
    (unhex('00FF807F0100FE83D62E95D6DF6D4A')),
    (unhex('00FF807F0100FE83D62E95D6DF6D4A00')),
    (unhex('00FF807F0100FE83D62E95D6DF6D4A00FF')),
    (unhex('83D62E95D6DF6D4A'));

SELECT 'String';
SELECT length(s), hex(s), hex(CRC32IEEE(materialize(s))), hex(CRC64(materialize(s)))
FROM 05394_crc_slice8
ORDER BY length(s), hex(s);

SELECT 'FixedString';
SELECT length(s), hex(s), hex(CRC32IEEE(materialize(toFixedString(s, 7)))), hex(CRC64(materialize(toFixedString(s, 7))))
FROM (SELECT s FROM 05394_crc_slice8 WHERE length(s) = 7);

SELECT length(s), hex(s), hex(CRC32IEEE(materialize(toFixedString(s, 8)))), hex(CRC64(materialize(toFixedString(s, 8))))
FROM (SELECT s FROM 05394_crc_slice8 WHERE length(s) = 8)
ORDER BY hex(s);

SELECT length(s), hex(s), hex(CRC32IEEE(materialize(toFixedString(s, 9)))), hex(CRC64(materialize(toFixedString(s, 9))))
FROM (SELECT s FROM 05394_crc_slice8 WHERE length(s) = 9);

SELECT length(s), hex(s), hex(CRC32IEEE(materialize(toFixedString(s, 15)))), hex(CRC64(materialize(toFixedString(s, 15))))
FROM (SELECT s FROM 05394_crc_slice8 WHERE length(s) = 15);

SELECT length(s), hex(s), hex(CRC32IEEE(materialize(toFixedString(s, 16)))), hex(CRC64(materialize(toFixedString(s, 16))))
FROM (SELECT s FROM 05394_crc_slice8 WHERE length(s) = 16);

SELECT length(s), hex(s), hex(CRC32IEEE(materialize(toFixedString(s, 17)))), hex(CRC64(materialize(toFixedString(s, 17))))
FROM (SELECT s FROM 05394_crc_slice8 WHERE length(s) = 17);

DROP TABLE 05394_crc_slice8;
