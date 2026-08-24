DROP TABLE IF EXISTS table1;

CREATE TABLE table1 (str1 String, str2 String) ENGINE = Memory;

INSERT INTO table1 VALUES('qwerty', 'string');
INSERT INTO table1 VALUES('qqq', 'aaa');
INSERT INTO table1 VALUES('aasq', 'xxz');
INSERT INTO table1 VALUES('zxcqwer', '');
INSERT INTO table1 VALUES('', '');

select CRC32('string');
select CrC32('string'), crc32('test'); -- We want to test, that function name is case-insensitive
select CRC32(str1) from table1 order by CRC32(str1);
select CRC32(str2) from table1 order by CRC32(str2);
select CRC32(str1), CRC32(str2) from table1 order by CRC32(str1), CRC32(str2);
select str1, str2, CRC32(str1), CRC32(str2) from table1 order by CRC32(str1), CRC32(str2);

DROP TABLE table1;

SELECT 'CRC32IEEE()';
SELECT hex(CRC32IEEE('foo'));
SELECT 'CRC64()';
SELECT hex(CRC64('foo'));
