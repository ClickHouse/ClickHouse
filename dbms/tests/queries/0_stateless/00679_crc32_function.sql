USE test;

DROP TABLE IF EXISTS table1;

CREATE TABLE table1 (str1 String, str2 String) ENGINE = Memory;

INSERT INTO table1 VALUES('qwerty', 'string');
INSERT INTO table1 VALUES('qqq', 'aaa');
INSERT INTO table1 VALUES('aasq', 'xxz');
INSERT INTO table1 VALUES('zxcqwer', '');
INSERT INTO table1 VALUES('', '');

select crc32('string');
select crc32('string'), crc32('test');
select crc32(str1) from table1 order by crc32(str1);
select crc32(str2) from table1 order by crc32(str2);
select crc32(str1), crc32(str2) from table1 order by crc32(str1), crc32(str2);
select str1, str2, crc32(str1), crc32(str2) from table1 order by crc32(str1), crc32(str2);

DROP TABLE table1;
