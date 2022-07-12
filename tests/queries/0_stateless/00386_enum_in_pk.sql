DROP TABLE IF EXISTS enum_pk;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE enum_pk (date Date DEFAULT '0000-00-00', x Enum8('0' = 0, '1' = 1, '2' = 2), d Enum8('0' = 0, '1' = 1, '2' = 2)) ENGINE = MergeTree(date, x, 1);
INSERT INTO enum_pk (x, d) VALUES ('0', '0')('1', '1')('0', '0')('1', '1')('1', '1')('0', '0')('0', '0')('2', '2')('0', '0')('1', '1')('1', '1')('1', '1')('1', '1')('0', '0');

SELECT cityHash64(groupArray(x)) FROM enum_pk WHERE x = '0';
SELECT cityHash64(groupArray(d)) FROM enum_pk WHERE d = '0';

SELECT cityHash64(groupArray(x)) FROM enum_pk WHERE x != '0';
SELECT cityHash64(groupArray(d)) FROM enum_pk WHERE d != '0';

SELECT cityHash64(groupArray(x)) FROM enum_pk WHERE x = '1';
SELECT cityHash64(groupArray(d)) FROM enum_pk WHERE d = '1';

SELECT cityHash64(groupArray(x)) FROM enum_pk WHERE exp2(toInt64(x != '1')) > 1;
SELECT cityHash64(groupArray(d)) FROM enum_pk WHERE exp2(toInt64(d != '1')) > 1;

SELECT cityHash64(groupArray(x)) FROM enum_pk WHERE x = toString(0);
SELECT cityHash64(groupArray(d)) FROM enum_pk WHERE d = toString(0);

SELECT cityHash64(groupArray(x)) FROM enum_pk WHERE (x = toString(0)) > 0;
SELECT cityHash64(groupArray(d)) FROM enum_pk WHERE (d = toString(0)) > 0;

SELECT cityHash64(groupArray(x)) FROM enum_pk WHERE ((x != toString(1)) > 0) > 0;
SELECT cityHash64(groupArray(d)) FROM enum_pk WHERE ((d != toString(1)) > 0) > 0;

SELECT cityHash64(groupArray(x)) FROM enum_pk WHERE exp2((x != toString(0)) != 0) > 1;
SELECT cityHash64(groupArray(d)) FROM enum_pk WHERE exp2((d != toString(0)) != 0) > 1;

SELECT cityHash64(groupArray(x)) FROM enum_pk WHERE (-(x != toString(0)) = -1) > 0;
SELECT cityHash64(groupArray(d)) FROM enum_pk WHERE (-(d != toString(0)) = -1) > 0;

SELECT cityHash64(groupArray(x)) FROM enum_pk WHERE 1 = 1;
SELECT cityHash64(groupArray(d)) FROM enum_pk WHERE 1 = 1;

SELECT cityHash64(groupArray(x)) FROM enum_pk WHERE (x = '0' OR x = '1');
SELECT cityHash64(groupArray(d)) FROM enum_pk WHERE (d = '0' OR d = '1');

SELECT cityHash64(groupArray(x)) FROM enum_pk WHERE x IN ('0', '1');
SELECT cityHash64(groupArray(d)) FROM enum_pk WHERE d IN ('0', '1');

SELECT cityHash64(groupArray(x)) FROM enum_pk WHERE (x != '0' AND x != '1');
SELECT cityHash64(groupArray(d)) FROM enum_pk WHERE (d != '0' AND d != '1');

DROP TABLE enum_pk;
