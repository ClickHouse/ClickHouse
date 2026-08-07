SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') = '61f0c404-5cb3-11e7-907b-a6006ad3dba0';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         = toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') = '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         = toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1');

SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') != '61f0c404-5cb3-11e7-907b-a6006ad3dba0';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         != toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') != '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         != toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1');


SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') < '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         < toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1');
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1') < '61f0c404-5cb3-11e7-907b-a6006ad3dba0';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba1'         < toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') < '61f0c404-5cb3-11e7-907b-a6006ad3dba0';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         < toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0');


SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') > '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         > toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1');
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba2') > '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba2'         > toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1');
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1') > '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba1'         > toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1');


SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') <= '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         <= toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1');
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') <= '61f0c404-5cb3-11e7-907b-a6006ad3dba0';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         <= toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba2') <= '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba2'         <= toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1');

SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') >= '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         >= toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1');
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') >= '61f0c404-5cb3-11e7-907b-a6006ad3dba0';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         >= toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba2') >= '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba2'         >= toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba1');



