SELECT toTypeName(true);
SELECT toTypeName(false);

SELECT not false;
SELECT not 1;
SELECT not 0;
SELECT not 100000000;
SELECT toTypeName(not false);
SELECT toTypeName(not 1);
SELECT toTypeName(not 0);
SELECT toTypeName(not 100000000);

SELECT false and true;
SELECT 1 and 10;
SELECT 0 and 100000000;
SELECT 1 and true;
SELECT toTypeName(false and true);
SELECT toTypeName(1 and 10);
SELECT toTypeName(0 and 10000000);
SELECT toTypeName(1 and true);

SELECT xor(false, true);
SELECT xor(1, 10);
SELECT xor(0, 100000000);
SELECT xor(1, true);
SELECT toTypeName(xor(false, true));
SELECT toTypeName(xor(1, 10));
SELECT toTypeName(xor(0, 10000000));
SELECT toTypeName(xor(1, true));

SELECT false or true;
SELECT 1 or 10;
SELECT 0 or 100000000;
SELECT 1 or true;
SELECT toTypeName(false or true);
SELECT toTypeName(1 or 10);
SELECT toTypeName(0 or 10000000);
SELECT toTypeName(1 or true);

SELECT toBool(100000000000);
SELECT toBool(0);
SELECT toBool(-10000000000);
SELECT toBool(100000000000.0000001);
SELECT toBool(toDecimal32(10.10, 2));
SELECT toBool(toDecimal64(100000000000.1, 2));
SELECT toBool(toDecimal32(0, 2));
SELECT toBool('true');
SELECT toBool('yes');
SELECT toBool('enabled');
SELECT toBool('enable');
SELECT toBool('on');
SELECT toBool('y');
SELECT toBool('t');

SELECT toBool('false');
SELECT toBool('no');
SELECT toBool('disabled');
SELECT toBool('disable');
SELECT toBool('off');
SELECT toBool('n');
SELECT toBool('f');

