SET cast_keep_nullable = 0;

SELECT CAST(toNullable(toInt32(0)) AS Int32) as x, toTypeName(x);
SELECT CAST(toNullable(toInt8(0)) AS Int32) as x, toTypeName(x);

SET cast_keep_nullable = 1;

SELECT CAST(toNullable(toInt32(1)) AS Int32) as x, toTypeName(x);
SELECT CAST(toNullable(toInt8(1)) AS Int32) as x, toTypeName(x);

SELECT CAST(toNullable(toFloat32(2)), 'Float32') as x, toTypeName(x);
SELECT CAST(toNullable(toFloat32(2)), 'UInt8') as x, toTypeName(x);
SELECT CAST(toNullable(toFloat32(2)), 'UUID') as x, toTypeName(x); -- { serverError 70 }

SELECT CAST(if(1 = 1, toNullable(toInt8(3)), NULL) AS Int32) as x, toTypeName(x);
SELECT CAST(if(1 = 0, toNullable(toInt8(3)), NULL) AS Int32) as x, toTypeName(x);

SELECT CAST(a, 'Int32') as x, toTypeName(x) FROM (SELECT materialize(CAST(42, 'Nullable(UInt8)')) AS a);
SELECT CAST(a, 'Int32') as x, toTypeName(x) FROM (SELECT materialize(CAST(NULL, 'Nullable(UInt8)')) AS a);
