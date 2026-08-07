select transform(number, [1], [toFloat32(1)], toFloat32(1)) from numbers(3);
SELECT '---';
select transform(number, [3], [toFloat32(1)], toFloat32(1)) from numbers(6);
