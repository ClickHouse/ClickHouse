SELECT
    toFloat64('1.7091'),
    toFloat64('1.5008753E7'),
    toFloat64('6e-09'),
    toFloat64('6.000000000000001e-9'),
    toFloat32('1.7091'),
    toFloat32('1.5008753E7'),
    toFloat32('6e-09'),
    toFloat32('6.000000000000001e-9')
SETTINGS precise_float_parsing = 0;

SELECT
    toFloat64('1.7091'),
    toFloat64('1.5008753E7'),
    toFloat64('6e-09'),
    toFloat64('6.000000000000001e-9'),
    toFloat32('1.7091'),
    toFloat32('1.5008753E7'),
    toFloat32('6e-09'),
    toFloat32('6.000000000000001e-9')
SETTINGS precise_float_parsing = 1;
