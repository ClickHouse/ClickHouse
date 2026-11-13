SET enable_analyzer = 1;
SET optimize_const_name_size = 256;

SELECT
    reinterpretAsUInt64(materialize(reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090560))))) AS float64_a,
    reinterpretAsUInt64(materialize(reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090561))))) AS float64_b,
    reinterpretAsUInt32(materialize(reinterpretAsFloat32(reinterpretAsFixedString(toUInt32(2143289344))))) AS float32_a,
    reinterpretAsUInt32(materialize(reinterpretAsFloat32(reinterpretAsFixedString(toUInt32(2143289345))))) AS float32_b;

SELECT
    reinterpretAsUInt64(arrayElement(materialize([
        reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090560))),
        reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090561)))
    ]), 2)) AS float64_different,
    reinterpretAsUInt64(arrayElement(materialize([
        reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090560))),
        reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090560)))
    ]), 2)) AS float64_same,
    reinterpretAsUInt32(arrayElement(materialize([
        reinterpretAsFloat32(reinterpretAsFixedString(toUInt32(2143289344))),
        reinterpretAsFloat32(reinterpretAsFixedString(toUInt32(2143289345)))
    ]), 2)) AS float32_different,
    reinterpretAsUInt32(arrayElement(materialize([
        reinterpretAsFloat32(reinterpretAsFixedString(toUInt32(2143289344))),
        reinterpretAsFloat32(reinterpretAsFixedString(toUInt32(2143289344)))
    ]), 2)) AS float32_same;

SELECT
    reinterpretAsUInt16(materialize(reinterpret(unhex('c07f'), 'BFloat16'))) AS bfloat16_a,
    reinterpretAsUInt16(materialize(reinterpret(unhex('c17f'), 'BFloat16'))) AS bfloat16_b;

SELECT
    reinterpretAsUInt16(arrayElement(materialize([
        reinterpret(unhex('c07f'), 'BFloat16'),
        reinterpret(unhex('c17f'), 'BFloat16')
    ]), 2)) AS bfloat16_different,
    reinterpretAsUInt16(arrayElement(materialize([
        reinterpret(unhex('c07f'), 'BFloat16'),
        reinterpret(unhex('c07f'), 'BFloat16')
    ]), 2)) AS bfloat16_same;
