SELECT transposeBits(emptyArrayFloat32());
SELECT transposeBits([toFloat32(0), toFloat32(0), toFloat32(0)]);
SELECT transposeBits(transposeBits([toFloat32(-16), toFloat32(-15), toFloat32(-14), toFloat32(-13), toFloat32(-12),
                                                    toFloat32(-11), toFloat32(-10), toFloat32(-9), toFloat32(-8), toFloat32(-7),
                                                    toFloat32(-6), toFloat32(-5), toFloat32(-4), toFloat32(-3), toFloat32(-2),
                                                    toFloat32(-1), toFloat32(0), toFloat32(1), toFloat32(2), toFloat32(3), toFloat32(4),
                                                    toFloat32(5), toFloat32(6), toFloat32(7), toFloat32(8), toFloat32(9), toFloat32(10),
                                                    toFloat32(11), toFloat32(12), toFloat32(13), toFloat32(14), toFloat32(15)]));
SELECT transposeBits([toFloat32(-3.0316488e-13), toFloat32(14660155000000.), toFloat32(-3.0316488e-13)]);
SELECT transposeBits([toFloat32(-3.0316488e-13), NULL]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT transposeBits([1, 2, 3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
