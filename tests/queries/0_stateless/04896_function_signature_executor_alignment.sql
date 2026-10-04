-- A declarative signature must accept exactly the types the legacy `getReturnTypeImpl` accepted,
-- and it must agree with the executor on the result type. These are the families where the two are
-- easy to drift apart: wide integers and `BFloat16`.

-- `exp` / `log` / `sigmoid` / `tanh` widen `BFloat16` to `Float64`, matching the executor.
SELECT
    toTypeName(exp(materialize(toBFloat16(1)))),
    toTypeName(log(materialize(toBFloat16(1)))),
    toTypeName(sigmoid(materialize(toBFloat16(1)))),
    toTypeName(tanh(materialize(toBFloat16(1))));

-- `UnaryFunctionVectorized` returns `Float64` for every numeric argument. Note that `exp`, `log`,
-- `sigmoid` and `tanh` are deliberately not used here: with `USE_FASTOPS` they keep the native
-- float width of the argument instead, so only their `BFloat16` behaviour above is build-independent.
SELECT
    toTypeName(cos(materialize(toFloat32(1)))),
    toTypeName(cos(materialize(toFloat64(1)))),
    toTypeName(cos(materialize(1))),
    toTypeName(cos(materialize(toDecimal32(1, 2))));

-- `always_returns_float64` implementations stay `Float64` for every argument type.
SELECT toTypeName(cos(materialize(toBFloat16(1)))), toTypeName(sin(materialize(toBFloat16(1))));

-- The tuple-or-array wrappers delegate type-only resolution to the selected carrier.
SELECT toTypeName(L2Norm([toFloat32(1)]));
SELECT toTypeName(LpNorm([toFloat32(1)], toUInt128(2)));
SELECT toTypeName(LpDistance([toFloat32(1)], [toFloat32(1)], toInt128(2)));

-- `char` accepts every integer and float family at analysis time, exactly as before the
-- declarative signature (`isInt() || isUInt() || isFloat()`); the column dispatch is what
-- rejects the families it has no branch for.
SELECT char(65), char(toFloat64(65)), char(toInt64(66));
SELECT char(toUInt128(65)); -- { serverError ILLEGAL_COLUMN }
SELECT char(toInt256(65)); -- { serverError ILLEGAL_COLUMN }
SELECT char(toBFloat16(65)); -- { serverError ILLEGAL_COLUMN }
SELECT char('65'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- `bitTestAll` / `bitTestAny` likewise: `isInteger` for the value and `isUInt` for every bit
-- position, wide integers included.
SELECT bitTestAll(toUInt8(3), 0, 1), bitTestAny(toUInt8(2), 0, 1);
SELECT bitTestAll(toUInt128(1), 0); -- { serverError ILLEGAL_COLUMN }
SELECT bitTestAll(toInt128(3), 0); -- { serverError ILLEGAL_COLUMN }
SELECT bitTestAll(1, toUInt128(0)); -- { serverError ILLEGAL_COLUMN }
SELECT bitTestAll(1, materialize(toUInt128(0))); -- { serverError ILLEGAL_COLUMN }
SELECT bitTestAll(1, -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT bitTestAll('1', 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The JSON functions build their result type themselves, so the return type of a direct
-- resolution must keep the `Nullable` wrapper the resolver adds and accept a `Dynamic` document.
SELECT toTypeName(JSONHas(materialize(CAST('{"a":1}', 'Nullable(String)')), 'a'));
SELECT toTypeName(JSONLength(materialize(CAST('{"a":1}', 'Nullable(String)'))));
SELECT toTypeName(JSONExtract(materialize(CAST('{"a":1}', 'Nullable(String)')), 'a', 'UInt64'));
SELECT toTypeName(JSONHas(materialize(CAST('{"a":1}', 'Dynamic')), 'a'));
SELECT JSONExtract(materialize(CAST('{"a":1}', 'Nullable(String)')), 'a', 'UInt64');
SELECT JSONExtract(materialize(CAST(NULL, 'Nullable(String)')), 'a', 'UInt64');
