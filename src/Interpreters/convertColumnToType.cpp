#include <Interpreters/convertColumnToType.h>

#include <Interpreters/convertFieldToType.h>
#include <Interpreters/castColumn.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime.h>
#include <base/DayNum.h>
#include <Common/Exception.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/DateLUTImpl.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TYPE_MISMATCH;
}

namespace
{

/// Column-native conversion for the cases where CAST provably matches `convertFieldToType`:
/// native-numeric and wide-integer (`Int128`/`Int256`/`UInt128`/`UInt256`) values, in the default AND
/// `strict` modes. `castColumnAccurateOrNull` uses the same accurate numeric conversion
/// (`accurate::convertNumeric`) as `convertFieldToType`'s default path (out-of-range / inexact-narrowing
/// -> NULL), and `convertFieldToType` dispatches wide integers through that same template, so they agree.
/// For these types `strict` and default agree with it too: `strict` only rejects additional cases for
/// types that are already excluded here - `Bool` (10 is not a valid `Bool`) and `Decimal` (scale
/// reduction; `castColumnAccurateOrNull` would round, so it must NOT be used for strict Decimal - and it
/// isn't, `Decimal` is excluded). The strict equivalence is pinned by `gtest_convert_column_to_type`. Returns:
///   - the converted size-1 column of `to` on success,
///   - a null `ColumnPtr{}` when not representable,
///   - std::nullopt when this fast path does not apply (caller falls back to the `Field` path).
/// Excluded: `convert_inexact_floats` mode (allows rounding, so `castColumnAccurateOrNull` differs),
/// `Bool` (clamp/validity semantics), `Decimal`/`BFloat16`, and anything non-numeric
/// (Date/Enum/String/wrappers/composite).
std::optional<ColumnPtr> tryConvertNumericColumnNative(
    const IColumn & value,
    const DataTypePtr & from,
    const DataTypePtr & to,
    bool convert_inexact_floats)
{
    if (convert_inexact_floats)
        return std::nullopt;
    /// Native numbers plus wide integers (`Int128`/`Int256`/`UInt128`/`UInt256`): `convertFieldToType`
    /// routes wide-integer sources and targets through the very same `accurate::convertNumeric<From, To>`
    /// as native numbers, so `castColumnAccurateOrNull` matches it for them too (default AND `strict`).
    /// `Decimal` and `BFloat16` stay on the `Field` path (a strict `Decimal` must reject scale loss that
    /// the accurate cast would round; `BFloat16` has its own rounding semantics).
    auto is_native_or_wide_number = [](const DataTypePtr & type) { return isNativeNumber(type) || isInteger(type); };
    if (!is_native_or_wide_number(from) || !is_native_or_wide_number(to) || isBool(from) || isBool(to))
        return std::nullopt;
    /// `strict` is intentionally not a parameter: for native numbers it is equivalent to the default
    /// accurate path (both reject non-representable values), so this fast path serves strict too.

    ColumnWithTypeAndName arg{value.getPtr(), from, ""};
    ColumnPtr casted = castColumnAccurateOrNull(arg, to);
    /// `ExecutableFunctionCast` uses the default implementation for constants, so a `ColumnConst`
    /// argument yields a `ColumnConst` result. Callers already pass a full column, but unwrap here too
    /// so the `assert_cast` below is correct regardless of the argument's constness.
    casted = casted->convertToFullColumnIfConst();
    const auto & nullable = assert_cast<const ColumnNullable &>(*casted);
    if (nullable.isNullAt(0))
        return ColumnPtr{};
    return nullable.getNestedColumnPtr();
}

/// Faithful column-native `X -> Decimal` conversion. Reuses the exact public primitives that
/// `convertFieldToType`'s Decimal helpers call (`DataTypeDecimal::canStoreWhole`/`getScaleMultiplier`,
/// `convertToDecimal`, `convertDecimals`, and `accurateEquals` for the strict exactness check), reading
/// the value straight from the size-1 column instead of materializing a `Field`. Mirrors
/// `convertDecimalType` (and its `convertIntToDecimalType`/`convertFloatToDecimalType`/
/// `convertDecimalToDecimalType` cases): an integer or float too big for the target throws
/// `ARGUMENT_OUT_OF_BOUND` exactly as there. Returns the converted size-1 column, a null `ColumnPtr{}`
/// for a strict-rejected lossy value, or `std::nullopt` when the source is not handled here (caller
/// uses the `Field` fallback). Pinned by `gtest_convert_column_to_type`.
template <typename ToDataType>
std::optional<ColumnPtr> convertToDecimalColumnNative(
    const IColumn & value, const DataTypePtr & from, const ToDataType & to_type, bool strict)
{
    using ToField = typename ToDataType::FieldType;
    const UInt32 to_scale = to_type.getScale();

    auto build = [&](const ToField & converted) -> ColumnPtr
    {
        auto column = to_type.createColumn();
        assert_cast<ColumnDecimal<ToField> &>(*column).getData().push_back(converted);
        return column;
    };

    /// integers (native + wide): `int -> Decimal` is exact, so `strict` adds no check here.
    auto from_integer = [&]<typename From>(const From & v) -> ColumnPtr
    {
        if (!to_type.canStoreWhole(v))
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Number is too big to place in {}", to_type.getName());
        return build(to_type.getScaleMultiplier() * ToField(static_cast<typename ToField::NativeType>(v)));
    };

    /// Decimals: mirror `convertDecimalToDecimalType` + the strict `accurateEquals` lossy-reject.
    auto from_decimal = [&]<typename FromField>(const FromField & src, UInt32 from_scale) -> ColumnPtr
    {
        using FromDataType = DataTypeDecimal<FromField>;
        const ToField converted = convertDecimals<FromDataType, ToDataType>(src, from_scale, to_scale);
        if (strict
            && !accurateEquals(Field(DecimalField<FromField>(src, from_scale)), Field(DecimalField<ToField>(converted, to_scale))))
            return ColumnPtr{};
        return build(converted);
    };

    const WhichDataType which_from(from);

    if (which_from.isNativeUInt())
        return from_integer(value.getUInt(0));
    if (which_from.isNativeInt())
        return from_integer(value.getInt(0));
    if (which_from.isUInt128())
        return from_integer(assert_cast<const ColumnVector<UInt128> &>(value).getData()[0]);
    if (which_from.isUInt256())
        return from_integer(assert_cast<const ColumnVector<UInt256> &>(value).getData()[0]);
    if (which_from.isInt128())
        return from_integer(assert_cast<const ColumnVector<Int128> &>(value).getData()[0]);
    if (which_from.isInt256())
        return from_integer(assert_cast<const ColumnVector<Int256> &>(value).getData()[0]);

    /// floats (`Float32` widens to `Float64`, matching `NearestFieldType`): mirror `convertFloatToDecimalType`.
    if (which_from.isFloat32() || which_from.isFloat64())
    {
        const Float64 v = value.getFloat64(0);
        if (!to_type.canStoreWhole(v))
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Number is too big to place in {}", to_type.getName());
        const ToField converted = convertToDecimal<DataTypeNumber<Float64>, ToDataType>(v, to_scale);
        if (strict && DecimalUtils::convertTo<Float64>(converted, to_scale) != v)
            return ColumnPtr{};
        return build(converted);
    }

    if (which_from.isDecimal32())
        return from_decimal(assert_cast<const ColumnDecimal<Decimal32> &>(value).getData()[0],
                            assert_cast<const DataTypeDecimal<Decimal32> &>(*from).getScale());
    if (which_from.isDecimal64())
        return from_decimal(assert_cast<const ColumnDecimal<Decimal64> &>(value).getData()[0],
                            assert_cast<const DataTypeDecimal<Decimal64> &>(*from).getScale());
    if (which_from.isDecimal128())
        return from_decimal(assert_cast<const ColumnDecimal<Decimal128> &>(value).getData()[0],
                            assert_cast<const DataTypeDecimal<Decimal128> &>(*from).getScale());
    if (which_from.isDecimal256())
        return from_decimal(assert_cast<const ColumnDecimal<Decimal256> &>(value).getData()[0],
                            assert_cast<const DataTypeDecimal<Decimal256> &>(*from).getScale());

    return std::nullopt;  // unsupported source (String/Date/Enum/wrappers/...) -> caller uses the Field fallback
}

/// Dispatch on the Decimal target width. Returns std::nullopt when `to` is not a Decimal, the source is
/// `Bool` (which `convertFieldToType` does not accept for a Decimal target - it throws), or the source is
/// otherwise not handled here, so the caller falls back to the `Field` path.
std::optional<ColumnPtr> tryConvertToDecimalColumnNative(
    const IColumn & value, const DataTypePtr & from, const DataTypePtr & to, bool strict)
{
    if (isBool(from))
        return std::nullopt;

    const WhichDataType which_to(to);
    if (which_to.isDecimal32())
        return convertToDecimalColumnNative(value, from, assert_cast<const DataTypeDecimal<Decimal32> &>(*to), strict);
    if (which_to.isDecimal64())
        return convertToDecimalColumnNative(value, from, assert_cast<const DataTypeDecimal<Decimal64> &>(*to), strict);
    if (which_to.isDecimal128())
        return convertToDecimalColumnNative(value, from, assert_cast<const DataTypeDecimal<Decimal128> &>(*to), strict);
    if (which_to.isDecimal256())
        return convertToDecimalColumnNative(value, from, assert_cast<const DataTypeDecimal<Decimal256> &>(*to), strict);
    return std::nullopt;
}

/// Faithful column-native numeric -> date conversions - the `Date`/`Date32`/`DateTime` branches of
/// `convertFieldToType`'s number-representable path. Reuses the proven native-number path
/// (`tryConvertNumericColumnNative`, i.e. `accurate::convertNumeric`) for the accurate integer step and
/// mirrors each target's quirk exactly: `Date` is a range-checked `UInt16`; `Date32` is an `Int32` day
/// number restricted to the extended-range window `[DATE_LUT_MIN_EXTEND_DAY_NUM, DATE_LUT_MAX_EXTEND_DAY_NUM]`;
/// `DateTime` keeps the value unchanged (no range check - `convertFieldToType` returns it as-is and it is
/// then truncated into the `UInt32` column). Sources are native integers only (matching the `UInt64`/`Int64`
/// field branches there); `Bool`, wide integers and floats fall through to the `Field` path, as do the
/// cross-calendar (`Date`<->`DateTime`), `DateTime64`/`Time`/`Time64` conversions (a later increment).
/// Returns the converted size-1 column, a null `ColumnPtr{}` for a not-representable value, or
/// std::nullopt when not handled here. Pinned by `gtest_convert_column_to_type`.
std::optional<ColumnPtr> tryConvertToDateColumnNative(
    const IColumn & value, const DataTypePtr & from, const DataTypePtr & to, bool convert_inexact_floats)
{
    const WhichDataType which_from(from);
    if (isBool(from) || !(which_from.isNativeInt() || which_from.isNativeUInt()))
        return std::nullopt;

    const WhichDataType which_to(to);

    /// `Date` (UInt16): accurate range-checked conversion, identical to `convertNumericType<UInt16>`.
    /// A `UInt16` result column is exactly a `Date` column.
    if (which_to.isDate())
        return tryConvertNumericColumnNative(value, from, std::make_shared<DataTypeUInt16>(), convert_inexact_floats);

    /// `Date32` (Int32 day number): accurate to `Int64`, then restrict to the representable window.
    if (which_to.isDate32())
    {
        auto as_int64 = tryConvertNumericColumnNative(value, from, std::make_shared<DataTypeInt64>(), convert_inexact_floats);
        if (!as_int64 || !*as_int64)
            return as_int64;  // std::nullopt (n/a) or null `ColumnPtr` (out of `Int64` range)
        const Int64 day_num = (*as_int64)->getInt(0);
        if (day_num < DATE_LUT_MIN_EXTEND_DAY_NUM || day_num > DATE_LUT_MAX_EXTEND_DAY_NUM)
            return ColumnPtr{};
        auto column = to->createColumn();
        assert_cast<ColumnInt32 &>(*column).getData().push_back(static_cast<Int32>(day_num));
        return column;
    }

    /// `DateTime` (UInt32): `convertFieldToType` returns the unsigned value unchanged, which is then
    /// stored (truncated) into the `UInt32` column - no range check. `Int64` sources are not handled
    /// there, so leave them on the `Field` path.
    if (which_to.isDateTime() && which_from.isNativeUInt())
    {
        auto column = to->createColumn();
        assert_cast<ColumnUInt32 &>(*column).getData().push_back(static_cast<UInt32>(value.getUInt(0)));
        return column;
    }

    return std::nullopt;
}

/// Faithful column-native cross-calendar conversions between `Date`/`Date32` and `DateTime` (the
/// timezone-aware branches of `convertFieldToType`). Reads the underlying day number / timestamp
/// directly from the size-1 column and applies the very same `DateLUTImpl::toDayNum`/`fromDayNum` as
/// there (the timezone comes from the `DateTime` type, `from` or `to`). Reading the value from the
/// typed column avoids the `Field`'s `NearestFieldType` widening (`Date32` -> `Int64`) entirely.
/// `DateTime64`/`Time`/`Time64` cross-calendar conversions are a later increment. Returns std::nullopt
/// when the pair is not one of these. Pinned by `gtest_convert_column_to_type`.
std::optional<ColumnPtr> tryConvertBetweenDateAndDateTimeColumnNative(
    const IColumn & value, const DataTypePtr & from, const DataTypePtr & to)
{
    const WhichDataType which_from(from);
    const WhichDataType which_to(to);

    if (which_to.isDateTime() && (which_from.isDate() || which_from.isDate32()))
    {
        const auto & time_zone = assert_cast<const DataTypeDateTime &>(*to).getTimeZone();
        const UInt32 result = which_from.isDate()
            ? static_cast<UInt32>(time_zone.fromDayNum(DayNum(static_cast<UInt16>(value.getUInt(0)))))
            : static_cast<UInt32>(time_zone.fromDayNum(ExtendedDayNum(static_cast<Int32>(value.getInt(0)))));
        auto column = to->createColumn();
        assert_cast<ColumnUInt32 &>(*column).getData().push_back(result);
        return column;
    }

    if ((which_to.isDate() || which_to.isDate32()) && which_from.isDateTime())
    {
        const auto & time_zone = assert_cast<const DataTypeDateTime &>(*from).getTimeZone();
        const auto day_num = time_zone.toDayNum(value.getUInt(0)).toUnderType();
        auto column = to->createColumn();
        if (which_to.isDate())
            assert_cast<ColumnUInt16 &>(*column).getData().push_back(static_cast<UInt16>(day_num));
        else
            assert_cast<ColumnInt32 &>(*column).getData().push_back(static_cast<Int32>(day_num));
        return column;
    }

    return std::nullopt;
}

/// `IColumn::get` reconstructs a `Field` using the storage column's `NearestFieldType`, which does not
/// round-trip the `Field` tag for `Bool`: a `DataTypeBool` column is a plain `ColumnUInt8`, so `get`
/// yields a `UInt64` `Field`. `convertFieldToType` keys on that tag (e.g. `Bool -> String` gives
/// 'true'/'false' for a `Bool` field but '1'/'0' for a `UInt64` one), so re-tag `Bool` values in the
/// reconstructed field - recursing through `Array`/`Tuple`/`Map` and unwrapping `Nullable`/
/// `LowCardinality` - so the delegated `convertFieldToType` sees what it would for a genuine value of
/// `from`. Verified by `gtest_convert_column_to_type`.
///
/// Not handled: `Bool` under `Variant` (and thus `Dynamic`/`JSON`). `ColumnVariant::get` erases the
/// active alternative to the nested column's field, and for an ambiguous variant (e.g.
/// `Variant(Bool, UInt8)`) the reconstructed `Field` no longer says which alternative was active, so it
/// cannot be recovered structurally here - a `ColumnVariant`-aware path before `get` would be needed.
/// See the header for why this is acceptable (no caller needs it; the legacy `Field` path has the same
/// limitation). Other tag-sensitive types (IPv4/IPv6/UUID/Decimal) have dedicated columns/`Field` types
/// and round-trip through `get` already.
void retagBoolInField(Field & field, const DataTypePtr & type)
{
    if (field.isNull())
        return;

    const DataTypePtr unwrapped = removeLowCardinalityAndNullable(type);

    if (isBool(unwrapped))
    {
        field = Field(field.safeGet<UInt64>() != 0);
        return;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(unwrapped.get()))
    {
        for (auto & element : field.safeGet<Array>())
            retagBoolInField(element, array_type->getNestedType());
        return;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(unwrapped.get()))
    {
        auto & tuple = field.safeGet<Tuple>();
        const auto & element_types = tuple_type->getElements();
        for (size_t i = 0; i < tuple.size() && i < element_types.size(); ++i)
            retagBoolInField(tuple[i], element_types[i]);
        return;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(unwrapped.get()))
    {
        for (auto & entry : field.safeGet<Map>())
        {
            auto & key_and_value = entry.safeGet<Tuple>();
            if (key_and_value.size() == 2)
            {
                retagBoolInField(key_and_value[0], map_type->getKeyType());
                retagBoolInField(key_and_value[1], map_type->getValueType());
            }
        }
        return;
    }
}

}

ColumnPtr convertColumnToTypeOrNull(
    const IColumn & value,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings,
    bool strict,
    bool convert_inexact_floats)
{
    chassert(value.size() == 1);

    /// Callers usually pass a `ColumnConst` (e.g. from `evaluateConstantExpressionAsColumn`); operate
    /// on the underlying full column so the fast path's CAST returns a plain (non-const) column and the
    /// `Field` fallback reads the value directly.
    const ColumnPtr full = value.convertToFullColumnIfConst();
    const IColumn & unwrapped = *full;

    if (auto native = tryConvertNumericColumnNative(unwrapped, from, to, convert_inexact_floats))
        return std::move(*native);

    /// `X -> Decimal` column-native (an engaged optional whose value may be a null `ColumnPtr`, meaning
    /// a strict-rejected lossy value; std::nullopt means "not handled here, use the `Field` fallback").
    if (auto decimal = tryConvertToDecimalColumnNative(unwrapped, from, to, strict))
        return std::move(*decimal);

    /// numeric -> `Date`/`Date32`/`DateTime` column-native (same optional convention as above).
    if (auto date = tryConvertToDateColumnNative(unwrapped, from, to, convert_inexact_floats))
        return std::move(*date);

    /// cross-calendar `Date`/`Date32` <-> `DateTime` (timezone-aware).
    if (auto date = tryConvertBetweenDateAndDateTimeColumnNative(unwrapped, from, to))
        return std::move(*date);

    /// Fallback: materialize a `Field`, reuse `convertFieldToType`, rebuild a column. Column-native
    /// fast paths above shrink this over time; the differential test pins equivalence.
    Field field;
    unwrapped.get(0, field);
    /// `get` does not round-trip the `Bool` tag (see `retagBoolInField`); restore it so the delegated
    /// `convertFieldToType` behaves as it would for a genuine value of `from`.
    retagBoolInField(field, from);

    const Field converted = convertFieldToType(field, *to, from.get(), format_settings, strict, convert_inexact_floats);

    if (converted.isNull())
    {
        /// `convertFieldToType` returns a Null `Field` for two different outcomes: a legitimate NULL
        /// result (NULL input into a type that can hold NULL) and "not representable". Distinguish
        /// them here instead of collapsing both into a null `ColumnPtr`: a valid NULL becomes a
        /// size-1 column holding NULL, while "not representable" is the null `ColumnPtr{}`.
        if (field.isNull() && canContainNull(*to))
        {
            auto null_column = to->createColumn();
            null_column->insert(Field());
            return null_column;
        }
        return {};
    }

    auto column = to->createColumn();
    column->insert(converted);
    return column;
}

ColumnPtr tryConvertColumnToTypeOrNull(
    const IColumn & value,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings,
    bool strict,
    bool convert_inexact_floats)
{
    try
    {
        return convertColumnToTypeOrNull(value, from, to, format_settings, strict, convert_inexact_floats);
    }
    catch (...) // Ok: try-pattern that intentionally maps any conversion failure to a null result
    {
        return {};
    }
}

ColumnPtr convertColumnToTypeOrThrow(
    const IColumn & value,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings,
    bool convert_inexact_floats)
{
    chassert(value.size() == 1);

    /// Mirror `convertFieldToTypeOrThrow`: a NULL that the target cannot hold is a type mismatch,
    /// while a non-NULL value that does not fit the target is out of range.
    if (value.isNullAt(0) && !canContainNull(*to))
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert NULL to {}", to->getName());

    ColumnPtr result = convertColumnToTypeOrNull(value, from, to, format_settings, /*strict=*/false, convert_inexact_floats);

    if (!value.isNullAt(0) && !result)
    {
        /// Reproduce `convertFieldToTypeOrThrow`'s diagnostic (which names the offending value and the
        /// types) instead of a generic message - materializing a `Field` only on this exceptional path;
        /// the happy path in `convertColumnToTypeOrNull` stays `Field`-free.
        Field field;
        value.convertToFullColumnIfConst()->get(0, field);
        retagBoolInField(field, from);
        convertFieldToTypeOrThrow(field, *to, from.get(), format_settings, convert_inexact_floats);

        /// `convertFieldToTypeOrThrow` must have thrown for a value that `convertColumnToTypeOrNull`
        /// reported as not representable; guard in case the two ever disagree.
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "Value in column of type {} cannot be represented as {}",
            from->getName(),
            to->getName());
    }

    return result;
}

}
