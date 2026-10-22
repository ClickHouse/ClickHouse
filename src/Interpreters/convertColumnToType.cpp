#include <Interpreters/convertColumnToType.h>

#include <Interpreters/convertFieldToType.h>
#include <Interpreters/castColumn.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnVariant.h>
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
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <base/DayNum.h>
#include <Common/Exception.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/DateLUTImpl.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <limits>


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

    /// Decimals: mirror `convertDecimalToDecimalType` + the strict `accurateEquals` lossy-reject. A strict
    /// conversion that does not fit `to_type` at its scale is not representable rather than `DECIMAL_OVERFLOW`
    /// (`tryConvertDecimals`, as there), so a literal `IN` set skips such a member instead of failing.
    auto from_decimal = [&]<typename FromField>(const FromField & src, UInt32 from_scale) -> ColumnPtr
    {
        using FromDataType = DataTypeDecimal<FromField>;
        ToField converted{};
        if (!strict)
            converted = convertDecimals<FromDataType, ToDataType>(src, from_scale, to_scale);
        else if (!tryConvertDecimals<FromDataType, ToDataType>(src, from_scale, to_scale, converted))
            return ColumnPtr{};
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
/// `DateTime` is a range-checked `UInt32`. Sources are native integers only (matching the `UInt64`/`Int64`
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

    /// `DateTime` (UInt32): accurate range-checked conversion, identical to `convertNumericType<UInt32>`,
    /// so a value that does not fit `UInt32` is not representable instead of wrapping modulo 2^32.
    /// A `UInt32` result column is exactly a `DateTime` column. `Int64` sources are not handled there,
    /// so leave them on the `Field` path.
    if (which_to.isDateTime() && which_from.isNativeUInt())
        return tryConvertNumericColumnNative(value, from, std::make_shared<DataTypeUInt32>(), convert_inexact_floats);

    return std::nullopt;
}

/// Faithful column-native cross-calendar conversions between `Date`/`Date32` and `DateTime` (the
/// timezone-aware branches of `convertFieldToType`). Reads the underlying day number / timestamp
/// directly from the size-1 column and applies the very same `DateLUTImpl::toDayNum`/`fromDayNum` as
/// there (the timezone comes from the `DateTime` type, `from` or `to`). Reading the value from the
/// typed column avoids the `Field`'s `NearestFieldType` widening (`Date32` -> `Int64`) entirely.
/// `DateTime64`/`Time`/`Time64` cross-calendar conversions are a later increment.
/// A `Date`/`Date32` whose local midnight falls outside the `UInt32` range of `DateTime` (a `Date32` before
/// 1970, or a day after 2106-02-07 such as the `Date` maximum `2149-06-06`) is not representable: `convertFieldToType` keeps the exact `Int64` timestamp so
/// that it is outside the range of every stored value, but a `DateTime` column cannot hold it, and storing it
/// truncated modulo 2^32 would make an exact `IN` constant match an unrelated row. This is the same range
/// check as the `UInt64` -> `DateTime` path. Returns the converted size-1 column, a null `ColumnPtr{}` for a
/// not-representable value, or std::nullopt when the pair is not one of these. Pinned by
/// `gtest_convert_column_to_type`.
std::optional<ColumnPtr> tryConvertBetweenDateAndDateTimeColumnNative(
    const IColumn & value, const DataTypePtr & from, const DataTypePtr & to)
{
    const WhichDataType which_from(from);
    const WhichDataType which_to(to);

    if (which_to.isDateTime() && (which_from.isDate() || which_from.isDate32()))
    {
        const auto & time_zone = assert_cast<const DataTypeDateTime &>(*to).getTimeZone();
        const Int64 seconds = which_from.isDate()
            ? time_zone.fromDayNum(DayNum(static_cast<UInt16>(value.getUInt(0))))
            : time_zone.fromDayNum(ExtendedDayNum(static_cast<Int32>(value.getInt(0))));
        if (seconds < 0 || seconds > static_cast<Int64>(std::numeric_limits<UInt32>::max()))
            return ColumnPtr{};
        auto column = to->createColumn();
        assert_cast<ColumnUInt32 &>(*column).getData().push_back(static_cast<UInt32>(seconds));
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
/// A `Bool` under `Variant`/`Dynamic` is reached as well, because the callers resolve the active
/// alternative with `resolveActiveAlternative` and pass its type here. Other tag-sensitive types
/// (IPv4/IPv6/UUID/Decimal) have dedicated columns/`Field` types and round-trip through `get` already.
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

/// Whether a converted `DateTime` value - of `type`, looking through `Nullable`/`LowCardinality` - fits the
/// `UInt32` storage of `DateTime`. `convertFieldToType` keeps the exact timestamp of a `Date`/`Date32` outside
/// that range, and inserting it into a `DateTime` column would truncate it modulo 2^32, so such a value is not
/// representable. This guards the `Field` fallback whatever wraps the source (`Nullable`, `LowCardinality`, the
/// active alternative of `Dynamic`/`Variant`) or the target; the plain types take the same check in
/// `tryConvertBetweenDateAndDateTimeColumnNative`. Composites are not covered: inside `Array`/`Tuple`/`Map`,
/// `convertFieldToType` does not convert `Date` -> `DateTime` at all (pre-existing; the composite step of
/// closing B3).
bool dateTimeValueFitsStorage(const Field & field, const IDataType & type)
{
    if (field.isNull())
        return true;

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(&type))
        return dateTimeValueFitsStorage(field, *nullable->getNestedType());
    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(&type))
        return dateTimeValueFitsStorage(field, *low_cardinality->getDictionaryType());

    if (!WhichDataType(type).isDateTime())
        return true;
    if (field.getType() == Field::Types::Int64)
    {
        const Int64 seconds = field.safeGet<Int64>();
        return seconds >= 0 && seconds <= static_cast<Int64>(std::numeric_limits<UInt32>::max());
    }
    if (field.getType() == Field::Types::UInt64)
        return field.safeGet<UInt64>() <= std::numeric_limits<UInt32>::max();
    return true;
}

}

ActiveAlternative resolveActiveAlternative(const IColumn & value, const DataTypePtr & from, size_t row)
{
    if (const auto * dynamic_column = checkAndGetColumn<ColumnDynamic>(&value))
    {
        const auto & variant_column = dynamic_column->getVariantColumn();
        const auto discriminator = variant_column.globalDiscriminatorAt(row);
        if (discriminator == ColumnVariant::NULL_DISCRIMINATOR)
            return {from, &value, row, {}};

        if (discriminator == dynamic_column->getSharedVariantDiscriminator())
        {
            /// A value of a type beyond `max_types` is stored in the shared variant in binary, after its type.
            const auto data = dynamic_column->getSharedVariant().getDataAt(variant_column.offsetAt(row));
            ReadBufferFromMemory buf(data);
            DataTypePtr type = decodeDataType(buf);
            auto column = type->createColumn();
            type->getDefaultSerialization()->deserializeBinary(*column, buf, FormatSettings{});
            ColumnPtr holder = std::move(column);
            return {std::move(type), holder.get(), 0, holder};
        }

        const auto & variant_type = assert_cast<const DataTypeVariant &>(*dynamic_column->getVariantInfo().variant_type);
        return {
            variant_type.getVariant(discriminator),
            &variant_column.getVariantByGlobalDiscriminator(discriminator),
            variant_column.offsetAt(row),
            {}};
    }

    if (const auto * variant_column = checkAndGetColumn<ColumnVariant>(&value))
    {
        /// `ColumnVariant` does not store the alternatives' types, so they are named by `from`. The bound
        /// also rejects `NULL_DISCRIMINATOR`, which is above every valid alternative index.
        const auto discriminator = variant_column->globalDiscriminatorAt(row);
        const auto * variant_type = typeid_cast<const DataTypeVariant *>(from.get());
        if (variant_type && discriminator < variant_type->getVariants().size())
            return {
                variant_type->getVariant(discriminator),
                &variant_column->getVariantByGlobalDiscriminator(discriminator),
                variant_column->offsetAt(row),
                {}};
    }

    return {from, &value, row, {}};
}

namespace
{

bool isVariantOrDynamic(const IDataType & type)
{
    return typeid_cast<const DataTypeVariant *>(&type) || isDynamic(type);
}

/// A `JSON` value materialized as a `Field` loses the types of its paths in the same way: the dynamic
/// paths are `Dynamic` columns, and an `Object` field keeps only the nested values.
bool hasAlternatives(const IDataType & type)
{
    return isVariantOrDynamic(type) || isObject(type);
}

bool containsAlternatives(const DataTypePtr & type)
{
    if (hasAlternatives(*type))
        return true;
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        return containsAlternatives(nullable_type->getNestedType());
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        return containsAlternatives(array_type->getNestedType());
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
        return std::ranges::any_of(tuple_type->getElements(), [](const DataTypePtr & element) { return containsAlternatives(element); });
    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
        return containsAlternatives(map_type->getKeyType()) || containsAlternatives(map_type->getValueType());
    return false;
}

/// A `JSON` value, or a composite holding a `Variant`/`Dynamic`/`JSON`, converted to a `String` as a whole
/// is rendered from its column: its `Field` keeps none of the members' alternatives, so it cannot be
/// rendered as `CAST` renders it.
bool isRenderedFromColumnToString(const DataTypePtr & from, const DataTypePtr & unwrapped_to)
{
    if (!isStringOrFixedString(unwrapped_to))
        return false;
    return isObject(from) || ((isArray(from) || isTuple(from) || isMap(from)) && containsAlternatives(from));
}

/// `resolveActiveAlternative` recovers the alternative of a top-level `Variant`/`Dynamic` only. Inside a
/// composite (`Array(Dynamic)`, `Tuple(Variant(...))`, `Map(K, Dynamic)`) every member may occupy a
/// different alternative, so no single source type describes them and `convertFieldToType` would see just
/// the carrier - a `Bool` member then renders as `'1'` instead of what `CAST` writes. So convert each such
/// member here, with its own alternative as the source type, straight to the matching member of `to`.
/// `rewriteNestedAlternativesType` gives the source type of the result: those members already have the
/// type of `to`, and `convertFieldToType` leaves a value whose source type equals the target as it is. A
/// `JSON` value, and a composite that holds such members, converted to a `String` as a whole is rendered
/// here as well. `from` is never a `Variant`/`Dynamic` itself at the top level: the callers resolve that
/// alternative first.
DataTypePtr rewriteNestedAlternativesType(const DataTypePtr & from, const DataTypePtr & to)
{
    if (isVariantOrDynamic(*from))
        return to;

    if (const auto * nullable_from = typeid_cast<const DataTypeNullable *>(from.get()))
    {
        const DataTypePtr nested = rewriteNestedAlternativesType(nullable_from->getNestedType(), to);
        return nested == nullable_from->getNestedType() ? from : makeNullable(nested);
    }

    const DataTypePtr unwrapped_to = removeLowCardinalityAndNullable(to);

    if (isRenderedFromColumnToString(from, unwrapped_to))
        return std::make_shared<DataTypeString>();

    if (const auto * array_from = typeid_cast<const DataTypeArray *>(from.get()))
    {
        const auto * array_to = typeid_cast<const DataTypeArray *>(unwrapped_to.get());
        if (!array_to)
            return from;
        const DataTypePtr nested = rewriteNestedAlternativesType(array_from->getNestedType(), array_to->getNestedType());
        return nested == array_from->getNestedType() ? from : std::make_shared<DataTypeArray>(nested);
    }

    if (const auto * tuple_from = typeid_cast<const DataTypeTuple *>(from.get()))
    {
        const auto * tuple_to = typeid_cast<const DataTypeTuple *>(unwrapped_to.get());
        if (!tuple_to || tuple_to->getElements().size() != tuple_from->getElements().size())
            return from;
        DataTypes elements = tuple_from->getElements();
        bool changed = false;
        for (size_t i = 0; i < elements.size(); ++i)
        {
            DataTypePtr element = rewriteNestedAlternativesType(elements[i], tuple_to->getElements()[i]);
            changed |= element != elements[i];
            elements[i] = std::move(element);
        }
        if (!changed)
            return from;
        if (tuple_from->hasExplicitNames())
            return std::make_shared<DataTypeTuple>(elements, tuple_from->getElementNames());
        return std::make_shared<DataTypeTuple>(elements);
    }

    if (const auto * map_from = typeid_cast<const DataTypeMap *>(from.get()))
    {
        const auto * map_to = typeid_cast<const DataTypeMap *>(unwrapped_to.get());
        if (!map_to)
            return from;
        const DataTypePtr key = rewriteNestedAlternativesType(map_from->getKeyType(), map_to->getKeyType());
        const DataTypePtr value = rewriteNestedAlternativesType(map_from->getValueType(), map_to->getValueType());
        if (key == map_from->getKeyType() && value == map_from->getValueType())
            return from;
        return std::make_shared<DataTypeMap>(key, value);
    }

    return from;
}

/// Converts the members of `field` that `rewriteNestedAlternativesType` retypes, walking row `row` of `column`
/// alongside it to find the alternative of each member. `field` was read from that row, so its shape matches.
/// Returns false when a member cannot be represented in its target, which a non-`Nullable` target cannot hold
/// as `NULL` either: then, as in `convertFieldToType`, the whole value cannot be represented.
bool rewriteNestedAlternatives(
    Field & field,
    const IColumn & column,
    size_t row,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings,
    bool strict,
    bool convert_inexact_floats);

/// Converts `field`, read from row `row` of `column` of type `from`, which may be a `Variant`/`Dynamic`,
/// to `to`. Returns false if it cannot be represented.
bool convertAlternative(
    Field & field,
    const IColumn & column,
    size_t row,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings,
    bool strict,
    bool convert_inexact_floats)
{
    if (field.isNull())
        return canContainNull(*to);

    const ActiveAlternative alternative = resolveActiveAlternative(column, from, row);
    retagBoolInField(field, alternative.type);
    /// The alternative can be a composite holding `Variant`/`Dynamic` members of its own.
    const DataTypePtr source = rewriteNestedAlternativesType(alternative.type, to);
    if (source != alternative.type
        && !rewriteNestedAlternatives(
            field, *alternative.column, alternative.row, alternative.type, to, format_settings, strict, convert_inexact_floats))
        return false;
    field = convertFieldToType(field, *to, source.get(), format_settings, strict, convert_inexact_floats);
    /// Strict: a member that is not representable is not a `NULL` member, even if `to` can hold one.
    return !field.isNull() || (canContainNull(*to) && !strict);
}

bool rewriteNestedAlternatives(
    Field & field,
    const IColumn & column,
    size_t row,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings,
    bool strict,
    bool convert_inexact_floats)
{
    if (isVariantOrDynamic(*from))
        return convertAlternative(field, column, row, from, to, format_settings, strict, convert_inexact_floats);

    /// A `NULL` of a `Nullable` member is left to `convertFieldToType`, as its type is not retyped to `to`.
    if (field.isNull())
        return true;

    if (const auto * nullable_from = typeid_cast<const DataTypeNullable *>(from.get()))
    {
        const auto & nullable_column = assert_cast<const ColumnNullable &>(column);
        return rewriteNestedAlternatives(
            field, nullable_column.getNestedColumn(), row, nullable_from->getNestedType(), to,
            format_settings, strict, convert_inexact_floats);
    }

    const DataTypePtr unwrapped_to = removeLowCardinalityAndNullable(to);

    if (isRenderedFromColumnToString(from, unwrapped_to))
    {
        WriteBufferFromOwnString out;
        from->getDefaultSerialization()->serializeText(column, row, out, format_settings);
        field = out.str();
        return true;
    }

    if (const auto * array_from = typeid_cast<const DataTypeArray *>(from.get()))
    {
        const auto * array_to = typeid_cast<const DataTypeArray *>(unwrapped_to.get());
        if (!array_to)
            return true;
        const auto & array_column = assert_cast<const ColumnArray &>(column);
        const size_t offset = row == 0 ? 0 : array_column.getOffsets()[row - 1];
        auto & elements = field.safeGet<Array>();
        for (size_t i = 0; i < elements.size(); ++i)
            if (!rewriteNestedAlternatives(
                    elements[i], array_column.getData(), offset + i, array_from->getNestedType(), array_to->getNestedType(),
                    format_settings, strict, convert_inexact_floats))
                return false;
        return true;
    }

    if (const auto * tuple_from = typeid_cast<const DataTypeTuple *>(from.get()))
    {
        const auto * tuple_to = typeid_cast<const DataTypeTuple *>(unwrapped_to.get());
        if (!tuple_to || tuple_to->getElements().size() != tuple_from->getElements().size())
            return true;
        const auto & tuple_column = assert_cast<const ColumnTuple &>(column);
        auto & elements = field.safeGet<Tuple>();
        for (size_t i = 0; i < elements.size(); ++i)
            if (!rewriteNestedAlternatives(
                    elements[i], tuple_column.getColumn(i), row, tuple_from->getElements()[i], tuple_to->getElements()[i],
                    format_settings, strict, convert_inexact_floats))
                return false;
        return true;
    }

    if (const auto * map_from = typeid_cast<const DataTypeMap *>(from.get()))
    {
        const auto * map_to = typeid_cast<const DataTypeMap *>(unwrapped_to.get());
        if (!map_to)
            return true;
        const auto & nested_array = assert_cast<const ColumnMap &>(column).getNestedColumn();
        const auto & key_value_columns = assert_cast<const ColumnTuple &>(nested_array.getData());
        const size_t offset = row == 0 ? 0 : nested_array.getOffsets()[row - 1];
        auto & entries = field.safeGet<Map>();
        for (size_t i = 0; i < entries.size(); ++i)
        {
            auto & key_and_value = entries[i].safeGet<Tuple>();
            if (!rewriteNestedAlternatives(
                    key_and_value[0], key_value_columns.getColumn(0), offset + i, map_from->getKeyType(), map_to->getKeyType(),
                    format_settings, strict, convert_inexact_floats)
                || !rewriteNestedAlternatives(
                    key_and_value[1], key_value_columns.getColumn(1), offset + i, map_from->getValueType(), map_to->getValueType(),
                    format_settings, strict, convert_inexact_floats))
                return false;
        }
        return true;
    }

    return true;
}

/// Row 0 of `value` as the `Field` and the source type that the delegated `convertFieldToType` should see.
struct SourceField
{
    Field field;
    DataTypePtr type;
    /// False when a member of a composite cannot be represented in the matching member of `to`, so that
    /// the whole value cannot be represented either. Then `field` and `type` are the value as it was read.
    bool representable = true;
};

SourceField getSourceField(
    const IColumn & value,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings,
    bool strict,
    bool convert_inexact_floats)
{
    SourceField source;
    value.get(0, source.field);
    /// `get` keeps neither the `Bool` tag nor the active `Variant`/`Dynamic` alternative; restore both so
    /// the delegated `convertFieldToType` behaves as it would for a genuine value of the constant.
    const ActiveAlternative alternative = resolveActiveAlternative(value, from, 0);
    source.type = alternative.type;
    retagBoolInField(source.field, source.type);

    if (source.field.isNull())
        return source;

    const DataTypePtr rewritten_type = rewriteNestedAlternativesType(source.type, to);
    if (rewritten_type == source.type)
        return source;

    Field rewritten_field = source.field;
    if (!rewriteNestedAlternatives(
            rewritten_field, *alternative.column, alternative.row, source.type, to, format_settings, strict, convert_inexact_floats))
    {
        source.representable = false;
        return source;
    }

    source.field = std::move(rewritten_field);
    source.type = rewritten_type;
    return source;
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
    const SourceField source = getSourceField(unwrapped, from, to, format_settings, strict, convert_inexact_floats);
    if (!source.representable)
        return {};
    const Field & field = source.field;

    const Field converted = convertFieldToType(field, *to, source.type.get(), format_settings, strict, convert_inexact_floats);

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

    /// An out-of-range timestamp would be stored truncated; see `dateTimeValueFitsStorage`.
    if (!dateTimeValueFitsStorage(converted, *to))
        return {};

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
        const ColumnPtr full = value.convertToFullColumnIfConst();
        const SourceField source = getSourceField(*full, from, to, format_settings, /*strict=*/false, convert_inexact_floats);
        if (!source.representable)
            throw Exception(
                ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "Cannot convert value '{}' from {}: it cannot be represented as {}",
                fieldToString(source.field),
                source.type->getName(),
                to->getName());
        convertFieldToTypeOrThrow(source.field, *to, source.type.get(), format_settings, convert_inexact_floats);

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
