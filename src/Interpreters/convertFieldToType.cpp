#include <Interpreters/convertFieldToType.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/Serializations/SerializationQBit.h>

#include <Core/AccurateComparison.h>

#include <Common/typeid_cast.h>
#include <Common/checkStackSize.h>
#include <Common/DateLUTImpl.h>
#include <Common/NaNUtils.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/DateLUT.h>

#include <base/wide_integer_to_string.h>

#include <cmath>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TYPE_MISMATCH;
    extern const int UNEXPECTED_DATA_AFTER_PARSED_VALUE;
    extern const int DECIMAL_OVERFLOW;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
    extern const int CANNOT_CONVERT_TYPE;
}


/** Checking for a `Field from` of `From` type falls to a range of values of type `To`.
  * `From` and `To` - numeric types. They can be floating-point types.
  * `From` is one of UInt64, Int64, Float64,
  *  whereas `To` can also be 8, 16, 32 bit.
  *
  * If falls into a range, then `from` is converted to the `Field` closest to the `To` type.
  * If not, return Field(Null).
  */

namespace
{

template <typename From, typename To>
Field convertNumericTypeImpl(const Field & from, bool strict, bool convert_inexact_floats)
{
    To result{};

    /// Conversion to a floating-point type is inherently lossy: most decimal literals
    /// (e.g. `0.1`) are not exactly representable, so requiring exact equality after the
    /// conversion would reject otherwise valid values. The `values` table function, the `VALUES`
    /// section and `INSERT` opt into this with `convert_inexact_floats`, accepting the nearest
    /// representable value to match `CAST`. The range check inside `accurate::convertNumeric`
    /// still rejects out-of-range values (e.g. `1e300` -> `Float32`), and conversions to integer
    /// or `Decimal` types stay exact.
    ///
    /// By default `convert_inexact_floats` is false, so a value that is not exactly representable
    /// in the target floating-point type returns Null. This is what optimizer/pruning callers
    /// (`KeyCondition`, sharding-key rewrite, ...) and the strict `IN` operator rely on: rounding
    /// the constant and then treating it as an exact comparison bound could prune a mark or a shard
    /// that actually contains a matching row. Keeping the conversion exact there makes the caller
    /// fall back to not using the bound instead of building a wrong one, and keeps set membership
    /// consistent with the `=` operator: `toFloat32(0.1) IN (0.1)` is `0`, like `toFloat32(0.1) = 0.1`.
    ///
    /// `is_floating_point` here is the ClickHouse concept (`base/base/extended_types.h`), defined as
    /// `std::is_floating_point_v<T> || std::is_same_v<T, BFloat16>`. So `BFloat16` is a floating-point
    /// type here too and takes the same lossy path when `convert_inexact_floats` is set (e.g.
    /// `values('x BFloat16', 0.1)` rounds to the nearest `BFloat16` like `CAST`, rather than being rejected).
    const bool exact = strict || !convert_inexact_floats || !is_floating_point<To>;

    const bool converted = exact
        ? accurate::convertNumeric<From, To, true>(from.safeGet<From>(), result)
        : accurate::convertNumeric<From, To, false>(from.safeGet<From>(), result);

    if (!converted)
        return {};
    return result;
}

template <typename To>
Field convertNumericType(const Field & from, const IDataType & type, bool strict, bool convert_inexact_floats)
{
    if (from.getType() == Field::Types::UInt64 || from.getType() == Field::Types::Bool)
        return convertNumericTypeImpl<UInt64, To>(from, strict, convert_inexact_floats);
    if (from.getType() == Field::Types::Int64)
        return convertNumericTypeImpl<Int64, To>(from, strict, convert_inexact_floats);
    if (from.getType() == Field::Types::Float64)
        return convertNumericTypeImpl<Float64, To>(from, strict, convert_inexact_floats);
    if (from.getType() == Field::Types::UInt128)
        return convertNumericTypeImpl<UInt128, To>(from, strict, convert_inexact_floats);
    if (from.getType() == Field::Types::Int128)
        return convertNumericTypeImpl<Int128, To>(from, strict, convert_inexact_floats);
    if (from.getType() == Field::Types::UInt256)
        return convertNumericTypeImpl<UInt256, To>(from, strict, convert_inexact_floats);
    if (from.getType() == Field::Types::Int256)
        return convertNumericTypeImpl<Int256, To>(from, strict, convert_inexact_floats);

    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch in IN or VALUES section. Expected: {}. Got: {}",
        type.getName(), from.getType());
}


template <typename From, typename T>
Field convertIntToDecimalType(const Field & from, const DataTypeDecimal<T> & type)
{
    From value = from.safeGet<From>();
    if (!type.canStoreWhole(value))
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Number is too big to place in {}", type.getName());

    T scaled_value = type.getScaleMultiplier() * T(static_cast<typename T::NativeType>(value));
    return DecimalField<T>(scaled_value, type.getScale());
}


template <typename T>
Field convertStringToDecimalType(const Field & from, const DataTypeDecimal<T> & type)
{
    const String & str_value = from.safeGet<String>();
    T value = type.parseFromString(str_value);
    return DecimalField<T>(value, type.getScale());
}

template <typename From, typename T>
Field convertDecimalToDecimalType(const Field & from, const DataTypeDecimal<T> & type)
{
    auto field = from.safeGet<DecimalField<From>>();
    T value = convertDecimals<DataTypeDecimal<From>, DataTypeDecimal<T>>(field.getValue(), field.getScale(), type.getScale());
    return DecimalField<T>(value, type.getScale());
}

template <typename From, typename T>
Field convertFloatToDecimalType(const Field & from, const DataTypeDecimal<T> & type)
{
    From value = from.safeGet<From>();
    if (!type.canStoreWhole(value))
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Number is too big to place in {}", type.getName());

    //String sValue = convertFieldToString(from);
    //int fromScale = sValue.length()- sValue.find('.') - 1;
    UInt32 scale = type.getScale();

    auto scaled_value = convertToDecimal<DataTypeNumber<From>, DataTypeDecimal<T>>(value, scale);
    return DecimalField<T>(scaled_value, scale);
}

/// When `strict` is true, rejects lossy Decimal conversions by returning Null.
/// This is used by the IN operator to avoid false positives from precision loss.
///
/// For Decimal -> Decimal conversions, `convertFieldToType` may round/truncate (e.g. due to different scales).
/// Strict mode rejects any lossy conversion by requiring exact equality after conversion.
/// Example:
///   SELECT CAST('33.3', 'Decimal64(1)') IN (CAST('33.33', 'Decimal64(2)')); -- 0 (would be 1 without strict check)
///   SELECT CAST('33.3', 'Decimal64(1)') IN (CAST('33.30', 'Decimal64(2)')); -- 1
///
/// For Float64 -> Decimal conversions, the strictness check is done by converting the Decimal back to Float64
/// and comparing with the original Float64. This prevents surprising membership results like:
/// Example:
///   SELECT CAST('33.3', 'Decimal64(1)') IN (33.33); -- 0 (RHS would round to 33.3 without strict check)
///   SELECT CAST('33.3', 'Decimal64(1)') IN (33.3);  -- 1
template <typename To>
Field convertDecimalType(const Field & from, const To & type, bool strict)
{
    using T = typename To::FieldType;
    Field result;

    if (from.getType() == Field::Types::UInt64)
        result = convertIntToDecimalType<UInt64>(from, type);
    else if (from.getType() == Field::Types::Int64)
        result = convertIntToDecimalType<Int64>(from, type);
    else if (from.getType() == Field::Types::UInt128)
        result = convertIntToDecimalType<UInt128>(from, type);
    else if (from.getType() == Field::Types::Int128)
        result = convertIntToDecimalType<Int128>(from, type);
    else if (from.getType() == Field::Types::UInt256)
        result = convertIntToDecimalType<UInt256>(from, type);
    else if (from.getType() == Field::Types::Int256)
        result = convertIntToDecimalType<Int256>(from, type);
    else if (from.getType() == Field::Types::String)
        result = convertStringToDecimalType(from, type);
    else if (from.getType() == Field::Types::Decimal32)
        result = convertDecimalToDecimalType<Decimal32>(from, type);
    else if (from.getType() == Field::Types::Decimal64)
        result = convertDecimalToDecimalType<Decimal64>(from, type);
    else if (from.getType() == Field::Types::Decimal128)
        result = convertDecimalToDecimalType<Decimal128>(from, type);
    else if (from.getType() == Field::Types::Decimal256)
        result = convertDecimalToDecimalType<Decimal256>(from, type);
    else if (from.getType() == Field::Types::Float64)
        result = convertFloatToDecimalType<Float64>(from, type);
    else
        throw Exception(
            ErrorCodes::TYPE_MISMATCH, "Type mismatch in IN or VALUES section. Expected: {}. Got: {}", type.getName(), from.getType());

    if (strict)
    {
        if (Field::isDecimal(from.getType()) && !accurateEquals(from, result))
            return {};

        if (from.getType() == Field::Types::Float64)
        {
            auto decimal_field = result.safeGet<DecimalField<T>>();
            auto decimal_to_float = DecimalUtils::convertTo<Float64>(decimal_field.getValue(), decimal_field.getScale());
            if (decimal_to_float != from.safeGet<Float64>())
                return {};
        }
    }

    return result;
}

/// Bounds of the numeric domain for a Field being coerced into a temporal column
/// (mirror MAX_TIME_TIMESTAMP / MAX_DATETIME_TIMESTAMP / MAX_DATE32_TIMESTAMP in
/// Functions/DateTimeTransforms.h and the day-number maxima in Common/DateLUTImpl.h).
constexpr Int64 MAX_TIME_TIMESTAMP_FIELD = 3599999;      /// 999:59:59
constexpr Int64 MAX_DATETIME_TIMESTAMP_FIELD = 0xFFFFFFFF;
constexpr Int64 DATE_DAY_NUM_MAX_FIELD = DATE_LUT_MAX_DAY_NUM;               /// 0xFFFF, 2149-06-06
constexpr Int64 DATE_MAX_TIMESTAMP_FIELD = 0xFFFFFFFF;                       /// MAX_DATETIME_TIMESTAMP
constexpr Int64 DATE32_DAY_NUM_MAX_FIELD = DATE_LUT_MAX_EXTEND_DAY_NUM;      /// last extended day num
constexpr Int64 DATE32_MAX_TIMESTAMP_FIELD = 10413791999LL;                  /// MAX_DATE32_TIMESTAMP, 2299-12-31

/// Clamp an in-bounds numeric value to a time_t (mirrors saturateToRange in FunctionsConversion.h):
/// the value is proven inside [min_bound, max_bound] with accurate comparisons before the narrowing
/// cast, so it never wraps (unsigned/wide above INT64_MAX) or is undefined (float out of time_t range).
/// Every caller rejects a non-finite value before reaching this point.
template <typename T>
Int64 clampToTimestamp(const T & value, Int64 min_bound, Int64 max_bound)
{
    if (accurate::greaterOp(value, max_bound))
        return max_bound;
    if (accurate::lessOp(value, min_bound))
        return min_bound;
    return static_cast<Int64>(value);
}

/// Coerce a numeric value into the seconds domain of a Time or DateTime column: a non-finite value is
/// rejected, `throw` raises on out-of-range, and the other modes clamp to [min_bound, max_bound].
/// Returns the target's canonical Field type (Int64 signed / UInt64 unsigned) so a clamped value still
/// compares equal to the serializer's read-back in getPartitionIDFromQuery.
template <typename T>
Field coerceNumericValueToDateTimeOrTime(
    const T & value, Int64 min_bound, Int64 max_bound, bool signed_target,
    FormatSettings::DateTimeOverflowBehavior overflow_behavior, const char * type_name)
{
    /// A non-finite value is not out of range but unrepresentable, so no mode may clamp it. The CAST
    /// path rejects it the same way, and materialization must not diverge from CAST.
    if constexpr (is_floating_point<T>)
        if (!isFinite(value))
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Unexpected inf or nan to integer conversion");

    const bool over_max = accurate::greaterOp(value, max_bound);
    const bool under_min = accurate::lessOp(value, min_bound);

    if (over_max || under_min)
    {
        if (overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
            throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                "Value {} is out of bounds of type {}", value, type_name);

        /// Saturate / ignore: clamp into the representable range.
        const Int64 clamped = over_max ? max_bound : min_bound;
        return signed_target ? Field(clamped) : Field(static_cast<UInt64>(clamped));
    }

    /// In range: return the value in the target's canonical Field representation.
    const Int64 in_range = clampToTimestamp(value, min_bound, max_bound);
    return signed_target ? Field(in_range) : Field(static_cast<UInt64>(in_range));
}

/// An exact target rejects any float a temporal integer cannot hold exactly (fractional or non-finite),
/// matching convertNumericType<integer> rather than truncating. Integral floats pass through and are then
/// range-checked by date_time_overflow_behavior.
bool floatRejectedByExactContract(Float64 value, bool exact)
{
    return exact && (!isFinite(value) || value != std::trunc(value));
}

/// Dispatch every numeric Field type evaluateConstantExpression can produce (integer, float, wide
/// integer) to coerceNumericValueToDateTimeOrTime. `exact` rejects an inexact float as Null.
Field coerceNumericFieldToDateTimeOrTime(
    const Field & src, Int64 min_bound, Int64 max_bound,
    FormatSettings::DateTimeOverflowBehavior overflow_behavior, const char * type_name, bool exact)
{
    /// A signed target (Time, min_bound < 0) is backed by Int32; an unsigned one (DateTime) by UInt32.
    const bool signed_target = min_bound < 0;
    auto run = [&](const auto & value) -> Field
    {
        return coerceNumericValueToDateTimeOrTime(value, min_bound, max_bound, signed_target, overflow_behavior, type_name);
    };

    switch (src.getType())
    {
        case Field::Types::UInt64: return run(src.safeGet<UInt64>());
        case Field::Types::Int64:  return run(src.safeGet<Int64>());
        case Field::Types::Float64:
        {
            const Float64 value = src.safeGet<Float64>();
            if (floatRejectedByExactContract(value, exact))
                return {};
            return run(value);
        }
        case Field::Types::UInt128: return run(src.safeGet<UInt128>());
        case Field::Types::Int128:  return run(src.safeGet<Int128>());
        case Field::Types::UInt256: return run(src.safeGet<UInt256>());
        case Field::Types::Int256:  return run(src.safeGet<Int256>());
        default: return {}; /// non-numeric: let the caller fall through to its existing handling
    }
}

/// Coerce a numeric value into a Date day-number Field, mirroring ToDateTransformFromSecondsOrDays:
/// [0, 0xFFFF] is a day number, anything larger is a unix timestamp. Returns a UInt64 Field, Date's
/// canonical representation.
template <typename T>
Field coerceNumericToDateField(const T & value, const DateLUTImpl & time_zone, FormatSettings::DateTimeOverflowBehavior overflow)
{
    const bool throw_mode = overflow == FormatSettings::DateTimeOverflowBehavior::Throw;
    /// Only reached under `throw`; the saturating modes clamp at the call sites below.
    auto out_of_bounds = [&]() -> Field
    {
        throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Value {} is out of bounds of type Date", value);
    };

    if constexpr (is_floating_point<T>)
        if (!isFinite(value))
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Unexpected inf or nan to integer conversion");

    if (accurate::lessOp(value, 0))
        return throw_mode ? out_of_bounds() : Field(static_cast<UInt64>(0));

    /// Above the day-number domain: treat as a unix timestamp (matches the CAST "a bit weird" behavior).
    if (accurate::greaterOp(value, DATE_DAY_NUM_MAX_FIELD))
    {
        if (throw_mode && accurate::greaterOp(value, DATE_MAX_TIMESTAMP_FIELD))
            return out_of_bounds();
        const Int64 ts = clampToTimestamp(value, 0, DATE_MAX_TIMESTAMP_FIELD);
        return Field(static_cast<UInt64>(time_zone.toDayNum(ts).toUnderType()));
    }

    /// In the day-number domain (0 .. 0xFFFF): the value is itself the day number.
    return Field(static_cast<UInt64>(value));
}

/// Coerce a numeric value into a Date32 day-number Field, mirroring ToDate32TransformFromSecondsOrDays:
/// [daynum_min_offset, DATE_LUT_MAX_EXTEND_DAY_NUM) is a signed day number, anything larger is a unix
/// timestamp. Returns an Int64 Field, Date32's canonical representation.
template <typename T>
Field coerceNumericToDate32Field(const T & value, const DateLUTImpl & time_zone, FormatSettings::DateTimeOverflowBehavior overflow)
{
    const bool throw_mode = overflow == FormatSettings::DateTimeOverflowBehavior::Throw;
    const Int64 daynum_min_offset = -static_cast<Int64>(DateLUTImpl::getDayNumOffsetEpoch());

    /// A non-finite value is unrepresentable rather than out of range, so no mode may clamp it.
    if constexpr (is_floating_point<T>)
        if (!isFinite(value))
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Unexpected inf or nan to integer conversion");

    if (accurate::lessOp(value, daynum_min_offset))
    {
        if (throw_mode)
            throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type Date32", value);
        return Field(static_cast<Int64>(daynum_min_offset));
    }

    /// Above the extended day-number domain: treat as a unix timestamp. Predicate kept
    /// identical to ToDate32TransformFromSecondsOrDays so a fractional day number just
    /// below the boundary lands on the same arm as CAST.
    if (accurate::greaterOrEqualsOp(value, DATE32_DAY_NUM_MAX_FIELD))
    {
        if (throw_mode && accurate::greaterOp(value, DATE32_MAX_TIMESTAMP_FIELD))
            throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Timestamp value {} is out of bounds of type Date32", value);
        const Int64 ts = clampToTimestamp(value, 0, DATE32_MAX_TIMESTAMP_FIELD);
        return Field(static_cast<Int64>(time_zone.toDayNum(ts).toUnderType()));
    }

    /// In the extended day-number domain: the value is itself the (signed) day number.
    return Field(static_cast<Int64>(value));
}

/// Dispatch every numeric Field type evaluateConstantExpression can produce (integer, float, wide
/// integer) to coerceNumericToDate/coerceNumericToDate32. `exact` rejects an inexact float as Null.
Field coerceNumericFieldToDateOrDate32(const Field & src, bool is_date32, FormatSettings::DateTimeOverflowBehavior overflow, bool exact)
{
    const auto & time_zone = DateLUT::instance();
    auto run = [&](const auto & value) -> Field
    {
        return is_date32 ? coerceNumericToDate32Field(value, time_zone, overflow)
                         : coerceNumericToDateField(value, time_zone, overflow);
    };

    switch (src.getType())
    {
        case Field::Types::UInt64: return run(src.safeGet<UInt64>());
        case Field::Types::Int64:  return run(src.safeGet<Int64>());
        case Field::Types::Float64:
        {
            const Float64 value = src.safeGet<Float64>();
            if (floatRejectedByExactContract(value, exact))
                return {};
            return run(value);
        }
        case Field::Types::UInt128: return run(src.safeGet<UInt128>());
        case Field::Types::Int128:  return run(src.safeGet<Int128>());
        case Field::Types::UInt256: return run(src.safeGet<UInt256>());
        case Field::Types::Int256:  return run(src.safeGet<Int256>());
        default: return {}; /// non-numeric: let the caller fall through to its existing handling
    }
}

/// The numeric Field types evaluateConstantExpression can hand to the temporal coercion helpers above.
bool isNumericFieldForTemporalCoercion(const Field & src)
{
    switch (src.getType())
    {
        case Field::Types::UInt64:
        case Field::Types::Int64:
        case Field::Types::Float64:
        case Field::Types::UInt128:
        case Field::Types::Int128:
        case Field::Types::UInt256:
        case Field::Types::Int256:
            return true;
        default:
            return false;
    }
}

Field convertFieldToTypeImpl(const Field & src, const IDataType & type, const IDataType * from_type_hint, const FormatSettings & format_settings, bool strict, bool convert_inexact_floats, bool temporal_numeric_is_offset)
{
    if (from_type_hint && from_type_hint->equals(type))
    {
        return src;
    }

    WhichDataType which_type(type);
    WhichDataType which_from_type;

    if (from_type_hint)
    {
        which_from_type = WhichDataType(*from_type_hint);
    }

    /// Conversion between Date and DateTime, Time and vice versa.
    if (which_type.isDate() && which_from_type.isDateTime())
    {
        return static_cast<UInt16>(static_cast<const DataTypeDateTime &>(*from_type_hint).getTimeZone().toDayNum(src.safeGet<UInt64>()).toUnderType());
    }
    if (which_type.isDate32() && which_from_type.isDateTime())
    {
        return static_cast<Int32>(
            static_cast<const DataTypeDateTime &>(*from_type_hint).getTimeZone().toDayNum(src.safeGet<UInt64>()).toUnderType());
    }
    if (which_type.isDateTime() && which_from_type.isDate())
    {
        return static_cast<const DataTypeDateTime &>(type).getTimeZone().fromDayNum(DayNum(static_cast<UInt16>(src.safeGet<UInt64>())));
    }
    if (which_type.isDateTime() && which_from_type.isDate32())
    {
        return static_cast<const DataTypeDateTime &>(type).getTimeZone().fromDayNum(DayNum(static_cast<UInt16>(src.safeGet<Int32>())));
    }
    if (which_type.isDateTime64() && which_from_type.isDate())
    {
        const auto & date_time64_type = static_cast<const DataTypeDateTime64 &>(type);
        const auto value = date_time64_type.getTimeZone().fromDayNum(DayNum(static_cast<UInt16>(src.safeGet<UInt16>())));
        return DecimalField<DateTime64>(
            DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(value, 0, date_time64_type.getScaleMultiplier()),
            date_time64_type.getScale());
    }
    if (which_type.isDateTime64() && which_from_type.isDate32())
    {
        const auto & date_time64_type = static_cast<const DataTypeDateTime64 &>(type);
        const auto value = date_time64_type.getTimeZone().fromDayNum(ExtendedDayNum(static_cast<Int32>(src.safeGet<Int32>())));
        return DecimalField<DateTime64>(
            DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(value, 0, date_time64_type.getScaleMultiplier()),
            date_time64_type.getScale());
    }
    if (which_type.isDate() && which_from_type.isTime())
    {
        return static_cast<UInt16>(static_cast<const DataTypeTime &>(*from_type_hint).getTimeZone().toDayNum(src.safeGet<UInt64>()).toUnderType());
    }
    if (which_type.isDate32() && which_from_type.isTime())
    {
        return static_cast<Int32>(
            static_cast<const DataTypeTime &>(*from_type_hint).getTimeZone().toDayNum(src.safeGet<UInt64>()).toUnderType());
    }
    if (which_type.isTime() && which_from_type.isDate())
    {
        return static_cast<const DataTypeTime &>(type).getTimeZone().fromDayNum(DayNum(static_cast<DayNum::UnderlyingType>(src.safeGet<UInt64>())));
    }
    if (which_type.isTime() && which_from_type.isDate32())
    {
        return static_cast<const DataTypeTime &>(type).getTimeZone().fromDayNum(DayNum(static_cast<DayNum::UnderlyingType>(src.safeGet<Int32>())));
    }
    if (which_type.isTime64() && which_from_type.isDate())
    {
        const auto & time64_type = static_cast<const DataTypeTime64 &>(type);
        const auto value = time64_type.getTimeZone().fromDayNum(DayNum(static_cast<DayNum::UnderlyingType>(src.safeGet<UInt16>())));
        return DecimalField<Time64>(
            DecimalUtils::decimalFromComponentsWithMultiplier<Time64>(value, 0, time64_type.getScaleMultiplier()),
            time64_type.getScale());
    }
    if (which_type.isTime64() && which_from_type.isDate32())
    {
        const auto & time64_type = static_cast<const DataTypeTime64 &>(type);
        const auto value = time64_type.getTimeZone().fromDayNum(ExtendedDayNum(static_cast<Int32>(src.safeGet<Int32>())));
        return DecimalField<Time64>(
            DecimalUtils::decimalFromComponentsWithMultiplier<Time64>(value, 0, time64_type.getScaleMultiplier()),
            time64_type.getScale());
    }
    if (type.isValueRepresentedByNumber() && src.getType() != Field::Types::String)
    {
        /// Bool is not represented in which_type, so we need to handle it separately.
        /// See the comment for `strict` in convertFieldToType.h for details.
        if (isInt64OrUInt64orBoolFieldType(src.getType()) && type.getName() == "Bool")
        {
            if (strict)
            {
                if (src.getType() == Field::Types::Int64)
                {
                    Int64 val = src.safeGet<Int64>();
                    if (val == 0 || val == 1)
                        return bool(val);
                    return {};
                }
                UInt64 val = src.safeGet<UInt64>();
                if (val <= 1)
                    return bool(val);
                return {};
            }
            return bool(src.safeGet<bool>());
        }

        if (which_type.isUInt8())
            return convertNumericType<UInt8>(src, type, strict, convert_inexact_floats);
        if (which_type.isUInt16())
            return convertNumericType<UInt16>(src, type, strict, convert_inexact_floats);
        if (which_type.isUInt32())
            return convertNumericType<UInt32>(src, type, strict, convert_inexact_floats);
        if (which_type.isUInt64())
            return convertNumericType<UInt64>(src, type, strict, convert_inexact_floats);
        if (which_type.isUInt128())
            return convertNumericType<UInt128>(src, type, strict, convert_inexact_floats);
        if (which_type.isUInt256())
            return convertNumericType<UInt256>(src, type, strict, convert_inexact_floats);
        if (which_type.isInt8())
            return convertNumericType<Int8>(src, type, strict, convert_inexact_floats);
        if (which_type.isInt16())
            return convertNumericType<Int16>(src, type, strict, convert_inexact_floats);
        if (which_type.isInt32())
            return convertNumericType<Int32>(src, type, strict, convert_inexact_floats);
        if (which_type.isInt64())
            return convertNumericType<Int64>(src, type, strict, convert_inexact_floats);
        if (which_type.isInt128())
            return convertNumericType<Int128>(src, type, strict, convert_inexact_floats);
        if (which_type.isInt256())
            return convertNumericType<Int256>(src, type, strict, convert_inexact_floats);
        if (which_type.isBFloat16())
            return convertNumericType<BFloat16>(src, type, strict, convert_inexact_floats);
        if (which_type.isFloat32())
            return convertNumericType<Float32>(src, type, strict, convert_inexact_floats);
        if (which_type.isFloat64())
            return convertNumericType<Float64>(src, type, strict, convert_inexact_floats);
        if (const auto * ptype = typeid_cast<const DataTypeDecimal<Decimal32> *>(&type))
            return convertDecimalType(src, *ptype, strict);
        if (const auto * ptype = typeid_cast<const DataTypeDecimal<Decimal64> *>(&type))
            return convertDecimalType(src, *ptype, strict);
        if (const auto * ptype = typeid_cast<const DataTypeDecimal<Decimal128> *>(&type))
            return convertDecimalType(src, *ptype, strict);
        if (const auto * ptype = typeid_cast<const DataTypeDecimal<Decimal256> *>(&type))
            return convertDecimalType(src, *ptype, strict);

        if (which_type.isEnum() && (src.getType() == Field::Types::UInt64 || src.getType() == Field::Types::Int64))
        {
            /// Convert UInt64 or Int64 to Enum's value
            return dynamic_cast<const IDataTypeEnum &>(type).castToValue(src);
        }

        const bool overflow_ignore = format_settings.date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Ignore;

        /// Temporal types are integer-backed, so an exact target cannot accept a non-integral float.
        const bool exact = strict || !convert_inexact_floats;

        /// Take the plain storage-type arm instead of the overflow-aware coerce helpers. Required for an
        /// exact target in ignore mode (an out-of-storage value must stay Null so convertFieldToTypeOrThrow
        /// raises ARGUMENT_OUT_OF_BOUND, and an in-storage value must stay verbatim to remain addressable),
        /// and unconditionally for a frame offset, which is a distance and never a temporal point.
        const bool ignore_exact_target = (overflow_ignore && exact) || temporal_numeric_is_offset;

        if (which_type.isDate() && isNumericFieldForTemporalCoercion(src))
        {
            if (ignore_exact_target)
                return convertNumericType<UInt16>(src, type, strict, convert_inexact_floats);
            return coerceNumericFieldToDateOrDate32(src, /*is_date32=*/false, format_settings.date_time_overflow_behavior, exact);
        }

        if (which_type.isDateTime() && isNumericFieldForTemporalCoercion(src))
        {
            if (ignore_exact_target)
                return convertNumericType<UInt32>(src, type, strict, convert_inexact_floats);
            return coerceNumericFieldToDateTimeOrTime(
                src, 0, MAX_DATETIME_TIMESTAMP_FIELD, format_settings.date_time_overflow_behavior, "DateTime", exact);
        }

        if (which_type.isTime() && isNumericFieldForTemporalCoercion(src))
        {
            if (ignore_exact_target)
                return convertNumericType<Int32>(src, type, strict, convert_inexact_floats);
            return coerceNumericFieldToDateTimeOrTime(
                src, -MAX_TIME_TIMESTAMP_FIELD, MAX_TIME_TIMESTAMP_FIELD, format_settings.date_time_overflow_behavior, "Time", exact);
        }

        if (which_type.isDate32() && isNumericFieldForTemporalCoercion(src))
        {
            if (ignore_exact_target)
                return convertNumericType<Int32>(src, type, strict, convert_inexact_floats);
            return coerceNumericFieldToDateOrDate32(src, /*is_date32=*/true, format_settings.date_time_overflow_behavior, exact);
        }

        if (which_type.isDateTime64() && src.getType() == Field::Types::Decimal64)
        {
            const auto & from_type = src.safeGet<Decimal64>();
            const auto & to_type = static_cast<const DataTypeDateTime64 &>(type);

            const auto scale_from = from_type.getScale();
            const auto scale_to = to_type.getScale();
            const auto scale_multiplier_diff = scale_from > scale_to ? from_type.getScaleMultiplier() / to_type.getScaleMultiplier()
                                                                     : to_type.getScaleMultiplier() / from_type.getScaleMultiplier();

            if (scale_multiplier_diff == 1) /// Already in needed type.
                return src;

            /// in case if we need to make DateTime64(a) from DateTime64(b), a != b, we need to convert datetime value to the right scale
            Int64 value = from_type.getValue().value;

            if (scale_from > scale_to)
            {
                value /= scale_multiplier_diff;
            }
            else if (scale_from < scale_to)
            {
                Int64 result = 0;
                if (common::mulOverflow(value, scale_multiplier_diff.value, result))
                    throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Cannot convert {} to {} as it overflows: {} * {} does not fit in Int64",
                        src.getTypeName(), type.getName(), value, scale_multiplier_diff.value);
                value = result;
            }

            return DecimalField<DateTime64>(DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(value, 0, 1), scale_to);
        }

        if (which_type.isTime64() && src.getType() == Field::Types::Decimal64)
        {
            const auto & from_type = src.safeGet<Decimal64>();
            const auto & to_type = static_cast<const DataTypeTime64 &>(type);

            const auto scale_from = from_type.getScale();
            const auto scale_to = to_type.getScale();
            const auto scale_multiplier_diff = scale_from > scale_to ? from_type.getScaleMultiplier() / to_type.getScaleMultiplier()
                                                                     : to_type.getScaleMultiplier() / from_type.getScaleMultiplier();

            if (scale_multiplier_diff == 1) /// Already in needed type.
                return src;

            /// in case if we need to make Time64(a) from Time64(b), a != b, we need to convert time value to the right scale
            const UInt64 value = scale_from > scale_to ? from_type.getValue().value / scale_multiplier_diff
                                                       : from_type.getValue().value * scale_multiplier_diff;
            return DecimalField<Time64>(DecimalUtils::decimalFromComponentsWithMultiplier<Time64>(value, 0, 1), scale_to);
        }

        if (which_type.isDateTime64()
            && (src.getType() == Field::Types::UInt64 || src.getType() == Field::Types::Int64 || src.getType() == Field::Types::Decimal64))
        {
            const auto scale = static_cast<const DataTypeDateTime64 &>(type).getScale();
            const auto decimal_value
                = DecimalUtils::decimalFromComponents<DateTime64>(applyVisitor(FieldVisitorConvertToNumber<Int64>(), src), 0, scale);
            return Field(DecimalField<DateTime64>(decimal_value, scale));
        }

        if (which_type.isTime64()
            && (src.getType() == Field::Types::UInt64 || src.getType() == Field::Types::Int64 || src.getType() == Field::Types::Decimal64))
        {
            const auto scale = static_cast<const DataTypeTime64 &>(type).getScale();
            const auto decimal_value
                = DecimalUtils::decimalFromComponents<Time64>(applyVisitor(FieldVisitorConvertToNumber<Int64>(), src), 0, scale);
            return Field(DecimalField<Time64>(decimal_value, scale));
        }

        if (which_type.isIPv4() && src.getType() == Field::Types::IPv4)
        {
            /// Already in needed type.
            return src;
        }
        if (which_type.isIPv4() && src.getType() == Field::Types::UInt64)
        {
            /// convert through UInt32 which is the underlying type for native IPv4
            return static_cast<IPv4>(convertNumericType<UInt32>(src, type, strict, convert_inexact_floats).safeGet<UInt32>());
        }
    }
    else if (which_type.isUUID() && src.getType() == Field::Types::UUID)
    {
        /// Already in needed type.
        return src;
    }
    else if (which_type.isIPv6())
    {
        /// Already in needed type.
        if (src.getType() == Field::Types::IPv6)
            return src;
        /// Treat FixedString(16) as a binary representation of IPv6
        if (which_from_type.isFixedString() && assert_cast<const DataTypeFixedString *>(from_type_hint)->getN() == IPV6_BINARY_LENGTH)
        {
            const auto col = type.createColumn();
            ReadBufferFromString in_buffer(src.safeGet<String>());
            type.getDefaultSerialization()->deserializeBinary(*col, in_buffer, {});
            return (*col)[0];
        }
    }
    else if (which_type.isStringOrFixedString())
    {
        if (src.getType() == Field::Types::String)
        {
            if (which_type.isFixedString())
            {
                size_t n = assert_cast<const DataTypeFixedString &>(type).getN();
                const auto & src_str = src.safeGet<String>();
                if (src_str.size() < n)
                {
                    String src_str_extended = src_str;
                    src_str_extended.resize(n);
                    return src_str_extended;
                }
            }
            return src;
        }

        return applyVisitor(FieldVisitorToString(), src);
    }
    else if (const DataTypeArray * type_array = typeid_cast<const DataTypeArray *>(&type))
    {
        if (src.getType() == Field::Types::Array)
        {
            const Array & src_arr = src.safeGet<Array>();
            size_t src_arr_size = src_arr.size();

            const auto & element_type = *(type_array->getNestedType());
            bool have_unconvertible_element = false;
            Array res(src_arr_size);
            for (size_t i = 0; i < src_arr_size; ++i)
            {
                res[i] = convertFieldToType(src_arr[i], element_type, nullptr, format_settings, strict, convert_inexact_floats, temporal_numeric_is_offset);
                if (res[i].isNull() && !canContainNull(element_type))
                {
                    // See the comment for Tuples below.
                    have_unconvertible_element = true;
                }
            }

            return have_unconvertible_element ? Field(Null()) : Field(res);
        }
    }
    else if (const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(&type))
    {
        if (src.getType() == Field::Types::Tuple)
        {
            const auto & src_tuple = src.safeGet<Tuple>();
            size_t src_tuple_size = src_tuple.size();
            size_t dst_tuple_size = type_tuple->getElements().size();

            if (dst_tuple_size != src_tuple_size)
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Bad size of tuple in IN or VALUES section. "
                    "Expected size: {}, actual size: {}",
                    dst_tuple_size,
                    src_tuple_size);

            Tuple res(dst_tuple_size);
            bool have_unconvertible_element = false;
            for (size_t i = 0; i < dst_tuple_size; ++i)
            {
                const auto & element_type = *(type_tuple->getElements()[i]);
                res[i] = convertFieldToType(src_tuple[i], element_type, nullptr, format_settings, strict, convert_inexact_floats, temporal_numeric_is_offset);
                if (res[i].isNull() && !canContainNull(element_type))
                {
                    /*
                     * Either the source element was Null, or the conversion did not
                     * succeed, because the source and the requested types of the
                     * element are compatible, but the value is not convertible
                     * (e.g. trying to convert -1 from Int8 to UInt8). In these
                     * cases, consider the whole tuple also compatible but not
                     * convertible. According to the specification of this function,
                     * we must return Null in this case.
                     *
                     * The following elements might be not even compatible, so it
                     * makes sense to check them to detect user errors. Remember
                     * that there is an unconvertible element, and try to process
                     * the remaining ones. The convertFieldToType for each element
                     * will throw if it detects incompatibility.
                     */
                    have_unconvertible_element = true;
                }
            }

            return have_unconvertible_element ? Field(Null()) : Field(res);
        }
    }
    else if (const DataTypeQBit * type_qbit = typeid_cast<const DataTypeQBit *>(&type))
    {
        size_t dst_dimension = type_qbit->getDimension();
        size_t dst_element_size = type_qbit->getElementSize();
        size_t dst_stride = type_qbit->getStride();
        size_t dst_num_strides = type_qbit->getNumStrides();
        /// One FixedString per (stride group, bit plane), grouped as [group][bit].
        size_t dst_num_columns = dst_element_size * dst_num_strides;
        size_t bytes_per_fixedstring = DataTypeQBit::bitsToBytes(dst_stride);
        const size_t padded_stride = bytes_per_fixedstring * 8;

        /// For tuples, we expect the input to be in the transposed format already s.t. it can by directly copied inside a QBit
        auto convert_tuple_to_qbit = [&](const auto & src_container, size_t src_size) -> Field
        {
            /// Check that we have element_size * num_strides strings (one per bit plane of each stride group)
            if (dst_num_columns != src_size)
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Bad number of elements in IN or VALUES section when converting to QBit. Expected size: {}, actual size: {}",
                    dst_num_columns,
                    src_size);

            /// Check that each string is of expected length
            for (size_t i = 0; i < src_container.size(); i++)
            {
                const auto elem = src_container[i].template safeGet<String>();

                if (elem.size() != bytes_per_fixedstring)
                    throw Exception(
                        ErrorCodes::TYPE_MISMATCH,
                        "To construct QBit, each string within tuple must consist of {} byte{}. Got {}",
                        bytes_per_fixedstring,
                        bytes_per_fixedstring != 1 ? "s" : "",
                        elem.size());
            }
            return src_container;
        };

        /// For arrays, we expect to get normal numeric data
        auto convert_array_to_qbit = [&](const auto & src_container, size_t src_size) -> Field
        {
            if (dst_dimension != src_size)
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Bad size of QBit in IN or VALUES section. Expected size: {}, actual size: {}",
                    dst_dimension,
                    src_size);

            for (const auto & elem : src_container)
            {
                if (dst_element_size == 8)
                {
                    /// Int8 QBit accepts any numeric field; the value is converted to Int8 below.
                    if (elem.getType() != Field::Types::Int64 && elem.getType() != Field::Types::UInt64
                        && elem.getType() != Field::Types::Float64)
                        throw Exception(
                            ErrorCodes::TYPE_MISMATCH,
                            "QBit(Int8) can only be constructed from numeric values, got {}",
                            elem.getTypeName());
                }
                else if (elem.getType() != Field::Types::Float64)
                    throw Exception(
                        ErrorCodes::TYPE_MISMATCH,
                        "QBit can only be constructed from BFloat16, Float32 and Float64 values, got {}",
                        elem.getTypeName());
            }

            Tuple res(dst_num_columns);

            auto transpose_bits = [&]<typename Word, typename ElementType>()
            {
                /// Prepare output tuple buffers
                std::vector<std::string> out(dst_num_columns, std::string(bytes_per_fixedstring, '\0'));
                std::vector<char *> plane(dst_num_columns);
                for (size_t i = 0; i < dst_num_columns; ++i)
                    plane[i] = reinterpret_cast<char *>(out[i].data());

                /// Transpose each stride group independently. Dimension `i` belongs to group `i / stride` and is written into
                /// that group's element_size bit planes (tuple indices [group * element_size, group * element_size + element_size)).
                for (size_t i = 0; i < dst_dimension; ++i)
                {
                    Word w = 0;
                    ElementType v;
                    if constexpr (std::is_same_v<ElementType, Int8>)
                    {
                        /// Truncate (wrap) the numeric field to Int8 so QBit(Int8) construction has a single
                        /// contract, matching how `toInt8` / `CAST(... AS Int8)` and the VALUES / Array(Int8)
                        /// conversions handle out-of-range values (e.g. 128 -> -128). Non-finite or absurdly
                        /// large Float64 values cannot be wrapped (and casting them to a narrow integer is
                        /// undefined behaviour), so reject them explicitly, as `toInt8` does for inf/nan.
                        const Field & elem = src_container[i];
                        if (elem.getType() == Field::Types::Float64)
                        {
                            const Float64 f = elem.safeGet<Float64>();
                            if (!isFinite(f) || f < static_cast<Float64>(std::numeric_limits<Int64>::min())
                                || f >= static_cast<Float64>(std::numeric_limits<Int64>::max()))
                                throw Exception(
                                    ErrorCodes::TYPE_MISMATCH,
                                    "Cannot convert {} to the Int8 element of QBit",
                                    applyVisitor(FieldVisitorToString(), elem));
                            v = static_cast<Int8>(static_cast<Int64>(f));
                        }
                        else if (elem.getType() == Field::Types::UInt64)
                            v = static_cast<Int8>(elem.safeGet<UInt64>());
                        else
                            v = static_cast<Int8>(elem.safeGet<Int64>());
                    }
                    else
                        v = static_cast<const ElementType>(src_container[i].template safeGet<ElementType>());
                    std::memcpy(&w, &v, sizeof(Word));

                    const size_t group = i / dst_stride;
                    const size_t local_i = i - group * dst_stride;
                    SerializationQBit::transposeBits<Word>(w, local_i, padded_stride, plane.data() + group * dst_element_size);
                }

                /// Move into Fields
                for (size_t i = 0; i < dst_num_columns; ++i)
                    res[i] = Field(std::move(out[i]));
            };

            if (dst_element_size == 8)
                transpose_bits.template operator()<uint8_t, Int8>();
            else if (dst_element_size == 16)
                transpose_bits.template operator()<UInt16, BFloat16>();
            else if (dst_element_size == 32)
                transpose_bits.template operator()<UInt32, Float32>();
            else
                transpose_bits.template operator()<UInt64, Float64>();

            return Field(res);
        };

        if (src.getType() == Field::Types::Tuple)
        {
            const auto & src_tuple = src.safeGet<Tuple>();
            return convert_tuple_to_qbit(src_tuple, src_tuple.size());
        }
        else if (src.getType() == Field::Types::Array)
        {
            const auto & src_array = src.safeGet<Array>();
            return convert_array_to_qbit(src_array, src_array.size());
        }
    }
    else if (const DataTypeMap * type_map = typeid_cast<const DataTypeMap *>(&type))
    {
        if (src.getType() == Field::Types::Map)
        {
            const auto & key_type = *type_map->getKeyType();
            const auto & value_type = *type_map->getValueType();

            const auto & map = src.safeGet<Map>();
            size_t map_size = map.size();

            Map res(map_size);

            bool have_unconvertible_element = false;

            for (size_t i = 0; i < map_size; ++i)
            {
                const auto & map_entry = map[i].safeGet<Tuple>();

                const auto & key = map_entry[0];
                const auto & value = map_entry[1];

                Tuple updated_entry(2);

                updated_entry[0] = convertFieldToType(key, key_type, nullptr, format_settings, strict, convert_inexact_floats, temporal_numeric_is_offset);

                if (updated_entry[0].isNull() && !canContainNull(key_type))
                    have_unconvertible_element = true;

                updated_entry[1] = convertFieldToType(value, value_type, nullptr, format_settings, strict, convert_inexact_floats, temporal_numeric_is_offset);
                if (updated_entry[1].isNull() && !canContainNull(value_type))
                    have_unconvertible_element = true;

                res[i] = updated_entry;
            }

            return have_unconvertible_element ? Field(Null()) : Field(res);
        }
    }
    else if (const DataTypeAggregateFunction * agg_func_type = typeid_cast<const DataTypeAggregateFunction *>(&type))
    {
        if (src.getType() != Field::Types::AggregateFunctionState)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert {} to {}", src.getTypeName(), agg_func_type->getName());

        const auto & name = src.safeGet<AggregateFunctionStateData>().name;
        if (agg_func_type->getName() != name)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert {} to {}", name, agg_func_type->getName());

        return src;
    }
    else if (const DataTypeVariant * type_variant = typeid_cast<const DataTypeVariant *>(&type))
    {
        if (src.isNull())
            return src;

        /// If we have type hint and Variant contains such type, no need to convert field.
        if (from_type_hint && type_variant->tryGetVariantDiscriminator(from_type_hint->getName()))
            return src;

        /// Create temporary column and check if we can insert this field to the variant.
        /// If we can insert, no need to convert anything.
        auto col = type_variant->createColumn();
        if (col->tryInsert(src))
            return src;

        /// Note: the exact-alternative preference below applies only to fields that fall through to
        /// the conversion loop. A field accepted by the two fast paths above keeps `ColumnVariant`'s
        /// pre-existing alternative selection: the discriminator is chosen at column-insert time by
        /// `ColumnVariant::tryInsert`, which scans variants in sorted order. So for a suspicious
        /// `Variant(Float32, Float64)` (sorted: `Float32` before `Float64`), a `Float64` field that
        /// is exact in `Float64` but inexact in `Float32` is still stored lossily in the earlier
        /// `Float32` alternative. Making that selection lossless is a core `ColumnVariant::tryInsert`
        /// change with a far broader blast radius and is intentionally out of the scope of this fix.
        ///
        /// Otherwise try to convert field to any variant.
        /// Among the alternatives, prefer one that represents the value exactly: first try a
        /// strict (lossless) conversion across all variants, and only fall back to a lossy
        /// conversion if no alternative is exact. Without the exact-first pass, a value that
        /// fits one alternative without loss (e.g. an Int64 in `Array(Int64)`) could be stored
        /// lossily in an earlier-listed alternative (e.g. `Array(Float64)`), because non-strict
        /// conversion to a floating-point type accepts the nearest value.
        ///
        /// A strict outer conversion (the `IN` operator) must never round: it only accepts an
        /// exact alternative, so set membership stays consistent with the `=` operator. The
        /// lossy fallback therefore runs only when `strict` is false, and it only rounds floats
        /// when `convert_inexact_floats` was requested by the caller (the `values`/insert path).
        for (const auto & variant : type_variant->getVariants())
        {
            auto res = tryConvertFieldToType(src, *variant, from_type_hint, format_settings, /*strict=*/true);
            if (!res.isNull())
                return res;
        }

        if (!strict)
        {
            for (const auto & variant : type_variant->getVariants())
            {
                auto res = tryConvertFieldToType(src, *variant, from_type_hint, format_settings, /*strict=*/false, convert_inexact_floats);
                if (!res.isNull())
                    return res;
            }
        }
    }
    else if (isDynamic(type))
    {
        /// We can insert any field to Dynamic column.
        return src;
    }
    else if (isObject(type))
    {
        if (src.getType() == Field::Types::Object)
            return src; /// Already in needed type.

        /// TODO: add conversion from Map/Tuple to Object.
    }

    /// Conversion from string by parsing.
    if (src.getType() == Field::Types::String)
    {
        /// Promote data type to avoid overflows. Note that overflows in the largest data type are still possible.
        /// But don't promote narrow floats (Float32, BFloat16): parsing the string into Float64 and narrowing back
        /// would fail the strict equality check inside `accurate::convertNumeric` for any decimal value that is not
        /// exactly representable in the narrow type, producing a Null Field and silently zero-matching comparisons
        /// like `WHERE bf16_col = '49.9'`.
        /// Also don't promote domain types (like bool) because we would otherwise use the serializer of the promoted type (e.g. UInt64 for
        /// bool, which does not allow 'true' and 'false' as input values)
        const IDataType * type_to_parse = &type;
        DataTypePtr holder;

        if (type.canBePromoted() && !which_type.isFloat32() && !which_type.isBFloat16() && !type.getCustomSerialization())
        {
            holder = type.promoteNumericType();
            type_to_parse = holder.get();
        }

        const auto col = type_to_parse->createColumn();
        ReadBufferFromString in_buffer(src.safeGet<String>());
        try
        {
            type_to_parse->getDefaultSerialization()->deserializeWholeText(*col, in_buffer, format_settings);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNEXPECTED_DATA_AFTER_PARSED_VALUE)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert string '{}' to type {}", src.safeGet<String>(), type.getName());

            e.addMessage(fmt::format("while converting '{}' to {}", src.safeGet<String>(), type.getName()));
            throw;
        }

        Field parsed = (*col)[0];
        return convertFieldToType(parsed, type, from_type_hint, format_settings);
    }

    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch in IN or VALUES section. Expected: {}. Got: {}",
        type.getName(), src.getType());
}

}

Field tryConvertFieldToType(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint, const FormatSettings & format_settings, bool strict, bool convert_inexact_floats)
{
    /// TODO: implement proper tryConvertFieldToType without try/catch by adding template flag to convertFieldToTypeImpl to not throw an exception.
    try
    {
        return convertFieldToType(from_value, to_type, from_type_hint, format_settings, strict, convert_inexact_floats);
    }
    catch (...) // Ok: tryConvertFieldToType is a try-pattern
    {
        return {};
    }
}

Field convertFieldToType(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint, const FormatSettings & format_settings, bool strict, bool convert_inexact_floats, bool temporal_numeric_is_offset)
{
    checkStackSize();

    if (from_value.isNull())
        return from_value;

    if (from_type_hint && from_type_hint->equals(to_type))
        return from_value;

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(&to_type))
        return convertFieldToType(from_value, *low_cardinality_type->getDictionaryType(), from_type_hint, format_settings, strict, convert_inexact_floats, temporal_numeric_is_offset);
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(&to_type))
    {
        const IDataType & nested_type = *nullable_type->getNestedType();

        /// NULL remains NULL after any conversion.
        if (WhichDataType(nested_type).isNothing())
            return {};

        if (from_type_hint && from_type_hint->equals(nested_type))
            return from_value;
        return convertFieldToTypeImpl(from_value, nested_type, from_type_hint, format_settings, strict, convert_inexact_floats, temporal_numeric_is_offset);
    }
    return convertFieldToTypeImpl(from_value, to_type, from_type_hint, format_settings, strict, convert_inexact_floats, temporal_numeric_is_offset);
}


Field convertFieldToTypeOrThrow(const Field & from_value, const IDataType & to_type, const IDataType * from_type_hint, const FormatSettings & format_settings, bool convert_inexact_floats, bool temporal_numeric_is_offset)
{
    bool is_null = from_value.isNull();
    if (is_null && !canContainNull(to_type))
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert NULL to {}", to_type.getName());

    /// Value-materialization callers pass `convert_inexact_floats = true` so a decimal literal that is
    /// not exactly representable (e.g. `0.1` for a `Float32` column) is converted to the nearest
    /// representable value like `CAST` instead of being rejected. Callers that resolve an exact target -
    /// e.g. `ALTER ... PARTITION` in `MergeTreeData::getPartitionIDFromQuery` - keep the default (false)
    /// so a destructive statement never silently rounds an unrepresentable numeric literal. A quoted
    /// string literal (e.g. `DROP PARTITION '0.1'`) is still parsed into the target type by string
    /// deserialization before this exactness check and rounds there - a pre-existing string-parsing
    /// behavior shared with string-to-float comparisons, unchanged by this fix. See the header.
    Field converted = convertFieldToType(from_value, to_type, from_type_hint, format_settings, /*strict=*/false, convert_inexact_floats, temporal_numeric_is_offset);

    if (!is_null && converted.isNull())
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "Cannot convert value '{}'{}: it cannot be represented as {}",
            fieldToString(from_value),
            from_type_hint ? " from " + from_type_hint->getName() : "",
            to_type.getName());

    return converted;
}

}
