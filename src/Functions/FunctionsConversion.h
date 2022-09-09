#pragma once

#include "Common/Exception.h"
#include <cstddef>
#include <type_traits>

#include <IO/WriteBufferFromVector.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/Operators.h>
#include <IO/parseDateTimeBestEffort.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/Serializations/SerializationDecimal.h>
#include <Formats/FormatSettings.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnStringHelpers.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Core/AccurateComparison.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/toFixedString.h>
#include <Functions/TransformDateTime64.h>
#include <Functions/FunctionsCodingIP.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnLowCardinality.h>
#include <Interpreters/Context.h>
#include <Common/HashTable/HashMap.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_UUID;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
    extern const int CANNOT_CONVERT_TYPE;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
}


/** Type conversion functions.
  * toType - conversion in "natural way";
  */

inline UInt32 extractToDecimalScale(const ColumnWithTypeAndName & named_column)
{
    const auto * arg_type = named_column.type.get();
    bool ok = checkAndGetDataType<DataTypeUInt64>(arg_type)
        || checkAndGetDataType<DataTypeUInt32>(arg_type)
        || checkAndGetDataType<DataTypeUInt16>(arg_type)
        || checkAndGetDataType<DataTypeUInt8>(arg_type);
    if (!ok)
        throw Exception("Illegal type of toDecimal() scale " + named_column.type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    Field field;
    named_column.column->get(0, field);
    return field.get<UInt32>();
}

/// Function toUnixTimestamp has exactly the same implementation as toDateTime of String type.
struct NameToUnixTimestamp { static constexpr auto name = "toUnixTimestamp"; };

struct AccurateConvertStrategyAdditions
{
    UInt32 scale { 0 };
};

struct AccurateOrNullConvertStrategyAdditions
{
    UInt32 scale { 0 };
};


struct ConvertDefaultBehaviorTag {};
struct ConvertReturnNullOnErrorTag {};

/** Conversion of number types to each other, enums to numbers, dates and datetimes to numbers and back: done by straight assignment.
  *  (Date is represented internally as number of days from some day; DateTime - as unix timestamp)
  */
template <typename FromDataType, typename ToDataType, typename Name, typename SpecialTag = ConvertDefaultBehaviorTag>
struct ConvertImpl
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    template <typename Additions = void *>
    static ColumnPtr NO_SANITIZE_UNDEFINED execute(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type [[maybe_unused]], size_t input_rows_count,
        Additions additions [[maybe_unused]] = Additions())
    {
        const ColumnWithTypeAndName & named_from = arguments[0];

        using ColVecFrom = typename FromDataType::ColumnType;
        using ColVecTo = typename ToDataType::ColumnType;

        if (std::is_same_v<Name, NameToUnixTimestamp>)
        {
            if (isDateOrDate32(named_from.type))
                throw Exception("Illegal type " + named_from.type->getName() + " of first argument of function " + Name::name,
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        if constexpr ((IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>)
            && !(std::is_same_v<DataTypeDateTime64, FromDataType> || std::is_same_v<DataTypeDateTime64, ToDataType>))
        {
            if constexpr (!IsDataTypeDecimalOrNumber<FromDataType> || !IsDataTypeDecimalOrNumber<ToDataType>)
            {
                throw Exception("Illegal column " + named_from.column->getName() + " of first argument of function " + Name::name,
                    ErrorCodes::ILLEGAL_COLUMN);
            }
        }

        if (const ColVecFrom * col_from = checkAndGetColumn<ColVecFrom>(named_from.column.get()))
        {
            typename ColVecTo::MutablePtr col_to = nullptr;

            if constexpr (IsDataTypeDecimal<ToDataType>)
            {
                UInt32 scale;

                if constexpr (std::is_same_v<Additions, AccurateConvertStrategyAdditions>
                    || std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>)
                {
                    scale = additions.scale;
                }
                else
                {
                    scale = additions;
                }

                col_to = ColVecTo::create(0, scale);
            }
            else
                col_to = ColVecTo::create();

            const auto & vec_from = col_from->getData();
            auto & vec_to = col_to->getData();
            vec_to.resize(input_rows_count);

            ColumnUInt8::MutablePtr col_null_map_to;
            ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
            if constexpr (std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>)
            {
                col_null_map_to = ColumnUInt8::create(input_rows_count, false);
                vec_null_map_to = &col_null_map_to->getData();
            }

            bool result_is_bool = isBool(result_type);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if constexpr (std::is_same_v<ToDataType, DataTypeUInt8>)
                {
                    if (result_is_bool)
                    {
                        vec_to[i] = vec_from[i] != FromFieldType(0);
                        continue;
                    }
                }

                if constexpr (std::is_same_v<FromDataType, DataTypeUUID> != std::is_same_v<ToDataType, DataTypeUUID>)
                {
                    throw Exception("Conversion between numeric types and UUID is not supported", ErrorCodes::NOT_IMPLEMENTED);
                }
                else
                {
                    if constexpr (IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>)
                    {
                        if constexpr (std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>)
                        {
                            ToFieldType result;
                            bool convert_result = false;

                            if constexpr (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
                                convert_result = tryConvertDecimals<FromDataType, ToDataType>(vec_from[i], col_from->getScale(), col_to->getScale(), result);
                            else if constexpr (IsDataTypeDecimal<FromDataType> && IsDataTypeNumber<ToDataType>)
                                convert_result = tryConvertFromDecimal<FromDataType, ToDataType>(vec_from[i], col_from->getScale(), result);
                            else if constexpr (IsDataTypeNumber<FromDataType> && IsDataTypeDecimal<ToDataType>)
                                convert_result = tryConvertToDecimal<FromDataType, ToDataType>(vec_from[i], col_to->getScale(), result);

                            if (convert_result)
                                vec_to[i] = result;
                            else
                            {
                                vec_to[i] = static_cast<ToFieldType>(0);
                                (*vec_null_map_to)[i] = true;
                            }
                        }
                        else
                        {
                            if constexpr (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
                                vec_to[i] = convertDecimals<FromDataType, ToDataType>(vec_from[i], col_from->getScale(), col_to->getScale());
                            else if constexpr (IsDataTypeDecimal<FromDataType> && IsDataTypeNumber<ToDataType>)
                                vec_to[i] = convertFromDecimal<FromDataType, ToDataType>(vec_from[i], col_from->getScale());
                            else if constexpr (IsDataTypeNumber<FromDataType> && IsDataTypeDecimal<ToDataType>)
                                vec_to[i] = convertToDecimal<FromDataType, ToDataType>(vec_from[i], col_to->getScale());
                            else
                                throw Exception("Unsupported data type in conversion function", ErrorCodes::CANNOT_CONVERT_TYPE);
                        }
                    }
                    else
                    {
                        /// If From Data is Nan or Inf and we convert to integer type, throw exception
                        if constexpr (std::is_floating_point_v<FromFieldType> && !std::is_floating_point_v<ToFieldType>)
                        {
                            if (!isFinite(vec_from[i]))
                            {
                                if constexpr (std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>)
                                {
                                    vec_to[i] = 0;
                                    (*vec_null_map_to)[i] = true;
                                    continue;
                                }
                                else
                                    throw Exception("Unexpected inf or nan to integer conversion", ErrorCodes::CANNOT_CONVERT_TYPE);
                            }
                        }

                        if constexpr (std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>
                                || std::is_same_v<Additions, AccurateConvertStrategyAdditions>)
                        {
                            bool convert_result = accurate::convertNumeric(vec_from[i], vec_to[i]);

                            if (!convert_result)
                            {
                                if (std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>)
                                {
                                    vec_to[i] = 0;
                                    (*vec_null_map_to)[i] = true;
                                }
                                else
                                {
                                    throw Exception(
                                        "Value in column " + named_from.column->getName() + " cannot be safely converted into type "
                                            + result_type->getName(),
                                        ErrorCodes::CANNOT_CONVERT_TYPE);
                                }
                            }
                        }
                        else
                        {
                            vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
                        }
                    }
                }
            }

            if constexpr (std::is_same_v<Additions, AccurateOrNullConvertStrategyAdditions>)
                return ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
            else
                return col_to;
        }
        else
            throw Exception("Illegal column " + named_from.column->getName() + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

/** Conversion of DateTime to Date: throw off time component.
  */
template <typename Name> struct ConvertImpl<DataTypeDateTime, DataTypeDate, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeDateTime, DataTypeDate, ToDateImpl> {};

/** Conversion of DateTime to Date32: throw off time component.
  */
template <typename Name> struct ConvertImpl<DataTypeDateTime, DataTypeDate32, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeDateTime, DataTypeDate32, ToDate32Impl> {};

/** Conversion of Date to DateTime: adding 00:00:00 time component.
  */
struct ToDateTimeImpl
{
    static constexpr auto name = "toDateTime";

    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum(d));
    }

    static inline Int64 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(ExtendedDayNum(d));
    }

    static inline UInt32 execute(UInt32 dt, const DateLUTImpl & /*time_zone*/)
    {
        return dt;
    }

    // TODO: return UInt32 ???
    static inline Int64 execute(Int64 dt64, const DateLUTImpl & /*time_zone*/)
    {
        return dt64;
    }
};

template <typename Name> struct ConvertImpl<DataTypeDate, DataTypeDateTime, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeDate, DataTypeDateTime, ToDateTimeImpl> {};

template <typename Name> struct ConvertImpl<DataTypeDate32, DataTypeDateTime, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeDate32, DataTypeDateTime, ToDateTimeImpl> {};

/// Implementation of toDate function.

template <typename FromType, typename ToType>
struct ToDateTransform32Or64
{
    static constexpr auto name = "toDate";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl & time_zone)
    {
        // since converting to Date, no need in values outside of default LUT range.
        return (from < DATE_LUT_MAX_DAY_NUM)
            ? from
            : time_zone.toDayNum(std::min(time_t(from), time_t(0xFFFFFFFF)));
    }
};

template <typename FromType, typename ToType>
struct ToDateTransform32Or64Signed
{
    static constexpr auto name = "toDate";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl & time_zone)
    {
        // TODO: decide narrow or extended range based on FromType
        /// The function should be monotonic (better for query optimizations), so we saturate instead of overflow.
        if (from < 0)
            return 0;
        return (from < DATE_LUT_MAX_DAY_NUM)
            ? from
            : time_zone.toDayNum(std::min(time_t(from), time_t(0xFFFFFFFF)));
    }
};

template <typename FromType, typename ToType>
struct ToDateTransform8Or16Signed
{
    static constexpr auto name = "toDate";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl &)
    {
        if (from < 0)
            return 0;
        return from;
    }
};

template <typename Name> struct ConvertImpl<DataTypeDateTime64, DataTypeDate32, Name, ConvertDefaultBehaviorTag>
        : DateTimeTransformImpl<DataTypeDateTime64, DataTypeDate32, TransformDateTime64<ToDate32Impl>> {};

/// Implementation of toDate32 function.

template <typename FromType, typename ToType>
struct ToDate32Transform32Or64
{
    static constexpr auto name = "toDate32";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl & time_zone)
    {
        return (from < DATE_LUT_MAX_EXTEND_DAY_NUM)
            ? from
            : time_zone.toDayNum(std::min(time_t(from), time_t(0xFFFFFFFF)));
    }
};

template <typename FromType, typename ToType>
struct ToDate32Transform32Or64Signed
{
    static constexpr auto name = "toDate32";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl & time_zone)
    {
        static const Int32 daynum_min_offset = -static_cast<Int32>(DateLUT::instance().getDayNumOffsetEpoch());
        if (from < daynum_min_offset)
            return daynum_min_offset;
        return (from < DATE_LUT_MAX_EXTEND_DAY_NUM)
            ? from
            : time_zone.toDayNum(std::min(time_t(from), time_t(0xFFFFFFFF)));
    }
};

template <typename FromType, typename ToType>
struct ToDate32Transform8Or16Signed
{
    static constexpr auto name = "toDate32";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl &)
    {
        return from;
    }
};

/** Special case of converting Int8, Int16, (U)Int32 or (U)Int64 (and also, for convenience,
  * Float32, Float64) to Date. If the number is negative, saturate it to unix epoch time. If the
  * number is less than 65536, then it is treated as DayNum, and if it's greater or equals to 65536,
  * then treated as unix timestamp. If the number exceeds UInt32, saturate to MAX_UINT32 then as DayNum.
  * It's a bit illogical, as we actually have two functions in one.
  * But allows to support frequent case,
  *  when user write toDate(UInt32), expecting conversion of unix timestamp to Date.
  *  (otherwise such usage would be frequent mistake).
  */
template <typename Name> struct ConvertImpl<DataTypeUInt32, DataTypeDate, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeUInt32, DataTypeDate, ToDateTransform32Or64<UInt32, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeUInt64, DataTypeDate, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeUInt64, DataTypeDate, ToDateTransform32Or64<UInt64, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeInt8, DataTypeDate, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeInt8, DataTypeDate, ToDateTransform8Or16Signed<Int8, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeInt16, DataTypeDate, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeInt16, DataTypeDate, ToDateTransform8Or16Signed<Int16, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeInt32, DataTypeDate, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeInt32, DataTypeDate, ToDateTransform32Or64Signed<Int32, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeInt64, DataTypeDate, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeInt64, DataTypeDate, ToDateTransform32Or64Signed<Int64, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeFloat32, DataTypeDate, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeFloat32, DataTypeDate, ToDateTransform32Or64Signed<Float32, UInt16>> {};
template <typename Name> struct ConvertImpl<DataTypeFloat64, DataTypeDate, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeFloat64, DataTypeDate, ToDateTransform32Or64Signed<Float64, UInt16>> {};

template <typename Name> struct ConvertImpl<DataTypeUInt32, DataTypeDate32, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeUInt32, DataTypeDate32, ToDate32Transform32Or64<UInt32, Int32>> {};
template <typename Name> struct ConvertImpl<DataTypeUInt64, DataTypeDate32, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeUInt64, DataTypeDate32, ToDate32Transform32Or64<UInt64, Int32>> {};
template <typename Name> struct ConvertImpl<DataTypeInt8, DataTypeDate32, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeInt8, DataTypeDate32, ToDate32Transform8Or16Signed<Int8, Int32>> {};
template <typename Name> struct ConvertImpl<DataTypeInt16, DataTypeDate32, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeInt16, DataTypeDate32, ToDate32Transform8Or16Signed<Int16, Int32>> {};
template <typename Name> struct ConvertImpl<DataTypeInt32, DataTypeDate32, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeInt32, DataTypeDate32, ToDate32Transform32Or64Signed<Int32, Int32>> {};
template <typename Name> struct ConvertImpl<DataTypeInt64, DataTypeDate32, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeInt64, DataTypeDate32, ToDate32Transform32Or64Signed<Int64, Int32>> {};
template <typename Name> struct ConvertImpl<DataTypeFloat32, DataTypeDate32, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeFloat32, DataTypeDate32, ToDate32Transform32Or64Signed<Float32, Int32>> {};
template <typename Name> struct ConvertImpl<DataTypeFloat64, DataTypeDate32, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeFloat64, DataTypeDate32, ToDate32Transform32Or64Signed<Float64, Int32>> {};


template <typename FromType, typename ToType>
struct ToDateTimeTransform64
{
    static constexpr auto name = "toDateTime";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl &)
    {
        return std::min(time_t(from), time_t(0xFFFFFFFF));
    }
};

template <typename FromType, typename ToType>
struct ToDateTimeTransformSigned
{
    static constexpr auto name = "toDateTime";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl &)
    {
        if (from < 0)
            return 0;
        return from;
    }
};

template <typename FromType, typename ToType>
struct ToDateTimeTransform64Signed
{
    static constexpr auto name = "toDateTime";

    static inline NO_SANITIZE_UNDEFINED ToType execute(const FromType & from, const DateLUTImpl &)
    {
        if (from < 0)
            return 0;
        return std::min(time_t(from), time_t(0xFFFFFFFF));
    }
};

/** Special case of converting Int8, Int16, Int32 or (U)Int64 (and also, for convenience, Float32,
  * Float64) to DateTime. If the number is negative, saturate it to unix epoch time. If the number
  * exceeds UInt32, saturate to MAX_UINT32.
  */
template <typename Name> struct ConvertImpl<DataTypeInt8, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeInt8, DataTypeDateTime, ToDateTimeTransformSigned<Int8, UInt32>> {};
template <typename Name> struct ConvertImpl<DataTypeInt16, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeInt16, DataTypeDateTime, ToDateTimeTransformSigned<Int16, UInt32>> {};
template <typename Name> struct ConvertImpl<DataTypeInt32, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeInt32, DataTypeDateTime, ToDateTimeTransformSigned<Int32, UInt32>> {};
template <typename Name> struct ConvertImpl<DataTypeInt64, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeInt64, DataTypeDateTime, ToDateTimeTransform64Signed<Int64, UInt32>> {};
template <typename Name> struct ConvertImpl<DataTypeUInt64, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeUInt64, DataTypeDateTime, ToDateTimeTransform64<UInt64, UInt32>> {};
template <typename Name> struct ConvertImpl<DataTypeFloat32, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeFloat32, DataTypeDateTime, ToDateTimeTransform64Signed<Float32, UInt32>> {};
template <typename Name> struct ConvertImpl<DataTypeFloat64, DataTypeDateTime, Name>
    : DateTimeTransformImpl<DataTypeFloat64, DataTypeDateTime, ToDateTimeTransform64Signed<Float64, UInt32>> {};

const time_t LUT_MIN_TIME = -1420070400l;       // 1925-01-01 UTC
const time_t LUT_MAX_TIME = 9877248000l;        // 2282-12-31 UTC

/** Conversion of numeric to DateTime64
  */

template <typename FromType>
struct ToDateTime64TransformUnsigned
{
    static constexpr auto name = "toDateTime64";

    const DateTime64::NativeType scale_multiplier = 1;

    ToDateTime64TransformUnsigned(UInt32 scale = 0) /// NOLINT
        : scale_multiplier(DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale))
    {}

    inline NO_SANITIZE_UNDEFINED DateTime64::NativeType execute(FromType from, const DateLUTImpl &) const
    {
        from = std::min<time_t>(from, LUT_MAX_TIME);
        return DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(from, 0, scale_multiplier);
    }
};
template <typename FromType>
struct ToDateTime64TransformSigned
{
    static constexpr auto name = "toDateTime64";

    const DateTime64::NativeType scale_multiplier = 1;

    ToDateTime64TransformSigned(UInt32 scale = 0) /// NOLINT
        : scale_multiplier(DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale))
    {}

    inline NO_SANITIZE_UNDEFINED DateTime64::NativeType execute(FromType from, const DateLUTImpl &) const
    {
        from = std::max<time_t>(from, LUT_MIN_TIME);
        from = std::min<time_t>(from, LUT_MAX_TIME);
        return DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(from, 0, scale_multiplier);
    }
};
template <typename FromDataType, typename FromType>
struct ToDateTime64TransformFloat
{
    static constexpr auto name = "toDateTime64";

    const UInt32 scale = 1;

    ToDateTime64TransformFloat(UInt32 scale_ = 0) /// NOLINT
        : scale(scale_)
    {}

    inline NO_SANITIZE_UNDEFINED DateTime64::NativeType execute(FromType from, const DateLUTImpl &) const
    {
        if (from < 0)
            return 0;
        from = std::min<FromType>(from, FromType(0xFFFFFFFF));
        return convertToDecimal<FromDataType, DataTypeDateTime64>(from, scale);
    }
};

template <typename Name> struct ConvertImpl<DataTypeInt8, DataTypeDateTime64, Name>
    : DateTimeTransformImpl<DataTypeInt8, DataTypeDateTime64, ToDateTime64TransformSigned<Int8>> {};
template <typename Name> struct ConvertImpl<DataTypeInt16, DataTypeDateTime64, Name>
    : DateTimeTransformImpl<DataTypeInt16, DataTypeDateTime64, ToDateTime64TransformSigned<Int16>> {};
template <typename Name> struct ConvertImpl<DataTypeInt32, DataTypeDateTime64, Name>
    : DateTimeTransformImpl<DataTypeInt32, DataTypeDateTime64, ToDateTime64TransformSigned<Int32>> {};
template <typename Name> struct ConvertImpl<DataTypeInt64, DataTypeDateTime64, Name>
    : DateTimeTransformImpl<DataTypeInt64, DataTypeDateTime64, ToDateTime64TransformSigned<Int64>> {};
template <typename Name> struct ConvertImpl<DataTypeUInt64, DataTypeDateTime64, Name>
    : DateTimeTransformImpl<DataTypeUInt64, DataTypeDateTime64, ToDateTime64TransformUnsigned<UInt64>> {};
template <typename Name> struct ConvertImpl<DataTypeFloat32, DataTypeDateTime64, Name>
    : DateTimeTransformImpl<DataTypeFloat32, DataTypeDateTime64, ToDateTime64TransformFloat<DataTypeFloat32, Float32>> {};
template <typename Name> struct ConvertImpl<DataTypeFloat64, DataTypeDateTime64, Name>
    : DateTimeTransformImpl<DataTypeFloat64, DataTypeDateTime64, ToDateTime64TransformFloat<DataTypeFloat64, Float64>> {};


/** Conversion of DateTime64 to Date or DateTime: discards fractional part.
 */
template <typename Transform>
struct FromDateTime64Transform
{
    static constexpr auto name = Transform::name;

    const DateTime64::NativeType scale_multiplier = 1;

    FromDateTime64Transform(UInt32 scale) /// NOLINT
        : scale_multiplier(DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale))
    {}

    inline auto execute(DateTime64::NativeType dt, const DateLUTImpl & time_zone) const
    {
        const auto c = DecimalUtils::splitWithScaleMultiplier(DateTime64(dt), scale_multiplier);
        return Transform::execute(static_cast<UInt32>(c.whole), time_zone);
    }
};

/** Conversion of DateTime64 to Date or DateTime: discards fractional part.
 */
template <typename Name> struct ConvertImpl<DataTypeDateTime64, DataTypeDate, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeDateTime64, DataTypeDate, TransformDateTime64<ToDateImpl>> {};
template <typename Name> struct ConvertImpl<DataTypeDateTime64, DataTypeDateTime, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeDateTime64, DataTypeDateTime, TransformDateTime64<ToDateTimeImpl>> {};

struct ToDateTime64Transform
{
    static constexpr auto name = "toDateTime64";

    const DateTime64::NativeType scale_multiplier = 1;

    ToDateTime64Transform(UInt32 scale = 0) /// NOLINT
        : scale_multiplier(DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale))
    {}

    inline DateTime64::NativeType execute(UInt16 d, const DateLUTImpl & time_zone) const
    {
        const auto dt = ToDateTimeImpl::execute(d, time_zone);
        return execute(dt, time_zone);
    }

    inline DateTime64::NativeType execute(Int32 d, const DateLUTImpl & time_zone) const
    {
        const auto dt = ToDateTimeImpl::execute(d, time_zone);
        return DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(dt, 0, scale_multiplier);
    }

    inline DateTime64::NativeType execute(UInt32 dt, const DateLUTImpl & /*time_zone*/) const
    {
        return DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(dt, 0, scale_multiplier);
    }
};

/** Conversion of Date or DateTime to DateTime64: add zero sub-second part.
  */
template <typename Name> struct ConvertImpl<DataTypeDate, DataTypeDateTime64, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeDate, DataTypeDateTime64, ToDateTime64Transform> {};
template <typename Name> struct ConvertImpl<DataTypeDate32, DataTypeDateTime64, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeDate32, DataTypeDateTime64, ToDateTime64Transform> {};
template <typename Name> struct ConvertImpl<DataTypeDateTime, DataTypeDateTime64, Name, ConvertDefaultBehaviorTag>
    : DateTimeTransformImpl<DataTypeDateTime, DataTypeDateTime64, ToDateTime64Transform> {};


/** Transformation of numbers, dates, datetimes to strings: through formatting.
  */
template <typename DataType>
struct FormatImpl
{
    template <typename ReturnType = void>
    static ReturnType execute(const typename DataType::FieldType x, WriteBuffer & wb, const DataType *, const DateLUTImpl *)
    {
        writeText(x, wb);
        return ReturnType(true);
    }
};

template <>
struct FormatImpl<DataTypeDate>
{
    template <typename ReturnType = void>
    static ReturnType execute(const DataTypeDate::FieldType x, WriteBuffer & wb, const DataTypeDate *, const DateLUTImpl *)
    {
        writeDateText(DayNum(x), wb);
        return ReturnType(true);
    }
};

template <>
struct FormatImpl<DataTypeDate32>
{
    template <typename ReturnType = void>
    static ReturnType execute(const DataTypeDate32::FieldType x, WriteBuffer & wb, const DataTypeDate32 *, const DateLUTImpl *)
    {
        writeDateText(ExtendedDayNum(x), wb);
        return ReturnType(true);
    }
};

template <>
struct FormatImpl<DataTypeDateTime>
{
    template <typename ReturnType = void>
    static ReturnType execute(const DataTypeDateTime::FieldType x, WriteBuffer & wb, const DataTypeDateTime *, const DateLUTImpl * time_zone)
    {
        writeDateTimeText(x, wb, *time_zone);
        return ReturnType(true);
    }
};

template <>
struct FormatImpl<DataTypeDateTime64>
{
    template <typename ReturnType = void>
    static ReturnType execute(const DataTypeDateTime64::FieldType x, WriteBuffer & wb, const DataTypeDateTime64 * type, const DateLUTImpl * time_zone)
    {
        writeDateTimeText(DateTime64(x), type->getScale(), wb, *time_zone);
        return ReturnType(true);
    }
};


template <typename FieldType>
struct FormatImpl<DataTypeEnum<FieldType>>
{
    template <typename ReturnType = void>
    static ReturnType execute(const FieldType x, WriteBuffer & wb, const DataTypeEnum<FieldType> * type, const DateLUTImpl *)
    {
        static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

        if constexpr (throw_exception)
        {
            writeString(type->getNameForValue(x), wb);
        }
        else
        {
            StringRef res;
            bool is_ok = type->getNameForValue(x, res);
            if (is_ok)
                writeString(res, wb);
            return ReturnType(is_ok);
        }
    }
};

template <typename FieldType>
struct FormatImpl<DataTypeDecimal<FieldType>>
{
    template <typename ReturnType = void>
    static ReturnType execute(const FieldType x, WriteBuffer & wb, const DataTypeDecimal<FieldType> * type, const DateLUTImpl *)
    {
        writeText(x, type->getScale(), wb, false);
        return ReturnType(true);
    }
};


/// DataTypeEnum<T> to DataType<T> free conversion
template <typename FieldType, typename Name>
struct ConvertImpl<DataTypeEnum<FieldType>, DataTypeNumber<FieldType>, Name, ConvertDefaultBehaviorTag>
{
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/)
    {
        return arguments[0].column;
    }
};

static ColumnUInt8::MutablePtr copyNullMap(ColumnPtr col)
{
    ColumnUInt8::MutablePtr null_map = nullptr;
    if (const auto * col_null = checkAndGetColumn<ColumnNullable>(col.get()))
    {
        null_map = ColumnUInt8::create();
        null_map->insertRangeFrom(col_null->getNullMapColumn(), 0, col_null->size());
    }
    return null_map;
}

template <typename FromDataType, typename Name>
requires (!std::is_same_v<FromDataType, DataTypeString>)
struct ConvertImpl<FromDataType, DataTypeString, Name, ConvertDefaultBehaviorTag>
{
    using FromFieldType = typename FromDataType::FieldType;
    using ColVecType = ColumnVectorOrDecimal<FromFieldType>;

    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/)
    {
        ColumnUInt8::MutablePtr null_map = copyNullMap(arguments[0].column);

        const auto & col_with_type_and_name =  columnGetNested(arguments[0]);
        const auto & type = static_cast<const FromDataType &>(*col_with_type_and_name.type);

        const DateLUTImpl * time_zone = nullptr;
        /// For argument of DateTime type, second argument with time zone could be specified.
        if constexpr (std::is_same_v<FromDataType, DataTypeDateTime> || std::is_same_v<FromDataType, DataTypeDateTime64>)
        {
            auto non_null_args = createBlockWithNestedColumns(arguments);
            time_zone = &extractTimeZoneFromFunctionArguments(non_null_args, 1, 0);
        }

        if (const auto col_from = checkAndGetColumn<ColVecType>(col_with_type_and_name.column.get()))
        {
            auto col_to = ColumnString::create();

            const typename ColVecType::Container & vec_from = col_from->getData();
            ColumnString::Chars & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            size_t size = vec_from.size();

            if constexpr (std::is_same_v<FromDataType, DataTypeDate>)
                data_to.resize(size * (strlen("YYYY-MM-DD") + 1));
            else if constexpr (std::is_same_v<FromDataType, DataTypeDate32>)
                data_to.resize(size * (strlen("YYYY-MM-DD") + 1));
            else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime>)
                data_to.resize(size * (strlen("YYYY-MM-DD hh:mm:ss") + 1));
            else if constexpr (std::is_same_v<FromDataType, DataTypeDateTime64>)
                data_to.resize(size * (strlen("YYYY-MM-DD hh:mm:ss.") + col_from->getScale() + 1));
            else
                data_to.resize(size * 3);   /// Arbitrary

            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars> write_buffer(data_to);

            if (null_map)
            {
                for (size_t i = 0; i < size; ++i)
                {
                    bool is_ok = FormatImpl<FromDataType>::template execute<bool>(vec_from[i], write_buffer, &type, time_zone);
                    null_map->getData()[i] |= !is_ok;
                    writeChar(0, write_buffer);
                    offsets_to[i] = write_buffer.count();
                }
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                {
                    FormatImpl<FromDataType>::template execute<void>(vec_from[i], write_buffer, &type, time_zone);
                    writeChar(0, write_buffer);
                    offsets_to[i] = write_buffer.count();
                }
            }

            write_buffer.finalize();

            if (null_map)
                return ColumnNullable::create(std::move(col_to), std::move(null_map));
            return col_to;
        }
        else
            throw Exception("Illegal column " + arguments[0].column->getName()
                    + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Generic conversion of any type to String or FixedString via serialization to text.
template <typename StringColumnType>
struct ConvertImplGenericToString
{
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/)
    {
        static_assert(std::is_same_v<StringColumnType, ColumnString> || std::is_same_v<StringColumnType, ColumnFixedString>,
                "Can be used only to serialize to ColumnString or ColumnFixedString");

        ColumnUInt8::MutablePtr null_map = copyNullMap(arguments[0].column);

        const auto & col_with_type_and_name = columnGetNested(arguments[0]);
        const IDataType & type = *col_with_type_and_name.type;
        const IColumn & col_from = *col_with_type_and_name.column;

        size_t size = col_from.size();
        auto col_to = removeNullable(result_type)->createColumn();

        {
            ColumnStringHelpers::WriteHelper write_helper(
                    assert_cast<StringColumnType &>(*col_to),
                    size);

            auto & write_buffer = write_helper.getWriteBuffer();

            FormatSettings format_settings;
            auto serialization = type.getDefaultSerialization();
            for (size_t i = 0; i < size; ++i)
            {
                serialization->serializeText(col_from, i, write_buffer, format_settings);
                write_helper.rowWritten();
            }

            write_helper.finalize();
        }

        if (result_type->isNullable() && null_map)
            return ColumnNullable::create(std::move(col_to), std::move(null_map));
        return col_to;
    }
};

/** Conversion of time_t to UInt16, Int32, UInt32
  */
template <typename DataType>
void convertFromTime(typename DataType::FieldType & x, time_t & time)
{
    x = time;
}

template <>
inline void convertFromTime<DataTypeDate>(DataTypeDate::FieldType & x, time_t & time)
{
    if (unlikely(time < 0))
        x = 0;
    else if (unlikely(time > 0xFFFF))
        x = 0xFFFF;
    else
        x = time;
}

template <>
inline void convertFromTime<DataTypeDate32>(DataTypeDate32::FieldType & x, time_t & time)
{
    x = time;
}

template <>
inline void convertFromTime<DataTypeDateTime>(DataTypeDateTime::FieldType & x, time_t & time)
{
    if (unlikely(time < 0))
        x = 0;
    else if (unlikely(time > 0xFFFFFFFF))
        x = 0xFFFFFFFF;
    else
        x = time;
}

/** Conversion of strings to numbers, dates, datetimes: through parsing.
  */
template <typename DataType>
void parseImpl(typename DataType::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    readText(x, rb);
}

template <>
inline void parseImpl<DataTypeDate>(DataTypeDate::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    DayNum tmp(0);
    readDateText(tmp, rb);
    x = tmp;
}

template <>
inline void parseImpl<DataTypeDate32>(DataTypeDate32::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    ExtendedDayNum tmp(0);
    readDateText(tmp, rb);
    x = tmp;
}


// NOTE: no need of extra overload of DateTime64, since readDateTimeText64 has different signature and that case is explicitly handled in the calling code.
template <>
inline void parseImpl<DataTypeDateTime>(DataTypeDateTime::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone)
{
    time_t time = 0;
    readDateTimeText(time, rb, *time_zone);
    convertFromTime<DataTypeDateTime>(x, time);
}

template <>
inline void parseImpl<DataTypeUUID>(DataTypeUUID::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    UUID tmp;
    readUUIDText(tmp, rb);
    x = tmp.toUnderType();
}

template <typename DataType>
bool tryParseImpl(typename DataType::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    if constexpr (std::is_floating_point_v<typename DataType::FieldType>)
        return tryReadFloatText(x, rb);
    else /*if constexpr (is_integer_v<typename DataType::FieldType>)*/
        return tryReadIntText(x, rb);
}

template <>
inline bool tryParseImpl<DataTypeDate>(DataTypeDate::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    DayNum tmp(0);
    if (!tryReadDateText(tmp, rb))
        return false;
    x = tmp;
    return true;
}

template <>
inline bool tryParseImpl<DataTypeDate32>(DataTypeDate32::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    ExtendedDayNum tmp(0);
    if (!tryReadDateText(tmp, rb))
    {
        return false;
    }
    x = tmp;
    return true;
}

template <>
inline bool tryParseImpl<DataTypeDateTime>(DataTypeDateTime::FieldType & x, ReadBuffer & rb, const DateLUTImpl * time_zone)
{
    time_t tmp = 0;
    if (!tryReadDateTimeText(tmp, rb, *time_zone))
        return false;
    x = tmp;
    return true;
}

template <>
inline bool tryParseImpl<DataTypeUUID>(DataTypeUUID::FieldType & x, ReadBuffer & rb, const DateLUTImpl *)
{
    UUID tmp;
    if (!tryReadUUIDText(tmp, rb))
        return false;

    x = tmp.toUnderType();
    return true;
}


/** Throw exception with verbose message when string value is not parsed completely.
  */
[[noreturn]] inline void throwExceptionForIncompletelyParsedValue(ReadBuffer & read_buffer, const IDataType & result_type)
{
    WriteBufferFromOwnString message_buf;
    message_buf << "Cannot parse string " << quote << String(read_buffer.buffer().begin(), read_buffer.buffer().size())
                << " as " << result_type.getName()
                << ": syntax error";

    if (read_buffer.offset())
        message_buf << " at position " << read_buffer.offset()
                    << " (parsed just " << quote << String(read_buffer.buffer().begin(), read_buffer.offset()) << ")";
    else
        message_buf << " at begin of string";

    // Currently there are no functions toIPv{4,6}Or{Null,Zero}
    if (isNativeNumber(result_type) && !(result_type.getName() == "IPv4" || result_type.getName() == "IPv6"))
        message_buf << ". Note: there are to" << result_type.getName() << "OrZero and to" << result_type.getName() << "OrNull functions, which returns zero/NULL instead of throwing exception.";

    throw Exception(message_buf.str(), ErrorCodes::CANNOT_PARSE_TEXT);
}


enum class ConvertFromStringExceptionMode
{
    Throw,  /// Throw exception if value cannot be parsed.
    Zero,   /// Fill with zero or default if value cannot be parsed.
    Null    /// Return ColumnNullable with NULLs when value cannot be parsed.
};

enum class ConvertFromStringParsingMode
{
    Normal,
    BestEffort,  /// Only applicable for DateTime. Will use sophisticated method, that is slower.
    BestEffortUS
};

template <typename FromDataType, typename ToDataType, typename Name,
    ConvertFromStringExceptionMode exception_mode, ConvertFromStringParsingMode parsing_mode>
struct ConvertThroughParsing
{
    static_assert(std::is_same_v<FromDataType, DataTypeString> || std::is_same_v<FromDataType, DataTypeFixedString>,
        "ConvertThroughParsing is only applicable for String or FixedString data types");

    static constexpr bool to_datetime64 = std::is_same_v<ToDataType, DataTypeDateTime64>;

    // using ToFieldType = typename ToDataType::FieldType;

    static bool isAllRead(ReadBuffer & in)
    {
        /// In case of FixedString, skip zero bytes at end.
        if constexpr (std::is_same_v<FromDataType, DataTypeFixedString>)
            while (!in.eof() && *in.position() == 0)
                ++in.position();

        if (in.eof())
            return true;

        /// Special case, that allows to parse string with DateTime as Date.
        if (std::is_same_v<ToDataType, DataTypeDate> && (in.buffer().size()) == strlen("YYYY-MM-DD hh:mm:ss"))
            return true;

        return false;
    }

    template <typename Additions = void *>
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, size_t input_rows_count,
                        Additions additions [[maybe_unused]] = Additions())
    {
        using ColVecTo = typename ToDataType::ColumnType;

        const DateLUTImpl * local_time_zone [[maybe_unused]] = nullptr;
        const DateLUTImpl * utc_time_zone [[maybe_unused]] = nullptr;

        /// For conversion to DateTime type, second argument with time zone could be specified.
        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime> || to_datetime64)
        {
            const auto result_type = removeNullable(res_type);
            // Time zone is already figured out during result type resolution, no need to do it here.
            if (const auto dt_col = checkAndGetDataType<ToDataType>(result_type.get()))
                local_time_zone = &dt_col->getTimeZone();
            else
            {
                local_time_zone = &extractTimeZoneFromFunctionArguments(arguments, 1, 0);
            }

            if constexpr (parsing_mode == ConvertFromStringParsingMode::BestEffort || parsing_mode == ConvertFromStringParsingMode::BestEffortUS)
                utc_time_zone = &DateLUT::instance("UTC");
        }

        const IColumn * col_from = arguments[0].column.get();
        const ColumnString * col_from_string = checkAndGetColumn<ColumnString>(col_from);
        const ColumnFixedString * col_from_fixed_string = checkAndGetColumn<ColumnFixedString>(col_from);

        if (std::is_same_v<FromDataType, DataTypeString> && !col_from_string)
            throw Exception("Illegal column " + col_from->getName()
                + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);

        if (std::is_same_v<FromDataType, DataTypeFixedString> && !col_from_fixed_string)
            throw Exception("Illegal column " + col_from->getName()
                + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);

        size_t size = input_rows_count;
        typename ColVecTo::MutablePtr col_to = nullptr;

        if constexpr (IsDataTypeDecimal<ToDataType>)
        {
            UInt32 scale = additions;
            if constexpr (to_datetime64)
            {
                ToDataType check_bounds_in_ctor(scale, local_time_zone ? local_time_zone->getTimeZone() : String{});
            }
            else
            {
                ToDataType check_bounds_in_ctor(ToDataType::maxPrecision(), scale);
            }
            col_to = ColVecTo::create(size, scale);
        }
        else
            col_to = ColVecTo::create(size);

        typename ColVecTo::Container & vec_to = col_to->getData();

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
        if constexpr (exception_mode == ConvertFromStringExceptionMode::Null)
        {
            col_null_map_to = ColumnUInt8::create(size);
            vec_null_map_to = &col_null_map_to->getData();
        }

        const ColumnString::Chars * chars = nullptr;
        const IColumn::Offsets * offsets = nullptr;
        size_t fixed_string_size = 0;

        if constexpr (std::is_same_v<FromDataType, DataTypeString>)
        {
            chars = &col_from_string->getChars();
            offsets = &col_from_string->getOffsets();
        }
        else
        {
            chars = &col_from_fixed_string->getChars();
            fixed_string_size = col_from_fixed_string->getN();
        }

        size_t current_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            size_t next_offset = std::is_same_v<FromDataType, DataTypeString> ? (*offsets)[i] : (current_offset + fixed_string_size);
            size_t string_size = std::is_same_v<FromDataType, DataTypeString> ? next_offset - current_offset - 1 : fixed_string_size;

            ReadBufferFromMemory read_buffer(&(*chars)[current_offset], string_size);

            if constexpr (exception_mode == ConvertFromStringExceptionMode::Throw)
            {
                if constexpr (parsing_mode == ConvertFromStringParsingMode::BestEffort)
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 res = 0;
                        parseDateTime64BestEffort(res, col_to->getScale(), read_buffer, *local_time_zone, *utc_time_zone);
                        vec_to[i] = res;
                    }
                    else
                    {
                        time_t res;
                        parseDateTimeBestEffort(res, read_buffer, *local_time_zone, *utc_time_zone);
                        convertFromTime<ToDataType>(vec_to[i], res);
                    }
                }
                else if constexpr (parsing_mode == ConvertFromStringParsingMode::BestEffortUS)
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 res = 0;
                        parseDateTime64BestEffortUS(res, col_to->getScale(), read_buffer, *local_time_zone, *utc_time_zone);
                        vec_to[i] = res;
                    }
                    else
                    {
                        time_t res;
                        parseDateTimeBestEffortUS(res, read_buffer, *local_time_zone, *utc_time_zone);
                        convertFromTime<ToDataType>(vec_to[i], res);
                    }
                }
                else
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 value = 0;
                        readDateTime64Text(value, col_to->getScale(), read_buffer, *local_time_zone);
                        vec_to[i] = value;
                    }
                    else if constexpr (IsDataTypeDecimal<ToDataType>)
                        SerializationDecimal<typename ToDataType::FieldType>::readText(
                            vec_to[i], read_buffer, ToDataType::maxPrecision(), col_to->getScale());
                    else
                    {
                        parseImpl<ToDataType>(vec_to[i], read_buffer, local_time_zone);
                    }
                }

                if (!isAllRead(read_buffer))
                    throwExceptionForIncompletelyParsedValue(read_buffer, *res_type);
            }
            else
            {
                bool parsed;

                if constexpr (parsing_mode == ConvertFromStringParsingMode::BestEffort)
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 res = 0;
                        parsed = tryParseDateTime64BestEffort(res, col_to->getScale(), read_buffer, *local_time_zone, *utc_time_zone);
                        vec_to[i] = res;
                    }
                    else
                    {
                        time_t res;
                        parsed = tryParseDateTimeBestEffort(res, read_buffer, *local_time_zone, *utc_time_zone);
                        convertFromTime<ToDataType>(vec_to[i],res);
                    }
                }
                else if constexpr (parsing_mode == ConvertFromStringParsingMode::BestEffortUS)
                {
                    time_t res;
                    parsed = tryParseDateTimeBestEffortUS(res, read_buffer, *local_time_zone, *utc_time_zone);
                    convertFromTime<ToDataType>(vec_to[i],res);
                }
                else
                {
                    if constexpr (to_datetime64)
                    {
                        DateTime64 value = 0;
                        parsed = tryReadDateTime64Text(value, col_to->getScale(), read_buffer, *local_time_zone);
                        vec_to[i] = value;
                    }
                    else if constexpr (IsDataTypeDecimal<ToDataType>)
                        parsed = SerializationDecimal<typename ToDataType::FieldType>::tryReadText(
                            vec_to[i], read_buffer, ToDataType::maxPrecision(), col_to->getScale());
                    else
                        parsed = tryParseImpl<ToDataType>(vec_to[i], read_buffer, local_time_zone);
                }

                if (!isAllRead(read_buffer))
                    parsed = false;

                if (!parsed)
                {
                    if constexpr (std::is_same_v<ToDataType, DataTypeDate32>)
                    {
                        vec_to[i] = -static_cast<Int32>(DateLUT::instance().getDayNumOffsetEpoch());
                    }
                    else
                    {
                        vec_to[i] = static_cast<typename ToDataType::FieldType>(0);
                    }
                }

                if constexpr (exception_mode == ConvertFromStringExceptionMode::Null)
                    (*vec_null_map_to)[i] = !parsed;
            }

            current_offset = next_offset;
        }

        if constexpr (exception_mode == ConvertFromStringExceptionMode::Null)
            return ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        else
            return col_to;
    }
};


template <typename ToDataType, typename Name>
requires (!std::is_same_v<ToDataType, DataTypeString>)
struct ConvertImpl<DataTypeString, ToDataType, Name, ConvertDefaultBehaviorTag>
    : ConvertThroughParsing<DataTypeString, ToDataType, Name, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::Normal> {};

template <typename ToDataType, typename Name>
requires (!std::is_same_v<ToDataType, DataTypeFixedString>)
struct ConvertImpl<DataTypeFixedString, ToDataType, Name, ConvertDefaultBehaviorTag>
    : ConvertThroughParsing<DataTypeFixedString, ToDataType, Name, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::Normal> {};

template <typename ToDataType, typename Name>
requires (!std::is_same_v<ToDataType, DataTypeString>)
struct ConvertImpl<DataTypeString, ToDataType, Name, ConvertReturnNullOnErrorTag>
    : ConvertThroughParsing<DataTypeString, ToDataType, Name, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::Normal> {};

template <typename ToDataType, typename Name>
requires (!std::is_same_v<ToDataType, DataTypeFixedString>)
struct ConvertImpl<DataTypeFixedString, ToDataType, Name, ConvertReturnNullOnErrorTag>
    : ConvertThroughParsing<DataTypeFixedString, ToDataType, Name, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::Normal> {};

/// Generic conversion of any type from String. Used for complex types: Array and Tuple or types with custom serialization.
template <typename StringColumnType>
struct ConvertImplGenericFromString
{
    static ColumnPtr execute(ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count)
    {
        static_assert(std::is_same_v<StringColumnType, ColumnString> || std::is_same_v<StringColumnType, ColumnFixedString>,
                "Can be used only to parse from ColumnString or ColumnFixedString");

        const IColumn & column_from = *arguments[0].column;
        const IDataType & data_type_to = *result_type;
        auto res = data_type_to.createColumn();
        auto serialization = data_type_to.getDefaultSerialization();
        const auto * null_map = column_nullable ? &column_nullable->getNullMapData() : nullptr;

        executeImpl(column_from, *res, *serialization, input_rows_count, null_map, result_type.get());
        return res;
    }

    static void executeImpl(
        const IColumn & column_from,
        IColumn & column_to,
        const ISerialization & serialization_from,
        size_t input_rows_count,
        const PaddedPODArray<UInt8> * null_map = nullptr,
        const IDataType * result_type = nullptr)
    {
        static_assert(std::is_same_v<StringColumnType, ColumnString> || std::is_same_v<StringColumnType, ColumnFixedString>,
                "Can be used only to parse from ColumnString or ColumnFixedString");

        if (const StringColumnType * col_from_string = checkAndGetColumn<StringColumnType>(&column_from))
        {
            column_to.reserve(input_rows_count);

            FormatSettings format_settings;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (null_map && (*null_map)[i])
                {
                    column_to.insertDefault();
                    continue;
                }

                const auto & val = col_from_string->getDataAt(i);
                ReadBufferFromMemory read_buffer(val.data, val.size);
                serialization_from.deserializeWholeText(column_to, read_buffer, format_settings);

                if (!read_buffer.eof())
                {
                    if (result_type)
                        throwExceptionForIncompletelyParsedValue(read_buffer, *result_type);
                    else
                        throw Exception(ErrorCodes::CANNOT_PARSE_TEXT,
                            "Cannot parse string to column {}. Expected eof", column_to.getName());
                }
            }
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of conversion function from string",
                column_from.getName());
    }

};


template <>
struct ConvertImpl<DataTypeString, DataTypeUInt32, NameToUnixTimestamp, ConvertDefaultBehaviorTag>
    : ConvertImpl<DataTypeString, DataTypeDateTime, NameToUnixTimestamp, ConvertDefaultBehaviorTag> {};

template <>
struct ConvertImpl<DataTypeString, DataTypeUInt32, NameToUnixTimestamp, ConvertReturnNullOnErrorTag>
    : ConvertImpl<DataTypeString, DataTypeDateTime, NameToUnixTimestamp, ConvertReturnNullOnErrorTag> {};

/** If types are identical, just take reference to column.
  */
template <typename T, typename Name>
requires (!T::is_parametric)
struct ConvertImpl<T, T, Name, ConvertDefaultBehaviorTag>
{
    template <typename Additions = void *>
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/,
        Additions additions [[maybe_unused]] = Additions())
    {
        return arguments[0].column;
    }
};

template <typename Name>
struct ConvertImpl<DataTypeUInt8, DataTypeUInt8, Name, ConvertDefaultBehaviorTag>
{
    template <typename Additions = void *>
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/,
                             Additions additions [[maybe_unused]] = Additions())
    {

        return arguments[0].column;
    }
};


/** Conversion from FixedString to String.
  * Cutting sequences of zero bytes from end of strings.
  */
template <typename Name>
struct ConvertImpl<DataTypeFixedString, DataTypeString, Name, ConvertDefaultBehaviorTag>
{
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/)
    {
        if (const ColumnFixedString * col_from = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get()))
        {
            auto col_to = ColumnString::create();

            const ColumnFixedString::Chars & data_from = col_from->getChars();
            ColumnString::Chars & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            size_t size = col_from->size();
            size_t n = col_from->getN();
            data_to.resize(size * (n + 1)); /// + 1 - zero terminator
            offsets_to.resize(size);

            size_t offset_from = 0;
            size_t offset_to = 0;
            for (size_t i = 0; i < size; ++i)
            {
                size_t bytes_to_copy = n;
                while (bytes_to_copy > 0 && data_from[offset_from + bytes_to_copy - 1] == 0)
                    --bytes_to_copy;

                memcpy(&data_to[offset_to], &data_from[offset_from], bytes_to_copy);
                offset_from += n;
                offset_to += bytes_to_copy;
                data_to[offset_to] = 0;
                ++offset_to;
                offsets_to[i] = offset_to;
            }

            data_to.resize(offset_to);
            return col_to;
        }
        else
            throw Exception("Illegal column " + arguments[0].column->getName()
                    + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Declared early because used below.
struct NameToDate { static constexpr auto name = "toDate"; };
struct NameToDate32 { static constexpr auto name = "toDate32"; };
struct NameToDateTime { static constexpr auto name = "toDateTime"; };
struct NameToDateTime32 { static constexpr auto name = "toDateTime32"; };
struct NameToDateTime64 { static constexpr auto name = "toDateTime64"; };
struct NameToString { static constexpr auto name = "toString"; };
struct NameToDecimal32 { static constexpr auto name = "toDecimal32"; };
struct NameToDecimal64 { static constexpr auto name = "toDecimal64"; };
struct NameToDecimal128 { static constexpr auto name = "toDecimal128"; };
struct NameToDecimal256 { static constexpr auto name = "toDecimal256"; };


#define DEFINE_NAME_TO_INTERVAL(INTERVAL_KIND) \
    struct NameToInterval ## INTERVAL_KIND \
    { \
        static constexpr auto name = "toInterval" #INTERVAL_KIND; \
        static constexpr auto kind = IntervalKind::INTERVAL_KIND; \
    };

DEFINE_NAME_TO_INTERVAL(Nanosecond)
DEFINE_NAME_TO_INTERVAL(Microsecond)
DEFINE_NAME_TO_INTERVAL(Millisecond)
DEFINE_NAME_TO_INTERVAL(Second)
DEFINE_NAME_TO_INTERVAL(Minute)
DEFINE_NAME_TO_INTERVAL(Hour)
DEFINE_NAME_TO_INTERVAL(Day)
DEFINE_NAME_TO_INTERVAL(Week)
DEFINE_NAME_TO_INTERVAL(Month)
DEFINE_NAME_TO_INTERVAL(Quarter)
DEFINE_NAME_TO_INTERVAL(Year)

#undef DEFINE_NAME_TO_INTERVAL

struct NameParseDateTimeBestEffort;
struct NameParseDateTimeBestEffortOrZero;
struct NameParseDateTimeBestEffortOrNull;

template<typename Name, typename ToDataType>
static inline bool isDateTime64(const ColumnsWithTypeAndName & arguments)
{
    if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
        return true;
    else if constexpr (std::is_same_v<Name, NameToDateTime> || std::is_same_v<Name, NameParseDateTimeBestEffort>
        || std::is_same_v<Name, NameParseDateTimeBestEffortOrZero> || std::is_same_v<Name, NameParseDateTimeBestEffortOrNull>)
    {
        return (arguments.size() == 2 && isUnsignedInteger(arguments[1].type)) || arguments.size() == 3;
    }

    return false;
}

template <typename ToDataType, typename Name, typename MonotonicityImpl>
class FunctionConvert : public IFunction
{
public:
    using Monotonic = MonotonicityImpl;

    static constexpr auto name = Name::name;
    static constexpr bool to_decimal =
        std::is_same_v<Name, NameToDecimal32> || std::is_same_v<Name, NameToDecimal64>
         || std::is_same_v<Name, NameToDecimal128> || std::is_same_v<Name, NameToDecimal256>;

    static constexpr bool to_datetime64 = std::is_same_v<ToDataType, DataTypeDateTime64>;

    static constexpr bool to_string_or_fixed_string = std::is_same_v<ToDataType, DataTypeFixedString> ||
                                                      std::is_same_v<ToDataType, DataTypeString>;

    static constexpr bool to_date_or_datetime = std::is_same_v<ToDataType, DataTypeDate> ||
                                                std::is_same_v<ToDataType, DataTypeDate32> ||
                                                std::is_same_v<ToDataType, DataTypeDateTime>;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionConvert>(); }
    static FunctionPtr create() { return std::make_shared<FunctionConvert>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return std::is_same_v<Name, NameToString>; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        /// TODO: We can make more optimizations here.
        return !(to_date_or_datetime && isNumber(*arguments[0].type));
    }

    using DefaultReturnTypeGetter = std::function<DataTypePtr(const ColumnsWithTypeAndName &)>;
    static DataTypePtr getReturnTypeDefaultImplementationForNulls(const ColumnsWithTypeAndName & arguments, const DefaultReturnTypeGetter & getter)
    {
        NullPresence null_presence = getNullPresense(arguments);

        if (null_presence.has_null_constant)
        {
            return makeNullable(std::make_shared<DataTypeNothing>());
        }
        if (null_presence.has_nullable)
        {
            auto nested_columns = Block(createBlockWithNestedColumns(arguments));
            auto return_type = getter(ColumnsWithTypeAndName(nested_columns.begin(), nested_columns.end()));
            return makeNullable(return_type);
        }

        return getter(arguments);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto getter = [&] (const auto & args) { return getReturnTypeImplRemovedNullable(args); };
        auto res = getReturnTypeDefaultImplementationForNulls(arguments, getter);
        to_nullable = res->isNullable();
        checked_return_type = true;
        return res;
    }

    DataTypePtr getReturnTypeImplRemovedNullable(const ColumnsWithTypeAndName & arguments) const
    {
        FunctionArgumentDescriptors mandatory_args = {{"Value", nullptr, nullptr, nullptr}};
        FunctionArgumentDescriptors optional_args;

        if constexpr (to_decimal)
        {
            mandatory_args.push_back({"scale", &isNativeInteger<IDataType>, &isColumnConst, "const Integer"});
        }

        if (!to_decimal && isDateTime64<Name, ToDataType>(arguments))
        {
            mandatory_args.push_back({"scale", &isNativeInteger<IDataType>, &isColumnConst, "const Integer"});
        }

        // toString(DateTime or DateTime64, [timezone: String])
        if ((std::is_same_v<Name, NameToString> && !arguments.empty() && (isDateTime64(arguments[0].type) || isDateTime(arguments[0].type)))
            // toUnixTimestamp(value[, timezone : String])
            || std::is_same_v<Name, NameToUnixTimestamp>
            // toDate(value[, timezone : String])
            || std::is_same_v<ToDataType, DataTypeDate> // TODO: shall we allow timestamp argument for toDate? DateTime knows nothing about timezones and this argument is ignored below.
            // toDate(value[, timezone : String])
            || std::is_same_v<ToDataType, DataTypeDate32>
            // toDateTime(value[, timezone: String])
            || std::is_same_v<ToDataType, DataTypeDateTime>
            // toDateTime64(value, scale : Integer[, timezone: String])
            || std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            optional_args.push_back({"timezone", &isString<IDataType>, &isColumnConst, "const String"});
        }

        validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

        if constexpr (std::is_same_v<ToDataType, DataTypeInterval>)
        {
            return std::make_shared<DataTypeInterval>(Name::kind);
        }
        else if constexpr (to_decimal)
        {
            UInt64 scale = extractToDecimalScale(arguments[1]);

            if constexpr (std::is_same_v<Name, NameToDecimal32>)
                return createDecimalMaxPrecision<Decimal32>(scale);
            else if constexpr (std::is_same_v<Name, NameToDecimal64>)
                return createDecimalMaxPrecision<Decimal64>(scale);
            else if constexpr (std::is_same_v<Name, NameToDecimal128>)
                return createDecimalMaxPrecision<Decimal128>(scale);
            else if constexpr (std::is_same_v<Name, NameToDecimal256>)
                return createDecimalMaxPrecision<Decimal256>(scale);

            throw Exception("Unexpected branch in code of conversion function: it is a bug.", ErrorCodes::LOGICAL_ERROR);
        }
        else
        {
            // Optional second argument with time zone for DateTime.
            UInt8 timezone_arg_position = 1;
            UInt32 scale [[maybe_unused]] = DataTypeDateTime64::default_scale;

            // DateTime64 requires more arguments: scale and timezone. Since timezone is optional, scale should be first.
            if (isDateTime64<Name, ToDataType>(arguments))
            {
                timezone_arg_position += 1;
                scale = static_cast<UInt32>(arguments[1].column->get64(0));

                if (to_datetime64 || scale != 0) /// toDateTime('xxxx-xx-xx xx:xx:xx', 0) return DateTime
                    return std::make_shared<DataTypeDateTime64>(scale,
                        extractTimeZoneNameFromFunctionArguments(arguments, timezone_arg_position, 0));

                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, timezone_arg_position, 0));
            }

            if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, timezone_arg_position, 0));
            else if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
                throw Exception("Unexpected branch in code of conversion function: it is a bug.", ErrorCodes::LOGICAL_ERROR);
            else
                return std::make_shared<ToDataType>();
        }
    }

    /// Function actually uses default implementation for nulls,
    /// but we need to know if return type is Nullable or not,
    /// so we use checked_return_type only to intercept the first call to getReturnTypeImpl(...).
    bool useDefaultImplementationForNulls() const override
    {
        bool to_nullable_string = to_nullable && std::is_same_v<ToDataType, DataTypeString>;
        return checked_return_type && !to_nullable_string;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
            return {2};
        return {1};
    }
    bool canBeExecutedOnDefaultArguments() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        try
        {
            return executeInternal(arguments, result_type, input_rows_count);
        }
        catch (Exception & e)
        {
            /// More convenient error message.
            if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            {
                e.addMessage("Cannot parse "
                    + result_type->getName() + " from "
                    + arguments[0].type->getName()
                    + ", because value is too short");
            }
            else if (e.code() == ErrorCodes::CANNOT_PARSE_NUMBER
                || e.code() == ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT
                || e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED
                || e.code() == ErrorCodes::CANNOT_PARSE_QUOTED_STRING
                || e.code() == ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE
                || e.code() == ErrorCodes::CANNOT_PARSE_DATE
                || e.code() == ErrorCodes::CANNOT_PARSE_DATETIME
                || e.code() == ErrorCodes::CANNOT_PARSE_UUID)
            {
                e.addMessage("Cannot parse "
                    + result_type->getName() + " from "
                    + arguments[0].type->getName());
            }

            throw;
        }
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return Monotonic::has();
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return Monotonic::get(type, left, right);
    }

private:
    mutable bool checked_return_type = false;
    mutable bool to_nullable = false;

    ColumnPtr executeInternal(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        if (arguments.empty())
            throw Exception{"Function " + getName() + " expects at least 1 argument",
               ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};

        if (result_type->onlyNull())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        const DataTypePtr from_type = removeNullable(arguments[0].type);
        ColumnPtr result_column;

        auto call = [&](const auto & types, const auto & tag) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;
            using SpecialTag = std::decay_t<decltype(tag)>;

            if constexpr (IsDataTypeDecimal<RightDataType>)
            {
                if constexpr (std::is_same_v<RightDataType, DataTypeDateTime64>)
                {
                    /// Account for optional timezone argument.
                    if (arguments.size() != 2 && arguments.size() != 3)
                        throw Exception{"Function " + getName() + " expects 2 or 3 arguments for DataTypeDateTime64.",
                            ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};
                }
                else if (arguments.size() != 2)
                {
                    throw Exception{"Function " + getName() + " expects 2 arguments for Decimal.",
                        ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};
                }

                const ColumnWithTypeAndName & scale_column = arguments[1];
                UInt32 scale = extractToDecimalScale(scale_column);

                result_column = ConvertImpl<LeftDataType, RightDataType, Name, SpecialTag>::execute(arguments, result_type, input_rows_count, scale);
            }
            else if constexpr (IsDataTypeDateOrDateTime<RightDataType> && std::is_same_v<LeftDataType, DataTypeDateTime64>)
            {
                const auto * dt64 = assert_cast<const DataTypeDateTime64 *>(arguments[0].type.get());
                result_column = ConvertImpl<LeftDataType, RightDataType, Name, SpecialTag>::execute(arguments, result_type, input_rows_count, dt64->getScale());
            }
            else if constexpr (IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType>)
            {
                using LeftT = typename LeftDataType::FieldType;
                using RightT = typename RightDataType::FieldType;

                static constexpr bool bad_left =
                    is_decimal<LeftT> || std::is_floating_point_v<LeftT> || is_big_int_v<LeftT> || is_signed_v<LeftT>;
                static constexpr bool bad_right =
                    is_decimal<RightT> || std::is_floating_point_v<RightT> || is_big_int_v<RightT> || is_signed_v<RightT>;

                /// Disallow int vs UUID conversion (but support int vs UInt128 conversion)
                if constexpr ((bad_left && std::is_same_v<RightDataType, DataTypeUUID>) ||
                              (bad_right && std::is_same_v<LeftDataType, DataTypeUUID>))
                {
                    throw Exception("Wrong UUID conversion", ErrorCodes::CANNOT_CONVERT_TYPE);
                }
                else
                {
                    result_column
                        = ConvertImpl<LeftDataType, RightDataType, Name, SpecialTag>::execute(arguments, result_type, input_rows_count);
                }
            }
            else
            {
                result_column = ConvertImpl<LeftDataType, RightDataType, Name, SpecialTag>::execute(arguments, result_type, input_rows_count);
            }

            return true;
        };

        if (isDateTime64<Name, ToDataType>(arguments))
        {
            /// For toDateTime('xxxx-xx-xx xx:xx:xx.00', 2[, 'timezone']) we need to it convert to DateTime64
            const ColumnWithTypeAndName & scale_column = arguments[1];
            UInt32 scale = extractToDecimalScale(scale_column);

            if (to_datetime64 || scale != 0) /// When scale = 0, the data type is DateTime otherwise the data type is DateTime64
            {
                if (!callOnIndexAndDataType<DataTypeDateTime64>(from_type->getTypeId(), call, ConvertDefaultBehaviorTag{}))
                    throw Exception("Illegal type " + arguments[0].type->getName() + " of argument of function " + getName(),
                                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

                return result_column;
            }
        }

        if constexpr (std::is_same_v<ToDataType, DataTypeString>)
        {
            if (from_type->getCustomSerialization())
                return ConvertImplGenericToString<ColumnString>::execute(arguments, result_type, input_rows_count);
        }

        bool done;
        if constexpr (to_string_or_fixed_string)
        {
            done = callOnIndexAndDataType<ToDataType>(from_type->getTypeId(), call, ConvertDefaultBehaviorTag{});
        }
        else
        {
            /// We should use ConvertFromStringExceptionMode::Null mode when converting from String (or FixedString)
            /// to Nullable type, to avoid 'value is too short' error on attempt to parse empty string from NULL values.
            if (to_nullable && WhichDataType(from_type).isStringOrFixedString())
                done = callOnIndexAndDataType<ToDataType>(from_type->getTypeId(), call, ConvertReturnNullOnErrorTag{});
            else
                done = callOnIndexAndDataType<ToDataType>(from_type->getTypeId(), call, ConvertDefaultBehaviorTag{});
        }

        if (!done)
        {
            /// Generic conversion of any type to String.
            if (std::is_same_v<ToDataType, DataTypeString>)
            {
                return ConvertImplGenericToString<ColumnString>::execute(arguments, result_type, input_rows_count);
            }
            else
                throw Exception("Illegal type " + arguments[0].type->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return result_column;
    }
};


/** Function toTOrZero (where T is number of date or datetime type):
  *  try to convert from String to type T through parsing,
  *  if cannot parse, return default value instead of throwing exception.
  * Function toTOrNull will return Nullable type with NULL when cannot parse.
  * NOTE Also need to implement tryToUnixTimestamp with timezone.
  */
template <typename ToDataType, typename Name,
    ConvertFromStringExceptionMode exception_mode,
    ConvertFromStringParsingMode parsing_mode = ConvertFromStringParsingMode::Normal>
class FunctionConvertFromString : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static constexpr bool to_decimal =
        std::is_same_v<ToDataType, DataTypeDecimal<Decimal32>> ||
        std::is_same_v<ToDataType, DataTypeDecimal<Decimal64>> ||
        std::is_same_v<ToDataType, DataTypeDecimal<Decimal128>> ||
        std::is_same_v<ToDataType, DataTypeDecimal<Decimal256>>;

    static constexpr bool to_datetime64 = std::is_same_v<ToDataType, DataTypeDateTime64>;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionConvertFromString>(); }
    static FunctionPtr create() { return std::make_shared<FunctionConvertFromString>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool canBeExecutedOnDefaultArguments() const override { return false; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypePtr res;

        if (isDateTime64<Name, ToDataType>(arguments))
        {
            validateFunctionArgumentTypes(*this, arguments,
                FunctionArgumentDescriptors{{"string", &isStringOrFixedString<IDataType>, nullptr, "String or FixedString"}},
                // optional
                FunctionArgumentDescriptors{
                    {"precision", &isUInt8<IDataType>, isColumnConst, "const UInt8"},
                    {"timezone", &isStringOrFixedString<IDataType>, isColumnConst, "const String or FixedString"},
                });

            UInt64 scale = to_datetime64 ? DataTypeDateTime64::default_scale : 0;
            if (arguments.size() > 1)
                scale = extractToDecimalScale(arguments[1]);
            const auto timezone = extractTimeZoneNameFromFunctionArguments(arguments, 2, 0);

            res = scale == 0 ? res = std::make_shared<DataTypeDateTime>(timezone) : std::make_shared<DataTypeDateTime64>(scale, timezone);
        }
        else
        {
            if ((arguments.size() != 1 && arguments.size() != 2) || (to_decimal && arguments.size() != 2))
                throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size()) +
                    ", should be 1 or 2. Second argument only make sense for DateTime (time zone, optional) and Decimal (scale).",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            if (!isStringOrFixedString(arguments[0].type))
            {
                if (this->getName().find("OrZero") != std::string::npos ||
                    this->getName().find("OrNull") != std::string::npos)
                    throw Exception("Illegal type " + arguments[0].type->getName() + " of first argument of function " + getName() +
                            ". Conversion functions with postfix 'OrZero' or 'OrNull'  should take String argument",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                else
                    throw Exception("Illegal type " + arguments[0].type->getName() + " of first argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            if (arguments.size() == 2)
            {
                if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
                {
                    if (!isString(arguments[1].type))
                        throw Exception("Illegal type " + arguments[1].type->getName() + " of 2nd argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                }
                else if constexpr (to_decimal)
                {
                    if (!isInteger(arguments[1].type))
                        throw Exception("Illegal type " + arguments[1].type->getName() + " of 2nd argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                    if (!arguments[1].column)
                        throw Exception("Second argument for function " + getName() + " must be constant", ErrorCodes::ILLEGAL_COLUMN);
                }
                else
                {
                    throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                        + toString(arguments.size()) + ", should be 1. Second argument makes sense only for DateTime and Decimal.",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
                }
            }

            if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
                res = std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 1, 0));
            else if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
                throw Exception("LOGICAL ERROR: It is a bug.", ErrorCodes::LOGICAL_ERROR);
            else if constexpr (to_decimal)
            {
                UInt64 scale = extractToDecimalScale(arguments[1]);
                res = createDecimalMaxPrecision<typename ToDataType::FieldType>(scale);
                if (!res)
                    throw Exception("Something wrong with toDecimalNNOrZero() or toDecimalNNOrNull()", ErrorCodes::LOGICAL_ERROR);
            }
            else
                res = std::make_shared<ToDataType>();
        }

        if constexpr (exception_mode == ConvertFromStringExceptionMode::Null)
            res = std::make_shared<DataTypeNullable>(res);

        return res;
    }

    template <typename ConvertToDataType>
    ColumnPtr executeInternal(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, UInt32 scale = 0) const
    {
        const IDataType * from_type = arguments[0].type.get();

        if (checkAndGetDataType<DataTypeString>(from_type))
        {
            return ConvertThroughParsing<DataTypeString, ConvertToDataType, Name, exception_mode, parsing_mode>::execute(
                arguments, result_type, input_rows_count, scale);
        }
        else if (checkAndGetDataType<DataTypeFixedString>(from_type))
        {
            return ConvertThroughParsing<DataTypeFixedString, ConvertToDataType, Name, exception_mode, parsing_mode>::execute(
                arguments, result_type, input_rows_count, scale);
        }

        return nullptr;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnPtr result_column;

        if constexpr (to_decimal)
            result_column = executeInternal<ToDataType>(arguments, result_type, input_rows_count,
                assert_cast<const ToDataType &>(*removeNullable(result_type)).getScale());
        else
        {
            if (isDateTime64<Name, ToDataType>(arguments))
            {
                UInt64 scale = to_datetime64 ? DataTypeDateTime64::default_scale : 0;
                if (arguments.size() > 1)
                    scale = extractToDecimalScale(arguments[1]);

                if (scale == 0)
                    result_column = executeInternal<DataTypeDateTime>(arguments, result_type, input_rows_count);
                else
                {
                    result_column = executeInternal<DataTypeDateTime64>(arguments, result_type, input_rows_count, static_cast<UInt32>(scale));
                }
            }
            else
            {
                result_column = executeInternal<ToDataType>(arguments, result_type, input_rows_count);
            }
        }

        if (!result_column)
            throw Exception("Illegal type " + arguments[0].type->getName() + " of argument of function " + getName()
                + ". Only String or FixedString argument is accepted for try-conversion function."
                + " For other arguments, use function without 'orZero' or 'orNull'.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return result_column;
    }
};


/// Monotonicity.

struct PositiveMonotonicity
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const IDataType &, const Field &, const Field &)
    {
        return { .is_monotonic = true };
    }
};

struct UnknownMonotonicity
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const IDataType &, const Field &, const Field &)
    {
        return { };
    }
};

template <typename T>
struct ToNumberMonotonicity
{
    static bool has() { return true; }

    static UInt64 divideByRangeOfType(UInt64 x)
    {
        if constexpr (sizeof(T) < sizeof(UInt64))
            return x >> (sizeof(T) * 8);
        else
            return 0;
    }

    static IFunction::Monotonicity get(const IDataType & type, const Field & left, const Field & right)
    {
        if (!type.isValueRepresentedByNumber())
            return {};

        /// If type is same, the conversion is always monotonic.
        /// (Enum has separate case, because it is different data type)
        if (checkAndGetDataType<DataTypeNumber<T>>(&type) ||
            checkAndGetDataType<DataTypeEnum<T>>(&type))
            return { .is_monotonic = true, .is_always_monotonic = true };

        /// Float cases.

        /// When converting to Float, the conversion is always monotonic.
        if constexpr (std::is_floating_point_v<T>)
            return { .is_monotonic = true, .is_always_monotonic = true };

        /// If converting from Float, for monotonicity, arguments must fit in range of result type.
        if (WhichDataType(type).isFloat())
        {
            if (left.isNull() || right.isNull())
                return {};

            Float64 left_float = left.get<Float64>();
            Float64 right_float = right.get<Float64>();

            if (left_float >= static_cast<Float64>(std::numeric_limits<T>::min())
                && left_float <= static_cast<Float64>(std::numeric_limits<T>::max())
                && right_float >= static_cast<Float64>(std::numeric_limits<T>::min())
                && right_float <= static_cast<Float64>(std::numeric_limits<T>::max()))
                return { .is_monotonic = true };

            return {};
        }

        /// Integer cases.

        const bool from_is_unsigned = type.isValueRepresentedByUnsignedInteger();
        const bool to_is_unsigned = is_unsigned_v<T>;

        const size_t size_of_from = type.getSizeOfValueInMemory();
        const size_t size_of_to = sizeof(T);

        const bool left_in_first_half = left.isNull()
            ? from_is_unsigned
            : (left.get<Int64>() >= 0);

        const bool right_in_first_half = right.isNull()
            ? !from_is_unsigned
            : (right.get<Int64>() >= 0);

        /// Size of type is the same.
        if (size_of_from == size_of_to)
        {
            if (from_is_unsigned == to_is_unsigned)
                return { .is_monotonic = true, .is_always_monotonic = true };

            if (left_in_first_half == right_in_first_half)
                return { .is_monotonic = true };

            return {};
        }

        /// Size of type is expanded.
        if (size_of_from < size_of_to)
        {
            if (from_is_unsigned == to_is_unsigned)
                return { .is_monotonic = true, .is_always_monotonic = true };

            if (!to_is_unsigned)
                return { .is_monotonic = true, .is_always_monotonic = true };

            /// signed -> unsigned. If arguments from the same half, then function is monotonic.
            if (left_in_first_half == right_in_first_half)
                return { .is_monotonic = true };

            return {};
        }

        /// Size of type is shrunk.
        if (size_of_from > size_of_to)
        {
            /// Function cannot be monotonic on unbounded ranges.
            if (left.isNull() || right.isNull())
                return {};

            /// Function cannot be monotonic when left and right are not on the same ranges.
            if (divideByRangeOfType(left.get<UInt64>()) != divideByRangeOfType(right.get<UInt64>()))
                return {};

            if (to_is_unsigned)
                return { .is_monotonic = true };
            else
            {
                // If To is signed, it's possible that the signedness is different after conversion. So we check it explicitly.
                const bool is_monotonic = (T(left.get<UInt64>()) >= 0) == (T(right.get<UInt64>()) >= 0);

                return { .is_monotonic = is_monotonic };
            }
        }

        __builtin_unreachable();
    }
};

struct ToDateMonotonicity
{
    static bool has() { return true; }

    static IFunction::Monotonicity get(const IDataType & type, const Field & left, const Field & right)
    {
        auto which = WhichDataType(type);
        if (which.isDateOrDate32() || which.isDateTime() || which.isDateTime64() || which.isInt8() || which.isInt16() || which.isUInt8() || which.isUInt16())
            return { .is_monotonic = true, .is_always_monotonic = true };
        else if (
            (which.isUInt() && ((left.isNull() || left.get<UInt64>() < 0xFFFF) && (right.isNull() || right.get<UInt64>() >= 0xFFFF)))
            || (which.isInt() && ((left.isNull() || left.get<Int64>() < 0xFFFF) && (right.isNull() || right.get<Int64>() >= 0xFFFF)))
            || (which.isFloat() && ((left.isNull() || left.get<Float64>() < 0xFFFF) && (right.isNull() || right.get<Float64>() >= 0xFFFF)))
            || !type.isValueRepresentedByNumber())
            return {};
        else
            return { .is_monotonic = true, .is_always_monotonic = true };
    }
};

struct ToDateTimeMonotonicity
{
    static bool has() { return true; }

    static IFunction::Monotonicity get(const IDataType & type, const Field &, const Field &)
    {
        if (type.isValueRepresentedByNumber())
            return { .is_monotonic = true, .is_always_monotonic = true };
        else
            return {};
    }
};

/** The monotonicity for the `toString` function is mainly determined for test purposes.
  * It is doubtful that anyone is looking to optimize queries with conditions `toString(CounterID) = 34`.
  */
struct ToStringMonotonicity
{
    static bool has() { return true; }

    static IFunction::Monotonicity get(const IDataType & type, const Field & left, const Field & right)
    {
        IFunction::Monotonicity positive{ .is_monotonic = true };
        IFunction::Monotonicity not_monotonic;

        const auto * type_ptr = &type;
        if (const auto * low_cardinality_type = checkAndGetDataType<DataTypeLowCardinality>(type_ptr))
            type_ptr = low_cardinality_type->getDictionaryType().get();

        /// `toString` function is monotonous if the argument is Date or Date32 or DateTime or String, or non-negative numbers with the same number of symbols.
        if (checkDataTypes<DataTypeDate, DataTypeDate32, DataTypeDateTime, DataTypeString>(type_ptr))
            return positive;

        if (left.isNull() || right.isNull())
            return {};

        if (left.getType() == Field::Types::UInt64
            && right.getType() == Field::Types::UInt64)
        {
            return (left.get<Int64>() == 0 && right.get<Int64>() == 0)
                || (floor(log10(left.get<UInt64>())) == floor(log10(right.get<UInt64>())))
                ? positive : not_monotonic;
        }

        if (left.getType() == Field::Types::Int64
            && right.getType() == Field::Types::Int64)
        {
            return (left.get<Int64>() == 0 && right.get<Int64>() == 0)
                || (left.get<Int64>() > 0 && right.get<Int64>() > 0 && floor(log10(left.get<Int64>())) == floor(log10(right.get<Int64>())))
                ? positive : not_monotonic;
        }

        return not_monotonic;
    }
};


struct NameToUInt8 { static constexpr auto name = "toUInt8"; };
struct NameToUInt16 { static constexpr auto name = "toUInt16"; };
struct NameToUInt32 { static constexpr auto name = "toUInt32"; };
struct NameToUInt64 { static constexpr auto name = "toUInt64"; };
struct NameToUInt128 { static constexpr auto name = "toUInt128"; };
struct NameToUInt256 { static constexpr auto name = "toUInt256"; };
struct NameToInt8 { static constexpr auto name = "toInt8"; };
struct NameToInt16 { static constexpr auto name = "toInt16"; };
struct NameToInt32 { static constexpr auto name = "toInt32"; };
struct NameToInt64 { static constexpr auto name = "toInt64"; };
struct NameToInt128 { static constexpr auto name = "toInt128"; };
struct NameToInt256 { static constexpr auto name = "toInt256"; };
struct NameToFloat32 { static constexpr auto name = "toFloat32"; };
struct NameToFloat64 { static constexpr auto name = "toFloat64"; };
struct NameToUUID { static constexpr auto name = "toUUID"; };

using FunctionToUInt8 = FunctionConvert<DataTypeUInt8, NameToUInt8, ToNumberMonotonicity<UInt8>>;
using FunctionToUInt16 = FunctionConvert<DataTypeUInt16, NameToUInt16, ToNumberMonotonicity<UInt16>>;
using FunctionToUInt32 = FunctionConvert<DataTypeUInt32, NameToUInt32, ToNumberMonotonicity<UInt32>>;
using FunctionToUInt64 = FunctionConvert<DataTypeUInt64, NameToUInt64, ToNumberMonotonicity<UInt64>>;
using FunctionToUInt128 = FunctionConvert<DataTypeUInt128, NameToUInt128, ToNumberMonotonicity<UInt128>>;
using FunctionToUInt256 = FunctionConvert<DataTypeUInt256, NameToUInt256, ToNumberMonotonicity<UInt256>>;
using FunctionToInt8 = FunctionConvert<DataTypeInt8, NameToInt8, ToNumberMonotonicity<Int8>>;
using FunctionToInt16 = FunctionConvert<DataTypeInt16, NameToInt16, ToNumberMonotonicity<Int16>>;
using FunctionToInt32 = FunctionConvert<DataTypeInt32, NameToInt32, ToNumberMonotonicity<Int32>>;
using FunctionToInt64 = FunctionConvert<DataTypeInt64, NameToInt64, ToNumberMonotonicity<Int64>>;
using FunctionToInt128 = FunctionConvert<DataTypeInt128, NameToInt128, ToNumberMonotonicity<Int128>>;
using FunctionToInt256 = FunctionConvert<DataTypeInt256, NameToInt256, ToNumberMonotonicity<Int256>>;
using FunctionToFloat32 = FunctionConvert<DataTypeFloat32, NameToFloat32, ToNumberMonotonicity<Float32>>;
using FunctionToFloat64 = FunctionConvert<DataTypeFloat64, NameToFloat64, ToNumberMonotonicity<Float64>>;
using FunctionToDate = FunctionConvert<DataTypeDate, NameToDate, ToDateMonotonicity>;
using FunctionToDate32 = FunctionConvert<DataTypeDate32, NameToDate32, ToDateMonotonicity>;
using FunctionToDateTime = FunctionConvert<DataTypeDateTime, NameToDateTime, ToDateTimeMonotonicity>;
using FunctionToDateTime32 = FunctionConvert<DataTypeDateTime, NameToDateTime32, ToDateTimeMonotonicity>;
using FunctionToDateTime64 = FunctionConvert<DataTypeDateTime64, NameToDateTime64, UnknownMonotonicity>;
using FunctionToUUID = FunctionConvert<DataTypeUUID, NameToUUID, ToNumberMonotonicity<UInt128>>;
using FunctionToString = FunctionConvert<DataTypeString, NameToString, ToStringMonotonicity>;
using FunctionToUnixTimestamp = FunctionConvert<DataTypeUInt32, NameToUnixTimestamp, ToNumberMonotonicity<UInt32>>;
using FunctionToDecimal32 = FunctionConvert<DataTypeDecimal<Decimal32>, NameToDecimal32, UnknownMonotonicity>;
using FunctionToDecimal64 = FunctionConvert<DataTypeDecimal<Decimal64>, NameToDecimal64, UnknownMonotonicity>;
using FunctionToDecimal128 = FunctionConvert<DataTypeDecimal<Decimal128>, NameToDecimal128, UnknownMonotonicity>;
using FunctionToDecimal256 = FunctionConvert<DataTypeDecimal<Decimal256>, NameToDecimal256, UnknownMonotonicity>;


template <typename DataType> struct FunctionTo;

template <> struct FunctionTo<DataTypeUInt8> { using Type = FunctionToUInt8; };
template <> struct FunctionTo<DataTypeUInt16> { using Type = FunctionToUInt16; };
template <> struct FunctionTo<DataTypeUInt32> { using Type = FunctionToUInt32; };
template <> struct FunctionTo<DataTypeUInt64> { using Type = FunctionToUInt64; };
template <> struct FunctionTo<DataTypeUInt128> { using Type = FunctionToUInt128; };
template <> struct FunctionTo<DataTypeUInt256> { using Type = FunctionToUInt256; };
template <> struct FunctionTo<DataTypeInt8> { using Type = FunctionToInt8; };
template <> struct FunctionTo<DataTypeInt16> { using Type = FunctionToInt16; };
template <> struct FunctionTo<DataTypeInt32> { using Type = FunctionToInt32; };
template <> struct FunctionTo<DataTypeInt64> { using Type = FunctionToInt64; };
template <> struct FunctionTo<DataTypeInt128> { using Type = FunctionToInt128; };
template <> struct FunctionTo<DataTypeInt256> { using Type = FunctionToInt256; };
template <> struct FunctionTo<DataTypeFloat32> { using Type = FunctionToFloat32; };
template <> struct FunctionTo<DataTypeFloat64> { using Type = FunctionToFloat64; };
template <> struct FunctionTo<DataTypeDate> { using Type = FunctionToDate; };
template <> struct FunctionTo<DataTypeDate32> { using Type = FunctionToDate32; };
template <> struct FunctionTo<DataTypeDateTime> { using Type = FunctionToDateTime; };
template <> struct FunctionTo<DataTypeDateTime64> { using Type = FunctionToDateTime64; };
template <> struct FunctionTo<DataTypeUUID> { using Type = FunctionToUUID; };
template <> struct FunctionTo<DataTypeString> { using Type = FunctionToString; };
template <> struct FunctionTo<DataTypeFixedString> { using Type = FunctionToFixedString; };
template <> struct FunctionTo<DataTypeDecimal<Decimal32>> { using Type = FunctionToDecimal32; };
template <> struct FunctionTo<DataTypeDecimal<Decimal64>> { using Type = FunctionToDecimal64; };
template <> struct FunctionTo<DataTypeDecimal<Decimal128>> { using Type = FunctionToDecimal128; };
template <> struct FunctionTo<DataTypeDecimal<Decimal256>> { using Type = FunctionToDecimal256; };

template <typename FieldType> struct FunctionTo<DataTypeEnum<FieldType>>
    : FunctionTo<DataTypeNumber<FieldType>>
{
};

struct NameToUInt8OrZero { static constexpr auto name = "toUInt8OrZero"; };
struct NameToUInt16OrZero { static constexpr auto name = "toUInt16OrZero"; };
struct NameToUInt32OrZero { static constexpr auto name = "toUInt32OrZero"; };
struct NameToUInt64OrZero { static constexpr auto name = "toUInt64OrZero"; };
struct NameToUInt128OrZero { static constexpr auto name = "toUInt128OrZero"; };
struct NameToUInt256OrZero { static constexpr auto name = "toUInt256OrZero"; };
struct NameToInt8OrZero { static constexpr auto name = "toInt8OrZero"; };
struct NameToInt16OrZero { static constexpr auto name = "toInt16OrZero"; };
struct NameToInt32OrZero { static constexpr auto name = "toInt32OrZero"; };
struct NameToInt64OrZero { static constexpr auto name = "toInt64OrZero"; };
struct NameToInt128OrZero { static constexpr auto name = "toInt128OrZero"; };
struct NameToInt256OrZero { static constexpr auto name = "toInt256OrZero"; };
struct NameToFloat32OrZero { static constexpr auto name = "toFloat32OrZero"; };
struct NameToFloat64OrZero { static constexpr auto name = "toFloat64OrZero"; };
struct NameToDateOrZero { static constexpr auto name = "toDateOrZero"; };
struct NameToDate32OrZero { static constexpr auto name = "toDate32OrZero"; };
struct NameToDateTimeOrZero { static constexpr auto name = "toDateTimeOrZero"; };
struct NameToDateTime64OrZero { static constexpr auto name = "toDateTime64OrZero"; };
struct NameToDecimal32OrZero { static constexpr auto name = "toDecimal32OrZero"; };
struct NameToDecimal64OrZero { static constexpr auto name = "toDecimal64OrZero"; };
struct NameToDecimal128OrZero { static constexpr auto name = "toDecimal128OrZero"; };
struct NameToDecimal256OrZero { static constexpr auto name = "toDecimal256OrZero"; };
struct NameToUUIDOrZero { static constexpr auto name = "toUUIDOrZero"; };

using FunctionToUInt8OrZero = FunctionConvertFromString<DataTypeUInt8, NameToUInt8OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUInt16OrZero = FunctionConvertFromString<DataTypeUInt16, NameToUInt16OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUInt32OrZero = FunctionConvertFromString<DataTypeUInt32, NameToUInt32OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUInt64OrZero = FunctionConvertFromString<DataTypeUInt64, NameToUInt64OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUInt128OrZero = FunctionConvertFromString<DataTypeUInt128, NameToUInt128OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUInt256OrZero = FunctionConvertFromString<DataTypeUInt256, NameToUInt256OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToInt8OrZero = FunctionConvertFromString<DataTypeInt8, NameToInt8OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToInt16OrZero = FunctionConvertFromString<DataTypeInt16, NameToInt16OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToInt32OrZero = FunctionConvertFromString<DataTypeInt32, NameToInt32OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToInt64OrZero = FunctionConvertFromString<DataTypeInt64, NameToInt64OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToInt128OrZero = FunctionConvertFromString<DataTypeInt128, NameToInt128OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToInt256OrZero = FunctionConvertFromString<DataTypeInt256, NameToInt256OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToFloat32OrZero = FunctionConvertFromString<DataTypeFloat32, NameToFloat32OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToFloat64OrZero = FunctionConvertFromString<DataTypeFloat64, NameToFloat64OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDateOrZero = FunctionConvertFromString<DataTypeDate, NameToDateOrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDate32OrZero = FunctionConvertFromString<DataTypeDate32, NameToDate32OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDateTimeOrZero = FunctionConvertFromString<DataTypeDateTime, NameToDateTimeOrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDateTime64OrZero = FunctionConvertFromString<DataTypeDateTime64, NameToDateTime64OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDecimal32OrZero = FunctionConvertFromString<DataTypeDecimal<Decimal32>, NameToDecimal32OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDecimal64OrZero = FunctionConvertFromString<DataTypeDecimal<Decimal64>, NameToDecimal64OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDecimal128OrZero = FunctionConvertFromString<DataTypeDecimal<Decimal128>, NameToDecimal128OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToDecimal256OrZero = FunctionConvertFromString<DataTypeDecimal<Decimal256>, NameToDecimal256OrZero, ConvertFromStringExceptionMode::Zero>;
using FunctionToUUIDOrZero = FunctionConvertFromString<DataTypeUUID, NameToUUIDOrZero, ConvertFromStringExceptionMode::Zero>;

struct NameToUInt8OrNull { static constexpr auto name = "toUInt8OrNull"; };
struct NameToUInt16OrNull { static constexpr auto name = "toUInt16OrNull"; };
struct NameToUInt32OrNull { static constexpr auto name = "toUInt32OrNull"; };
struct NameToUInt64OrNull { static constexpr auto name = "toUInt64OrNull"; };
struct NameToUInt128OrNull { static constexpr auto name = "toUInt128OrNull"; };
struct NameToUInt256OrNull { static constexpr auto name = "toUInt256OrNull"; };
struct NameToInt8OrNull { static constexpr auto name = "toInt8OrNull"; };
struct NameToInt16OrNull { static constexpr auto name = "toInt16OrNull"; };
struct NameToInt32OrNull { static constexpr auto name = "toInt32OrNull"; };
struct NameToInt64OrNull { static constexpr auto name = "toInt64OrNull"; };
struct NameToInt128OrNull { static constexpr auto name = "toInt128OrNull"; };
struct NameToInt256OrNull { static constexpr auto name = "toInt256OrNull"; };
struct NameToFloat32OrNull { static constexpr auto name = "toFloat32OrNull"; };
struct NameToFloat64OrNull { static constexpr auto name = "toFloat64OrNull"; };
struct NameToDateOrNull { static constexpr auto name = "toDateOrNull"; };
struct NameToDate32OrNull { static constexpr auto name = "toDate32OrNull"; };
struct NameToDateTimeOrNull { static constexpr auto name = "toDateTimeOrNull"; };
struct NameToDateTime64OrNull { static constexpr auto name = "toDateTime64OrNull"; };
struct NameToDecimal32OrNull { static constexpr auto name = "toDecimal32OrNull"; };
struct NameToDecimal64OrNull { static constexpr auto name = "toDecimal64OrNull"; };
struct NameToDecimal128OrNull { static constexpr auto name = "toDecimal128OrNull"; };
struct NameToDecimal256OrNull { static constexpr auto name = "toDecimal256OrNull"; };
struct NameToUUIDOrNull { static constexpr auto name = "toUUIDOrNull"; };

using FunctionToUInt8OrNull = FunctionConvertFromString<DataTypeUInt8, NameToUInt8OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUInt16OrNull = FunctionConvertFromString<DataTypeUInt16, NameToUInt16OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUInt32OrNull = FunctionConvertFromString<DataTypeUInt32, NameToUInt32OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUInt64OrNull = FunctionConvertFromString<DataTypeUInt64, NameToUInt64OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUInt128OrNull = FunctionConvertFromString<DataTypeUInt128, NameToUInt128OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUInt256OrNull = FunctionConvertFromString<DataTypeUInt256, NameToUInt256OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToInt8OrNull = FunctionConvertFromString<DataTypeInt8, NameToInt8OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToInt16OrNull = FunctionConvertFromString<DataTypeInt16, NameToInt16OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToInt32OrNull = FunctionConvertFromString<DataTypeInt32, NameToInt32OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToInt64OrNull = FunctionConvertFromString<DataTypeInt64, NameToInt64OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToInt128OrNull = FunctionConvertFromString<DataTypeInt128, NameToInt128OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToInt256OrNull = FunctionConvertFromString<DataTypeInt256, NameToInt256OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToFloat32OrNull = FunctionConvertFromString<DataTypeFloat32, NameToFloat32OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToFloat64OrNull = FunctionConvertFromString<DataTypeFloat64, NameToFloat64OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDateOrNull = FunctionConvertFromString<DataTypeDate, NameToDateOrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDate32OrNull = FunctionConvertFromString<DataTypeDate32, NameToDate32OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDateTimeOrNull = FunctionConvertFromString<DataTypeDateTime, NameToDateTimeOrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDateTime64OrNull = FunctionConvertFromString<DataTypeDateTime64, NameToDateTime64OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDecimal32OrNull = FunctionConvertFromString<DataTypeDecimal<Decimal32>, NameToDecimal32OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDecimal64OrNull = FunctionConvertFromString<DataTypeDecimal<Decimal64>, NameToDecimal64OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDecimal128OrNull = FunctionConvertFromString<DataTypeDecimal<Decimal128>, NameToDecimal128OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToDecimal256OrNull = FunctionConvertFromString<DataTypeDecimal<Decimal256>, NameToDecimal256OrNull, ConvertFromStringExceptionMode::Null>;
using FunctionToUUIDOrNull = FunctionConvertFromString<DataTypeUUID, NameToUUIDOrNull, ConvertFromStringExceptionMode::Null>;

struct NameParseDateTimeBestEffort { static constexpr auto name = "parseDateTimeBestEffort"; };
struct NameParseDateTimeBestEffortOrZero { static constexpr auto name = "parseDateTimeBestEffortOrZero"; };
struct NameParseDateTimeBestEffortOrNull { static constexpr auto name = "parseDateTimeBestEffortOrNull"; };
struct NameParseDateTimeBestEffortUS { static constexpr auto name = "parseDateTimeBestEffortUS"; };
struct NameParseDateTimeBestEffortUSOrZero { static constexpr auto name = "parseDateTimeBestEffortUSOrZero"; };
struct NameParseDateTimeBestEffortUSOrNull { static constexpr auto name = "parseDateTimeBestEffortUSOrNull"; };
struct NameParseDateTime32BestEffort { static constexpr auto name = "parseDateTime32BestEffort"; };
struct NameParseDateTime32BestEffortOrZero { static constexpr auto name = "parseDateTime32BestEffortOrZero"; };
struct NameParseDateTime32BestEffortOrNull { static constexpr auto name = "parseDateTime32BestEffortOrNull"; };
struct NameParseDateTime64BestEffort { static constexpr auto name = "parseDateTime64BestEffort"; };
struct NameParseDateTime64BestEffortOrZero { static constexpr auto name = "parseDateTime64BestEffortOrZero"; };
struct NameParseDateTime64BestEffortOrNull { static constexpr auto name = "parseDateTime64BestEffortOrNull"; };


using FunctionParseDateTimeBestEffort = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffort, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffort>;
using FunctionParseDateTimeBestEffortOrZero = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffort>;
using FunctionParseDateTimeBestEffortOrNull = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffort>;

using FunctionParseDateTimeBestEffortUS = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortUS, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffortUS>;
using FunctionParseDateTimeBestEffortUSOrZero = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortUSOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffortUS>;
using FunctionParseDateTimeBestEffortUSOrNull = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTimeBestEffortUSOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffortUS>;

using FunctionParseDateTime32BestEffort = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTime32BestEffort, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffort>;
using FunctionParseDateTime32BestEffortOrZero = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTime32BestEffortOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffort>;
using FunctionParseDateTime32BestEffortOrNull = FunctionConvertFromString<
    DataTypeDateTime, NameParseDateTime32BestEffortOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffort>;

using FunctionParseDateTime64BestEffort = FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffort, ConvertFromStringExceptionMode::Throw, ConvertFromStringParsingMode::BestEffort>;
using FunctionParseDateTime64BestEffortOrZero = FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffortOrZero, ConvertFromStringExceptionMode::Zero, ConvertFromStringParsingMode::BestEffort>;
using FunctionParseDateTime64BestEffortOrNull = FunctionConvertFromString<
    DataTypeDateTime64, NameParseDateTime64BestEffortOrNull, ConvertFromStringExceptionMode::Null, ConvertFromStringParsingMode::BestEffort>;

class ExecutableFunctionCast : public IExecutableFunction
{
public:
    using WrapperType = std::function<ColumnPtr(ColumnsWithTypeAndName &, const DataTypePtr &, const ColumnNullable *, size_t)>;

    struct Diagnostic
    {
        std::string column_from;
        std::string column_to;
    };

    explicit ExecutableFunctionCast(
            WrapperType && wrapper_function_, const char * name_, std::optional<Diagnostic> diagnostic_)
            : wrapper_function(std::move(wrapper_function_)), name(name_), diagnostic(std::move(diagnostic_)) {}

    String getName() const override { return name; }

protected:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// drop second argument, pass others
        ColumnsWithTypeAndName new_arguments{arguments.front()};
        if (arguments.size() > 2)
            new_arguments.insert(std::end(new_arguments), std::next(std::begin(arguments), 2), std::end(arguments));

        try
        {
            return wrapper_function(new_arguments, result_type, nullptr, input_rows_count);
        }
        catch (Exception & e)
        {
            if (diagnostic)
                e.addMessage("while converting source column " + backQuoteIfNeed(diagnostic->column_from) +
                             " to destination column " + backQuoteIfNeed(diagnostic->column_to));
            throw;
        }
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    /// CAST(Nothing, T) -> T
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

private:
    WrapperType wrapper_function;
    const char * name;
    std::optional<Diagnostic> diagnostic;
};

struct CastName { static constexpr auto name = "CAST"; };
struct CastInternalName { static constexpr auto name = "_CAST"; };

enum class CastType
{
    nonAccurate,
    accurate,
    accurateOrNull
};

class FunctionCastBase : public IFunctionBase
{
public:
    using MonotonicityForRange = std::function<Monotonicity(const IDataType &, const Field &, const Field &)>;
    using Diagnostic = ExecutableFunctionCast::Diagnostic;
};

template <typename FunctionName>
class FunctionCast final : public FunctionCastBase
{
public:
    using WrapperType = std::function<ColumnPtr(ColumnsWithTypeAndName &, const DataTypePtr &, const ColumnNullable *, size_t)>;

    FunctionCast(const char * cast_name_
            , MonotonicityForRange && monotonicity_for_range_
            , const DataTypes & argument_types_
            , const DataTypePtr & return_type_
            , std::optional<Diagnostic> diagnostic_
            , CastType cast_type_
            , bool cast_ipv4_ipv6_default_on_conversion_error_)
        : cast_name(cast_name_), monotonicity_for_range(std::move(monotonicity_for_range_))
        , argument_types(argument_types_), return_type(return_type_), diagnostic(std::move(diagnostic_))
        , cast_type(cast_type_)
        , cast_ipv4_ipv6_default_on_conversion_error(cast_ipv4_ipv6_default_on_conversion_error_)
    {
    }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & /*sample_columns*/) const override
    {
        try
        {
            return std::make_unique<ExecutableFunctionCast>(
                    prepareUnpackDictionaries(getArgumentTypes()[0], getResultType()), cast_name, diagnostic);
        }
        catch (Exception & e)
        {
            if (diagnostic)
                e.addMessage("while converting source column " + backQuoteIfNeed(diagnostic->column_from) +
                             " to destination column " + backQuoteIfNeed(diagnostic->column_to));
            throw;
        }
    }

    String getName() const override { return cast_name; }

    bool isDeterministic() const override { return true; }
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool hasInformationAboutMonotonicity() const override
    {
        return static_cast<bool>(monotonicity_for_range);
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return monotonicity_for_range(type, left, right);
    }

private:

    const char * cast_name;
    MonotonicityForRange monotonicity_for_range;

    DataTypes argument_types;
    DataTypePtr return_type;

    std::optional<Diagnostic> diagnostic;
    CastType cast_type;
    bool cast_ipv4_ipv6_default_on_conversion_error;

    static WrapperType createFunctionAdaptor(FunctionPtr function, const DataTypePtr & from_type)
    {
        auto function_adaptor = std::make_unique<FunctionToOverloadResolverAdaptor>(function)->build({ColumnWithTypeAndName{nullptr, from_type, ""}});

        return [function_adaptor]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count)
        {
            return function_adaptor->execute(arguments, result_type, input_rows_count);
        };
    }

    static WrapperType createToNullableColumnWrapper()
    {
        return [] (ColumnsWithTypeAndName &, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count)
        {
            ColumnPtr res = result_type->createColumn();
            ColumnUInt8::Ptr col_null_map_to = ColumnUInt8::create(input_rows_count, true);
            return ColumnNullable::create(res->cloneResized(input_rows_count), std::move(col_null_map_to));
        };
    }

    template <typename ToDataType>
    WrapperType createWrapper(const DataTypePtr & from_type, const ToDataType * const to_type, bool requested_result_is_nullable) const
    {
        TypeIndex from_type_index = from_type->getTypeId();
        WhichDataType which(from_type_index);
        bool can_apply_accurate_cast = (cast_type == CastType::accurate || cast_type == CastType::accurateOrNull)
            && (which.isInt() || which.isUInt() || which.isFloat());

        if (requested_result_is_nullable && checkAndGetDataType<DataTypeString>(from_type.get()))
        {
            /// In case when converting to Nullable type, we apply different parsing rule,
            /// that will not throw an exception but return NULL in case of malformed input.

            FunctionPtr function = FunctionConvertFromString<ToDataType, FunctionName, ConvertFromStringExceptionMode::Null>::create();
            return createFunctionAdaptor(function, from_type);
        }
        else if (!can_apply_accurate_cast)
        {
            FunctionPtr function = FunctionTo<ToDataType>::Type::create();
            return createFunctionAdaptor(function, from_type);
        }

        auto wrapper_cast_type = cast_type;

        return [wrapper_cast_type, from_type_index, to_type]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *column_nullable, size_t input_rows_count)
        {
            ColumnPtr result_column;
            auto res = callOnIndexAndDataType<ToDataType>(from_type_index, [&](const auto & types) -> bool {
                using Types = std::decay_t<decltype(types)>;
                using LeftDataType = typename Types::LeftType;
                using RightDataType = typename Types::RightType;

                if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeNumber<RightDataType>)
                {
                    if (wrapper_cast_type == CastType::accurate)
                    {
                        result_column = ConvertImpl<LeftDataType, RightDataType, FunctionName>::execute(
                            arguments, result_type, input_rows_count, AccurateConvertStrategyAdditions());
                    }
                    else
                    {
                        result_column = ConvertImpl<LeftDataType, RightDataType, FunctionName>::execute(
                            arguments, result_type, input_rows_count, AccurateOrNullConvertStrategyAdditions());
                    }

                    return true;
                }

                return false;
            });

            /// Additionally check if callOnIndexAndDataType wasn't called at all.
            if (!res)
            {
                if (wrapper_cast_type == CastType::accurateOrNull)
                {
                    auto nullable_column_wrapper = FunctionCast<FunctionName>::createToNullableColumnWrapper();
                    return nullable_column_wrapper(arguments, result_type, column_nullable, input_rows_count);
                }
                else
                {
                    throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                        "Conversion from {} to {} is not supported",
                        from_type_index, to_type->getName());
                }
            }

            return result_column;
        };
    }

    template <typename ToDataType>
    WrapperType createBoolWrapper(const DataTypePtr & from_type, const ToDataType * const to_type, bool requested_result_is_nullable) const
    {
        if (checkAndGetDataType<DataTypeString>(from_type.get()))
        {
            return &ConvertImplGenericFromString<ColumnString>::execute;
        }

        return createWrapper<ToDataType>(from_type, to_type, requested_result_is_nullable);
    }

    WrapperType createUInt8ToBoolWrapper(const DataTypePtr from_type, const DataTypePtr to_type) const
    {
        return [from_type, to_type] (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t /*input_rows_count*/) -> ColumnPtr
        {
            /// Special case when we convert UInt8 column to Bool column.
            /// both columns have type UInt8, but we shouldn't use identity wrapper,
            /// because Bool column can contain only 0 and 1.
            auto res_column = to_type->createColumn();
            const auto & data_from = checkAndGetColumn<ColumnUInt8>(arguments[0].column.get())->getData();
            auto & data_to = assert_cast<ColumnUInt8 *>(res_column.get())->getData();
            data_to.resize(data_from.size());
            for (size_t i = 0; i != data_from.size(); ++i)
                data_to[i] = static_cast<bool>(data_from[i]);
            return res_column;
        };
    }

    static WrapperType createStringWrapper(const DataTypePtr & from_type)
    {
        FunctionPtr function = FunctionToString::create();
        return createFunctionAdaptor(function, from_type);
    }

    WrapperType createFixedStringWrapper(const DataTypePtr & from_type, const size_t N) const
    {
        if (!isStringOrFixedString(from_type))
            throw Exception{"CAST AS FixedString is only implemented for types String and FixedString", ErrorCodes::NOT_IMPLEMENTED};

        bool exception_mode_null = cast_type == CastType::accurateOrNull;
        return [exception_mode_null, N] (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t /*input_rows_count*/)
        {
            if (exception_mode_null)
                return FunctionToFixedString::executeForN<ConvertToFixedStringExceptionMode::Null>(arguments, N);
            else
                return FunctionToFixedString::executeForN<ConvertToFixedStringExceptionMode::Throw>(arguments, N);
        };
    }

    template <typename ToDataType>
    requires IsDataTypeDecimal<ToDataType>
    WrapperType createDecimalWrapper(const DataTypePtr & from_type, const ToDataType * to_type, bool requested_result_is_nullable) const
    {
        TypeIndex type_index = from_type->getTypeId();
        UInt32 scale = to_type->getScale();

        WhichDataType which(type_index);
        bool ok = which.isNativeInt() || which.isNativeUInt() || which.isDecimal() || which.isFloat() || which.isDateOrDate32() || which.isDateTime() || which.isDateTime64()
            || which.isStringOrFixedString();
        if (!ok)
        {
            if (cast_type == CastType::accurateOrNull)
                return createToNullableColumnWrapper();
            else
                throw Exception{"Conversion from " + from_type->getName() + " to " + to_type->getName() + " is not supported",
                    ErrorCodes::CANNOT_CONVERT_TYPE};
        }

        auto wrapper_cast_type = cast_type;

        return [wrapper_cast_type, type_index, scale, to_type, requested_result_is_nullable]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *column_nullable, size_t input_rows_count)
        {
            ColumnPtr result_column;
            auto res = callOnIndexAndDataType<ToDataType>(type_index, [&](const auto & types) -> bool
            {
                using Types = std::decay_t<decltype(types)>;
                using LeftDataType = typename Types::LeftType;
                using RightDataType = typename Types::RightType;

                if constexpr (IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType> && !std::is_same_v<DataTypeDateTime64, RightDataType>)
                {
                    if (wrapper_cast_type == CastType::accurate)
                    {
                        AccurateConvertStrategyAdditions additions;
                        additions.scale = scale;
                        result_column = ConvertImpl<LeftDataType, RightDataType, FunctionName>::execute(
                            arguments, result_type, input_rows_count, additions);

                        return true;
                    }
                    else if (wrapper_cast_type == CastType::accurateOrNull)
                    {
                        AccurateOrNullConvertStrategyAdditions additions;
                        additions.scale = scale;
                        result_column = ConvertImpl<LeftDataType, RightDataType, FunctionName>::execute(
                            arguments, result_type, input_rows_count, additions);

                        return true;
                    }
                }
                else if constexpr (std::is_same_v<LeftDataType, DataTypeString>)
                {
                    if (requested_result_is_nullable)
                    {
                        /// Consistent with CAST(Nullable(String) AS Nullable(Numbers))
                        /// In case when converting to Nullable type, we apply different parsing rule,
                        /// that will not throw an exception but return NULL in case of malformed input.
                        result_column = ConvertImpl<LeftDataType, RightDataType, FunctionName, ConvertReturnNullOnErrorTag>::execute(
                            arguments, result_type, input_rows_count, scale);

                        return true;
                    }
                }

                result_column = ConvertImpl<LeftDataType, RightDataType, FunctionName>::execute(arguments, result_type, input_rows_count, scale);

                return true;
            });

            /// Additionally check if callOnIndexAndDataType wasn't called at all.
            if (!res)
            {
                if (wrapper_cast_type == CastType::accurateOrNull)
                {
                    auto nullable_column_wrapper = FunctionCast<FunctionName>::createToNullableColumnWrapper();
                    return nullable_column_wrapper(arguments, result_type, column_nullable, input_rows_count);
                }
                else
                    throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                        "Conversion from {} to {} is not supported",
                        type_index, to_type->getName());
            }

            return result_column;
        };
    }

    WrapperType createAggregateFunctionWrapper(const DataTypePtr & from_type_untyped, const DataTypeAggregateFunction * to_type) const
    {
        /// Conversion from String through parsing.
        if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
        {
            return &ConvertImplGenericFromString<ColumnString>::execute;
        }
        else
        {
            if (cast_type == CastType::accurateOrNull)
                return createToNullableColumnWrapper();
            else
                throw Exception{"Conversion from " + from_type_untyped->getName() + " to " + to_type->getName() +
                    " is not supported", ErrorCodes::CANNOT_CONVERT_TYPE};
        }
    }

    WrapperType createArrayWrapper(const DataTypePtr & from_type_untyped, const DataTypeArray & to_type) const
    {
        /// Conversion from String through parsing.
        if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
        {
            return &ConvertImplGenericFromString<ColumnString>::execute;
        }

        const auto * from_type = checkAndGetDataType<DataTypeArray>(from_type_untyped.get());
        const auto * from_type_map = checkAndGetDataType<DataTypeMap>(from_type_untyped.get());

        /// Convert from Map
        if (from_type_map)
            from_type = checkAndGetDataType<DataTypeArray>(from_type_map->getNestedType().get());

        if (!from_type)
        {
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "CAST AS Array can only be performed between same-dimensional Array, Map or String types");
        }

        DataTypePtr from_nested_type = from_type->getNestedType();

        /// In query SELECT CAST([] AS Array(Array(String))) from type is Array(Nothing)
        bool from_empty_array = isNothing(from_nested_type);

        if (from_type->getNumberOfDimensions() != to_type.getNumberOfDimensions() && !from_empty_array)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "CAST AS Array can only be performed between same-dimensional array types");

        const DataTypePtr & to_nested_type = to_type.getNestedType();

        /// Prepare nested type conversion
        const auto nested_function = prepareUnpackDictionaries(from_nested_type, to_nested_type);

        return [nested_function, from_nested_type, to_nested_type](
                ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t /*input_rows_count*/) -> ColumnPtr
        {
            const auto & argument_column = arguments.front();

            const ColumnArray * col_array = nullptr;

            if (const ColumnMap * col_map = checkAndGetColumn<ColumnMap>(argument_column.column.get()))
                col_array = &col_map->getNestedColumn();
            else
                col_array = checkAndGetColumn<ColumnArray>(argument_column.column.get());

            if (col_array)
            {
                /// create columns for converting nested column containing original and result columns
                ColumnsWithTypeAndName nested_columns{{ col_array->getDataPtr(), from_nested_type, "" }};

                /// convert nested column
                auto result_column = nested_function(nested_columns, to_nested_type, nullable_source, nested_columns.front().column->size());

                /// set converted nested column to result
                return ColumnArray::create(result_column, col_array->getOffsetsPtr());
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Illegal column {} for function CAST AS Array",
                    argument_column.column->getName());
            }
        };
    }

    using ElementWrappers = std::vector<WrapperType>;

    ElementWrappers getElementWrappers(const DataTypes & from_element_types, const DataTypes & to_element_types) const
    {
        ElementWrappers element_wrappers;
        element_wrappers.reserve(from_element_types.size());

        /// Create conversion wrapper for each element in tuple
        for (size_t i = 0; i < from_element_types.size(); ++i)
        {
            const DataTypePtr & from_element_type = from_element_types[i];
            const DataTypePtr & to_element_type = to_element_types[i];
            element_wrappers.push_back(prepareUnpackDictionaries(from_element_type, to_element_type));
        }

        return element_wrappers;
    }

    WrapperType createTupleWrapper(const DataTypePtr & from_type_untyped, const DataTypeTuple * to_type) const
    {
        /// Conversion from String through parsing.
        if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
        {
            return &ConvertImplGenericFromString<ColumnString>::execute;
        }

        const auto * from_type = checkAndGetDataType<DataTypeTuple>(from_type_untyped.get());
        if (!from_type)
            throw Exception{"CAST AS Tuple can only be performed between tuple types or from String.\nLeft type: "
                + from_type_untyped->getName() + ", right type: " + to_type->getName(), ErrorCodes::TYPE_MISMATCH};

        const auto & from_element_types = from_type->getElements();
        const auto & to_element_types = to_type->getElements();

        std::vector<WrapperType> element_wrappers;
        std::vector<std::optional<size_t>> to_reverse_index;

        /// For named tuples allow conversions for tuples with
        /// different sets of elements. If element exists in @to_type
        /// and doesn't exist in @to_type it will be filled by default values.
        if (from_type->haveExplicitNames() && to_type->haveExplicitNames())
        {
            const auto & from_names = from_type->getElementNames();
            std::unordered_map<String, size_t> from_positions;
            from_positions.reserve(from_names.size());
            for (size_t i = 0; i < from_names.size(); ++i)
                from_positions[from_names[i]] = i;

            const auto & to_names = to_type->getElementNames();
            element_wrappers.reserve(to_names.size());
            to_reverse_index.reserve(from_names.size());

            for (size_t i = 0; i < to_names.size(); ++i)
            {
                auto it = from_positions.find(to_names[i]);
                if (it != from_positions.end())
                {
                    element_wrappers.emplace_back(prepareUnpackDictionaries(from_element_types[it->second], to_element_types[i]));
                    to_reverse_index.emplace_back(it->second);
                }
                else
                {
                    element_wrappers.emplace_back();
                    to_reverse_index.emplace_back();
                }
            }
        }
        else
        {
            if (from_element_types.size() != to_element_types.size())
                throw Exception{"CAST AS Tuple can only be performed between tuple types with the same number of elements or from String.\n"
                    "Left type: " + from_type->getName() + ", right type: " + to_type->getName(), ErrorCodes::TYPE_MISMATCH};

            element_wrappers = getElementWrappers(from_element_types, to_element_types);
            to_reverse_index.reserve(to_element_types.size());
            for (size_t i = 0; i < to_element_types.size(); ++i)
                to_reverse_index.emplace_back(i);
        }

        return [element_wrappers, from_element_types, to_element_types, to_reverse_index]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t input_rows_count) -> ColumnPtr
        {
            const auto * col = arguments.front().column.get();

            size_t tuple_size = to_element_types.size();
            const ColumnTuple & column_tuple = typeid_cast<const ColumnTuple &>(*col);

            Columns converted_columns(tuple_size);

            /// invoke conversion for each element
            for (size_t i = 0; i < tuple_size; ++i)
            {
                if (to_reverse_index[i])
                {
                    size_t from_idx = *to_reverse_index[i];
                    ColumnsWithTypeAndName element = {{column_tuple.getColumns()[from_idx], from_element_types[from_idx], "" }};
                    converted_columns[i] = element_wrappers[i](element, to_element_types[i], nullable_source, input_rows_count);
                }
                else
                {
                    converted_columns[i] = to_element_types[i]->createColumn()->cloneResized(input_rows_count);
                }
            }

            return ColumnTuple::create(converted_columns);
        };
    }

    /// The case of: tuple([key1, key2, ..., key_n], [value1, value2, ..., value_n])
    WrapperType createTupleToMapWrapper(const DataTypes & from_kv_types, const DataTypes & to_kv_types) const
    {
        return [element_wrappers = getElementWrappers(from_kv_types, to_kv_types), from_kv_types, to_kv_types]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t /*input_rows_count*/) -> ColumnPtr
        {
            const auto * col = arguments.front().column.get();
            const auto & column_tuple = assert_cast<const ColumnTuple &>(*col);

            Columns offsets(2);
            Columns converted_columns(2);
            for (size_t i = 0; i < 2; ++i)
            {
                const auto & column_array = assert_cast<const ColumnArray &>(column_tuple.getColumn(i));
                ColumnsWithTypeAndName element = {{column_array.getDataPtr(), from_kv_types[i], ""}};
                converted_columns[i] = element_wrappers[i](element, to_kv_types[i], nullable_source, (element[0].column)->size());
                offsets[i] = column_array.getOffsetsPtr();
            }

            const auto & keys_offsets = assert_cast<const ColumnArray::ColumnOffsets &>(*offsets[0]).getData();
            const auto & values_offsets = assert_cast<const ColumnArray::ColumnOffsets &>(*offsets[1]).getData();
            if (keys_offsets != values_offsets)
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "CAST AS Map can only be performed from tuple of arrays with equal sizes.");

            return ColumnMap::create(converted_columns[0], converted_columns[1], offsets[0]);
        };
    }

    WrapperType createMapToMapWrrapper(const DataTypes & from_kv_types, const DataTypes & to_kv_types) const
    {
        return [element_wrappers = getElementWrappers(from_kv_types, to_kv_types), from_kv_types, to_kv_types]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t /*input_rows_count*/) -> ColumnPtr
        {
            const auto * col = arguments.front().column.get();
            const auto & column_map = typeid_cast<const ColumnMap &>(*col);
            const auto & nested_data = column_map.getNestedData();

            Columns converted_columns(2);
            for (size_t i = 0; i < 2; ++i)
            {
                ColumnsWithTypeAndName element = {{nested_data.getColumnPtr(i), from_kv_types[i], ""}};
                converted_columns[i] = element_wrappers[i](element, to_kv_types[i], nullable_source, (element[0].column)->size());
            }

            return ColumnMap::create(converted_columns[0], converted_columns[1], column_map.getNestedColumn().getOffsetsPtr());
        };
    }

    /// The case of: [(key1, value1), (key2, value2), ...]
    WrapperType createArrayToMapWrrapper(const DataTypes & from_kv_types, const DataTypes & to_kv_types) const
    {
        return [element_wrappers = getElementWrappers(from_kv_types, to_kv_types), from_kv_types, to_kv_types]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t /*input_rows_count*/) -> ColumnPtr
        {
            const auto * col = arguments.front().column.get();
            const auto & column_array = typeid_cast<const ColumnArray &>(*col);
            const auto & nested_data = typeid_cast<const ColumnTuple &>(column_array.getData());

            Columns converted_columns(2);
            for (size_t i = 0; i < 2; ++i)
            {
                ColumnsWithTypeAndName element = {{nested_data.getColumnPtr(i), from_kv_types[i], ""}};
                converted_columns[i] = element_wrappers[i](element, to_kv_types[i], nullable_source, (element[0].column)->size());
            }

            return ColumnMap::create(converted_columns[0], converted_columns[1], column_array.getOffsetsPtr());
        };
    }


    WrapperType createMapWrapper(const DataTypePtr & from_type_untyped, const DataTypeMap * to_type) const
    {
        if (const auto * from_tuple = checkAndGetDataType<DataTypeTuple>(from_type_untyped.get()))
        {
            if (from_tuple->getElements().size() != 2)
                throw Exception{"CAST AS Map from tuple requeires 2 elements.\n"
                    "Left type: " + from_tuple->getName() + ", right type: " + to_type->getName(), ErrorCodes::TYPE_MISMATCH};

            DataTypes from_kv_types;
            const auto & to_kv_types = to_type->getKeyValueTypes();

            for (const auto & elem : from_tuple->getElements())
            {
                const auto * type_array = checkAndGetDataType<DataTypeArray>(elem.get());
                if (!type_array)
                    throw Exception(ErrorCodes::TYPE_MISMATCH,
                        "CAST AS Map can only be performed from tuples of array. Got: {}", from_tuple->getName());

                from_kv_types.push_back(type_array->getNestedType());
            }

            return createTupleToMapWrapper(from_kv_types, to_kv_types);
        }
        else if (const auto * from_array = typeid_cast<const DataTypeArray *>(from_type_untyped.get()))
        {
            const auto * nested_tuple = typeid_cast<const DataTypeTuple *>(from_array->getNestedType().get());
            if (!nested_tuple || nested_tuple->getElements().size() != 2)
                throw Exception{"CAST AS Map from array requeires nested tuple of 2 elements.\n"
                    "Left type: " + from_array->getName() + ", right type: " + to_type->getName(), ErrorCodes::TYPE_MISMATCH};

            return createArrayToMapWrrapper(nested_tuple->getElements(), to_type->getKeyValueTypes());
        }
        else if (const auto * from_type = checkAndGetDataType<DataTypeMap>(from_type_untyped.get()))
        {
            return createMapToMapWrrapper(from_type->getKeyValueTypes(), to_type->getKeyValueTypes());
        }
        else
        {
            throw Exception{"Unsupported types to CAST AS Map\n"
                "Left type: " + from_type_untyped->getName() + ", right type: " + to_type->getName(), ErrorCodes::TYPE_MISMATCH};
        }
    }

    WrapperType createTupleToObjectWrapper(const DataTypeTuple & from_tuple, bool has_nullable_subcolumns) const
    {
        if (!from_tuple.haveExplicitNames())
            throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Cast to Object can be performed only from flatten Named Tuple. Got: {}", from_tuple.getName());

        PathsInData paths;
        DataTypes from_types;

        std::tie(paths, from_types) = flattenTuple(from_tuple.getPtr());
        auto to_types = from_types;

        for (auto & type : to_types)
        {
            if (isTuple(type) || isNested(type))
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Cast to Object can be performed only from flatten Named Tuple. Got: {}",
                    from_tuple.getName());

            type = recursiveRemoveLowCardinality(type);
        }

        return [element_wrappers = getElementWrappers(from_types, to_types),
            has_nullable_subcolumns, from_types, to_types, paths]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t input_rows_count)
        {
            size_t tuple_size = to_types.size();
            auto flattened_column = flattenTuple(arguments.front().column);
            const auto & column_tuple = assert_cast<const ColumnTuple &>(*flattened_column);

            if (tuple_size != column_tuple.getColumns().size())
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Expected tuple with {} subcolumn, but got {} subcolumns",
                    tuple_size, column_tuple.getColumns().size());

            auto res = ColumnObject::create(has_nullable_subcolumns);
            for (size_t i = 0; i < tuple_size; ++i)
            {
                ColumnsWithTypeAndName element = {{column_tuple.getColumns()[i], from_types[i], "" }};
                auto converted_column = element_wrappers[i](element, to_types[i], nullable_source, input_rows_count);
                res->addSubcolumn(paths[i], converted_column->assumeMutable());
            }

            return res;
        };
    }

    WrapperType createMapToObjectWrapper(const DataTypeMap & from_map, bool has_nullable_subcolumns) const
    {
        auto key_value_types = from_map.getKeyValueTypes();

        if (!isStringOrFixedString(key_value_types[0]))
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "Cast to Object from Map can be performed only from Map "
                "with String or FixedString key. Got: {}", from_map.getName());

        const auto & value_type = key_value_types[1];
        auto to_value_type = value_type;

        if (!has_nullable_subcolumns && value_type->isNullable())
            to_value_type = removeNullable(value_type);

        if (has_nullable_subcolumns && !value_type->isNullable())
            to_value_type = makeNullable(value_type);

        DataTypes to_key_value_types{std::make_shared<DataTypeString>(), std::move(to_value_type)};
        auto element_wrappers = getElementWrappers(key_value_types, to_key_value_types);

        return [has_nullable_subcolumns, element_wrappers, key_value_types, to_key_value_types]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t) -> ColumnPtr
        {
            const auto & column_map = assert_cast<const ColumnMap &>(*arguments.front().column);
            const auto & offsets = column_map.getNestedColumn().getOffsets();
            auto key_value_columns = column_map.getNestedData().getColumnsCopy();

            for (size_t i = 0; i < 2; ++i)
            {
                ColumnsWithTypeAndName element{{key_value_columns[i], key_value_types[i], ""}};
                key_value_columns[i] = element_wrappers[i](element, to_key_value_types[i], nullable_source, key_value_columns[i]->size());
            }

            const auto & key_column_str = assert_cast<const ColumnString &>(*key_value_columns[0]);
            const auto & value_column = *key_value_columns[1];

            using SubcolumnsMap = HashMap<StringRef, MutableColumnPtr, StringRefHash>;
            SubcolumnsMap subcolumns;

            for (size_t row = 0; row < offsets.size(); ++row)
            {
                for (size_t i = offsets[static_cast<ssize_t>(row) - 1]; i < offsets[row]; ++i)
                {
                    auto ref = key_column_str.getDataAt(i);

                    bool inserted;
                    SubcolumnsMap::LookupResult it;
                    subcolumns.emplace(ref, it, inserted);
                    auto & subcolumn = it->getMapped();

                    if (inserted)
                        subcolumn = value_column.cloneEmpty()->cloneResized(row);

                    /// Map can have duplicated keys. We insert only first one.
                    if (subcolumn->size() == row)
                        subcolumn->insertFrom(value_column, i);
                }

                /// Insert default values for keys missed in current row.
                for (const auto & [_, subcolumn] : subcolumns)
                    if (subcolumn->size() == row)
                        subcolumn->insertDefault();
            }

            auto column_object = ColumnObject::create(has_nullable_subcolumns);
            for (auto && [key, subcolumn] : subcolumns)
            {
                PathInData path(key.toView());
                column_object->addSubcolumn(path, std::move(subcolumn));
            }

            return column_object;
        };
    }

    WrapperType createObjectWrapper(const DataTypePtr & from_type, const DataTypeObject * to_type) const
    {
        if (const auto * from_tuple = checkAndGetDataType<DataTypeTuple>(from_type.get()))
        {
            return createTupleToObjectWrapper(*from_tuple, to_type->hasNullableSubcolumns());
        }
        else if (const auto * from_map = checkAndGetDataType<DataTypeMap>(from_type.get()))
        {
            return createMapToObjectWrapper(*from_map, to_type->hasNullableSubcolumns());
        }
        else if (checkAndGetDataType<DataTypeString>(from_type.get()))
        {
            return [] (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * nullable_source, size_t input_rows_count)
            {
                auto res = ConvertImplGenericFromString<ColumnString>::execute(arguments, result_type, nullable_source, input_rows_count);
                auto & res_object = assert_cast<ColumnObject &>(res->assumeMutableRef());
                res_object.finalize();
                return res;
            };
        }
        else if (checkAndGetDataType<DataTypeObject>(from_type.get()))
        {
            return [is_nullable = to_type->hasNullableSubcolumns()] (ColumnsWithTypeAndName & arguments, const DataTypePtr & , const ColumnNullable * , size_t) -> ColumnPtr
            {
                auto & column_object = assert_cast<const ColumnObject &>(*arguments.front().column);
                auto res = ColumnObject::create(is_nullable);
                for (size_t i = 0; i < column_object.size(); i++)
                    res->insert(column_object[i]);

                res->finalize();
                return res;
            };
        }

        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Cast to Object can be performed only from flatten named Tuple, Map or String. Got: {}", from_type->getName());
    }

    template <typename FieldType>
    WrapperType createEnumWrapper(const DataTypePtr & from_type, const DataTypeEnum<FieldType> * to_type) const
    {
        using EnumType = DataTypeEnum<FieldType>;
        using Function = typename FunctionTo<EnumType>::Type;

        if (const auto * from_enum8 = checkAndGetDataType<DataTypeEnum8>(from_type.get()))
            checkEnumToEnumConversion(from_enum8, to_type);
        else if (const auto * from_enum16 = checkAndGetDataType<DataTypeEnum16>(from_type.get()))
            checkEnumToEnumConversion(from_enum16, to_type);

        if (checkAndGetDataType<DataTypeString>(from_type.get()))
            return createStringToEnumWrapper<ColumnString, EnumType>();
        else if (checkAndGetDataType<DataTypeFixedString>(from_type.get()))
            return createStringToEnumWrapper<ColumnFixedString, EnumType>();
        else if (isNativeNumber(from_type) || isEnum(from_type))
        {
            auto function = Function::create();
            return createFunctionAdaptor(function, from_type);
        }
        else
        {
            if (cast_type == CastType::accurateOrNull)
                return createToNullableColumnWrapper();
            else
                throw Exception{"Conversion from " + from_type->getName() + " to " + to_type->getName() + " is not supported",
                    ErrorCodes::CANNOT_CONVERT_TYPE};
        }
    }

    template <typename EnumTypeFrom, typename EnumTypeTo>
    void checkEnumToEnumConversion(const EnumTypeFrom * from_type, const EnumTypeTo * to_type) const
    {
        const auto & from_values = from_type->getValues();
        const auto & to_values = to_type->getValues();

        using ValueType = std::common_type_t<typename EnumTypeFrom::FieldType, typename EnumTypeTo::FieldType>;
        using NameValuePair = std::pair<std::string, ValueType>;
        using EnumValues = std::vector<NameValuePair>;

        EnumValues name_intersection;
        std::set_intersection(std::begin(from_values), std::end(from_values),
            std::begin(to_values), std::end(to_values), std::back_inserter(name_intersection),
            [] (auto && from, auto && to) { return from.first < to.first; });

        for (const auto & name_value : name_intersection)
        {
            const auto & old_value = name_value.second;
            const auto & new_value = to_type->getValue(name_value.first);
            if (old_value != new_value)
                throw Exception{"Enum conversion changes value for element '" + name_value.first +
                    "' from " + toString(old_value) + " to " + toString(new_value), ErrorCodes::CANNOT_CONVERT_TYPE};
        }
    }

    template <typename ColumnStringType, typename EnumType>
    WrapperType createStringToEnumWrapper() const
    {
        const char * function_name = cast_name;
        return [function_name] (
            ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, const ColumnNullable * nullable_col, size_t /*input_rows_count*/)
        {
            const auto & first_col = arguments.front().column.get();
            const auto & result_type = typeid_cast<const EnumType &>(*res_type);

            const ColumnStringType * col = typeid_cast<const ColumnStringType *>(first_col);

            if (col && nullable_col && nullable_col->size() != col->size())
                throw Exception("ColumnNullable is not compatible with original", ErrorCodes::LOGICAL_ERROR);

            if (col)
            {
                const auto size = col->size();

                auto res = result_type.createColumn();
                auto & out_data = static_cast<typename EnumType::ColumnType &>(*res).getData();
                out_data.resize(size);

                auto default_enum_value = result_type.getValues().front().second;

                if (nullable_col)
                {
                    for (size_t i = 0; i < size; ++i)
                    {
                        if (!nullable_col->isNullAt(i))
                            out_data[i] = result_type.getValue(col->getDataAt(i));
                        else
                            out_data[i] = default_enum_value;
                    }
                }
                else
                {
                    for (size_t i = 0; i < size; ++i)
                        out_data[i] = result_type.getValue(col->getDataAt(i));
                }

                return res;
            }
            else
                throw Exception{"Unexpected column " + first_col->getName() + " as first argument of function " + function_name,
                    ErrorCodes::LOGICAL_ERROR};
        };
    }

    static WrapperType createIdentityWrapper(const DataTypePtr &)
    {
        return [] (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t /*input_rows_count*/)
        {
            return arguments.front().column;
        };
    }

    static WrapperType createNothingWrapper(const IDataType * to_type)
    {
        ColumnPtr res = to_type->createColumnConstWithDefaultValue(1);
        return [res] (ColumnsWithTypeAndName &, const DataTypePtr &, const ColumnNullable *, size_t input_rows_count)
        {
            /// Column of Nothing type is trivially convertible to any other column
            return res->cloneResized(input_rows_count)->convertToFullColumnIfConst();
        };
    }

    WrapperType prepareUnpackDictionaries(const DataTypePtr & from_type, const DataTypePtr & to_type) const
    {
        const auto * from_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(from_type.get());
        const auto * to_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(to_type.get());
        const auto & from_nested = from_low_cardinality ? from_low_cardinality->getDictionaryType() : from_type;
        const auto & to_nested = to_low_cardinality ? to_low_cardinality->getDictionaryType() : to_type;

        if (from_type->onlyNull())
        {
            if (!to_nested->isNullable())
            {
                if (cast_type == CastType::accurateOrNull)
                {
                    return createToNullableColumnWrapper();
                }
                else
                {
                    throw Exception{"Cannot convert NULL to a non-nullable type", ErrorCodes::CANNOT_CONVERT_TYPE};
                }
            }

            return [](ColumnsWithTypeAndName &, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count)
            {
                return result_type->createColumnConstWithDefaultValue(input_rows_count)->convertToFullColumnIfConst();
            };
        }

        bool skip_not_null_check = false;

        if (from_low_cardinality && from_nested->isNullable() && !to_nested->isNullable())
            /// Disable check for dictionary. Will check that column doesn't contain NULL in wrapper below.
            skip_not_null_check = true;

        auto wrapper = prepareRemoveNullable(from_nested, to_nested, skip_not_null_check);
        if (!from_low_cardinality && !to_low_cardinality)
            return wrapper;

        return [wrapper, from_low_cardinality, to_low_cardinality, skip_not_null_check]
                (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * nullable_source, size_t input_rows_count) -> ColumnPtr
        {
            ColumnsWithTypeAndName args = {arguments[0]};
            auto & arg = args.front();
            auto res_type = result_type;

            ColumnPtr converted_column;

            ColumnPtr res_indexes;
            /// For some types default can't be casted (for example, String to Int). In that case convert column to full.
            bool src_converted_to_full_column = false;

            {
                auto tmp_rows_count = input_rows_count;

                if (to_low_cardinality)
                    res_type = to_low_cardinality->getDictionaryType();

                if (from_low_cardinality)
                {
                    const auto * col_low_cardinality = typeid_cast<const ColumnLowCardinality *>(arguments[0].column.get());

                    if (skip_not_null_check && col_low_cardinality->containsNull())
                        throw Exception{"Cannot convert NULL value to non-Nullable type",
                                        ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN};

                    arg.column = col_low_cardinality->getDictionary().getNestedColumn();
                    arg.type = from_low_cardinality->getDictionaryType();

                    /// TODO: Make map with defaults conversion.
                    src_converted_to_full_column = !removeNullable(arg.type)->equals(*removeNullable(res_type));
                    if (src_converted_to_full_column)
                        arg.column = arg.column->index(col_low_cardinality->getIndexes(), 0);
                    else
                        res_indexes = col_low_cardinality->getIndexesPtr();

                    tmp_rows_count = arg.column->size();
                }

                /// Perform the requested conversion.
                converted_column = wrapper(args, res_type, nullable_source, tmp_rows_count);
            }

            if (to_low_cardinality)
            {
                auto res_column = to_low_cardinality->createColumn();
                auto * col_low_cardinality = typeid_cast<ColumnLowCardinality *>(res_column.get());

                if (from_low_cardinality && !src_converted_to_full_column)
                {
                    col_low_cardinality->insertRangeFromDictionaryEncodedColumn(*converted_column, *res_indexes);
                }
                else
                    col_low_cardinality->insertRangeFromFullColumn(*converted_column, 0, converted_column->size());

                return res_column;
            }
            else if (!src_converted_to_full_column)
                return converted_column->index(*res_indexes, 0);
            else
                return converted_column;
        };
    }

    WrapperType prepareRemoveNullable(const DataTypePtr & from_type, const DataTypePtr & to_type, bool skip_not_null_check) const
    {
        /// Determine whether pre-processing and/or post-processing must take place during conversion.

        bool source_is_nullable = from_type->isNullable();
        bool result_is_nullable = to_type->isNullable();

        auto wrapper = prepareImpl(removeNullable(from_type), removeNullable(to_type), result_is_nullable);

        if (result_is_nullable)
        {
            return [wrapper, source_is_nullable]
                (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
            {
                /// Create a temporary columns on which to perform the operation.
                const auto & nullable_type = static_cast<const DataTypeNullable &>(*result_type);
                const auto & nested_type = nullable_type.getNestedType();

                ColumnsWithTypeAndName tmp_args;
                if (source_is_nullable)
                    tmp_args = createBlockWithNestedColumns(arguments);
                else
                    tmp_args = arguments;

                const ColumnNullable * nullable_source = nullptr;

                /// Add original ColumnNullable for createStringToEnumWrapper()
                if (source_is_nullable)
                {
                    if (arguments.size() != 1)
                        throw Exception("Invalid number of arguments", ErrorCodes::LOGICAL_ERROR);
                    nullable_source = typeid_cast<const ColumnNullable *>(arguments.front().column.get());
                }

                /// Perform the requested conversion.
                auto tmp_res = wrapper(tmp_args, nested_type, nullable_source, input_rows_count);

                /// May happen in fuzzy tests. For debug purpose.
                if (!tmp_res)
                    throw Exception("Couldn't convert " + arguments[0].type->getName() + " to "
                                    + nested_type->getName() + " in " + " prepareRemoveNullable wrapper.", ErrorCodes::LOGICAL_ERROR);

                return wrapInNullable(tmp_res, arguments, nested_type, input_rows_count);
            };
        }
        else if (source_is_nullable)
        {
            /// Conversion from Nullable to non-Nullable.

            return [wrapper, skip_not_null_check]
                (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
            {
                auto tmp_args = createBlockWithNestedColumns(arguments);
                auto nested_type = removeNullable(result_type);

                /// Check that all values are not-NULL.
                /// Check can be skipped in case if LowCardinality dictionary is transformed.
                /// In that case, correctness will be checked beforehand.
                if (!skip_not_null_check)
                {
                    const auto & col = arguments[0].column;
                    const auto & nullable_col = assert_cast<const ColumnNullable &>(*col);
                    const auto & null_map = nullable_col.getNullMapData();

                    if (!memoryIsZero(null_map.data(), 0, null_map.size()))
                        throw Exception{"Cannot convert NULL value to non-Nullable type",
                                        ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN};
                }
                const ColumnNullable * nullable_source = typeid_cast<const ColumnNullable *>(arguments.front().column.get());
                return wrapper(tmp_args, nested_type, nullable_source, input_rows_count);
            };
        }
        else
            return wrapper;
    }

    /// 'from_type' and 'to_type' are nested types in case of Nullable.
    /// 'requested_result_is_nullable' is true if CAST to Nullable type is requested.
    WrapperType prepareImpl(const DataTypePtr & from_type, const DataTypePtr & to_type, bool requested_result_is_nullable) const
    {
        if (isUInt8(from_type) && isBool(to_type))
            return createUInt8ToBoolWrapper(from_type, to_type);

        /// We can cast IPv6 into IPv6, IPv4 into IPv4, but we should not allow to cast FixedString(16) into IPv6 as part of identity cast
        bool safe_convert_custom_types = true;

        if (const auto * to_type_custom_name = to_type->getCustomName())
            safe_convert_custom_types = from_type->getCustomName() && from_type->getCustomName()->getName() == to_type_custom_name->getName();
        else if (const auto * from_type_custom_name = from_type->getCustomName())
            safe_convert_custom_types = to_type->getCustomName() && from_type_custom_name->getName() == to_type->getCustomName()->getName();

        if (from_type->equals(*to_type) && safe_convert_custom_types)
            return createIdentityWrapper(from_type);
        else if (WhichDataType(from_type).isNothing())
            return createNothingWrapper(to_type.get());

        WrapperType ret;

        auto make_default_wrapper = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using ToDataType = typename Types::LeftType;

            if constexpr (
                std::is_same_v<ToDataType, DataTypeUInt16> ||
                std::is_same_v<ToDataType, DataTypeUInt32> ||
                std::is_same_v<ToDataType, DataTypeUInt64> ||
                std::is_same_v<ToDataType, DataTypeUInt128> ||
                std::is_same_v<ToDataType, DataTypeUInt256> ||
                std::is_same_v<ToDataType, DataTypeInt8> ||
                std::is_same_v<ToDataType, DataTypeInt16> ||
                std::is_same_v<ToDataType, DataTypeInt32> ||
                std::is_same_v<ToDataType, DataTypeInt64> ||
                std::is_same_v<ToDataType, DataTypeInt128> ||
                std::is_same_v<ToDataType, DataTypeInt256> ||
                std::is_same_v<ToDataType, DataTypeFloat32> ||
                std::is_same_v<ToDataType, DataTypeFloat64> ||
                std::is_same_v<ToDataType, DataTypeDate> ||
                std::is_same_v<ToDataType, DataTypeDate32> ||
                std::is_same_v<ToDataType, DataTypeDateTime> ||
                std::is_same_v<ToDataType, DataTypeUUID>)
            {
                ret = createWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()), requested_result_is_nullable);
                return true;
            }
            if constexpr (std::is_same_v<ToDataType, DataTypeUInt8>)
            {
                if (isBool(to_type))
                    ret = createBoolWrapper<ToDataType>(from_type, checkAndGetDataType<ToDataType>(to_type.get()), requested_result_is_nullable);
                else
                    ret = createWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()), requested_result_is_nullable);
                return true;
            }
            if constexpr (
                std::is_same_v<ToDataType, DataTypeEnum8> ||
                std::is_same_v<ToDataType, DataTypeEnum16>)
            {
                ret = createEnumWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()));
                return true;
            }
            if constexpr (
                std::is_same_v<ToDataType, DataTypeDecimal<Decimal32>> ||
                std::is_same_v<ToDataType, DataTypeDecimal<Decimal64>> ||
                std::is_same_v<ToDataType, DataTypeDecimal<Decimal128>> ||
                std::is_same_v<ToDataType, DataTypeDecimal<Decimal256>> ||
                std::is_same_v<ToDataType, DataTypeDateTime64>)
            {
                ret = createDecimalWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()), requested_result_is_nullable);
                return true;
            }

            return false;
        };

        bool cast_ipv4_ipv6_default_on_conversion_error_value = cast_ipv4_ipv6_default_on_conversion_error;

        auto make_custom_serialization_wrapper = [&, cast_ipv4_ipv6_default_on_conversion_error_value](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using ToDataType = typename Types::RightType;
            using FromDataType = typename Types::LeftType;

            if constexpr (WhichDataType(FromDataType::type_id).isStringOrFixedString())
            {
                if (to_type->getCustomSerialization() && to_type->getCustomName())
                {
                    if (to_type->getCustomName()->getName() == "IPv4")
                    {
                        ret = [cast_ipv4_ipv6_default_on_conversion_error_value](
                                  ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t)
                            -> ColumnPtr
                        {
                            if (!WhichDataType(result_type).isUInt32())
                                throw Exception(ErrorCodes::TYPE_MISMATCH, "Wrong result type {}. Expected UInt32", result_type->getName());

                            const auto * null_map = column_nullable ? &column_nullable->getNullMapData() : nullptr;
                            if (cast_ipv4_ipv6_default_on_conversion_error_value)
                                return convertToIPv4<IPStringToNumExceptionMode::Default>(arguments[0].column, null_map);
                            else
                                return convertToIPv4<IPStringToNumExceptionMode::Throw>(arguments[0].column, null_map);
                        };

                        return true;
                    }

                    if (to_type->getCustomName()->getName() == "IPv6")
                    {
                        ret = [cast_ipv4_ipv6_default_on_conversion_error_value](
                                  ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t)
                            -> ColumnPtr
                        {
                            if (!WhichDataType(result_type).isFixedString())
                                throw Exception(
                                    ErrorCodes::TYPE_MISMATCH, "Wrong result type {}. Expected FixedString", result_type->getName());

                            const auto * null_map = column_nullable ? &column_nullable->getNullMapData() : nullptr;
                            if (cast_ipv4_ipv6_default_on_conversion_error_value)
                                return convertToIPv6<IPStringToNumExceptionMode::Default>(arguments[0].column, null_map);
                            else
                                return convertToIPv6<IPStringToNumExceptionMode::Throw>(arguments[0].column, null_map);
                        };

                        return true;
                    }

                    ret = &ConvertImplGenericFromString<typename FromDataType::ColumnType>::execute;
                    return true;
                }
            }
            if constexpr (WhichDataType(ToDataType::type_id).isStringOrFixedString())
            {
                if (from_type->getCustomSerialization())
                {
                    ret = [](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
                    {
                        return ConvertImplGenericToString<typename ToDataType::ColumnType>::execute(arguments, result_type, input_rows_count);
                    };
                    return true;
                }
            }

            return false;
        };

        if (callOnTwoTypeIndexes(from_type->getTypeId(), to_type->getTypeId(), make_custom_serialization_wrapper))
            return ret;

        if (callOnIndexAndDataType<void>(to_type->getTypeId(), make_default_wrapper))
            return ret;

        switch (to_type->getTypeId())
        {
            case TypeIndex::String:
                return createStringWrapper(from_type);
            case TypeIndex::FixedString:
                return createFixedStringWrapper(from_type, checkAndGetDataType<DataTypeFixedString>(to_type.get())->getN());
            case TypeIndex::Array:
                return createArrayWrapper(from_type, static_cast<const DataTypeArray &>(*to_type));
            case TypeIndex::Tuple:
                return createTupleWrapper(from_type, checkAndGetDataType<DataTypeTuple>(to_type.get()));
            case TypeIndex::Map:
                return createMapWrapper(from_type, checkAndGetDataType<DataTypeMap>(to_type.get()));
            case TypeIndex::Object:
                return createObjectWrapper(from_type, checkAndGetDataType<DataTypeObject>(to_type.get()));
            case TypeIndex::AggregateFunction:
                return createAggregateFunctionWrapper(from_type, checkAndGetDataType<DataTypeAggregateFunction>(to_type.get()));
            default:
                break;
        }

        if (cast_type == CastType::accurateOrNull)
            return createToNullableColumnWrapper();
        else
            throw Exception{"Conversion from " + from_type->getName() + " to " + to_type->getName() + " is not supported",
                ErrorCodes::CANNOT_CONVERT_TYPE};
    }
};

class MonotonicityHelper
{
public:
    using MonotonicityForRange = FunctionCastBase::MonotonicityForRange;

    template <typename DataType>
    static auto monotonicityForType(const DataType * const)
    {
        return FunctionTo<DataType>::Type::Monotonic::get;
    }

    static MonotonicityForRange getMonotonicityInformation(const DataTypePtr & from_type, const IDataType * to_type)
    {
        if (const auto * type = checkAndGetDataType<DataTypeUInt8>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeUInt16>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeUInt32>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeUInt64>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeUInt128>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeUInt256>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeInt8>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeInt16>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeInt32>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeInt64>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeInt128>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeInt256>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeFloat32>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeFloat64>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeDate>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeDate32>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeDateTime>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeString>(to_type))
            return monotonicityForType(type);
        if (isEnum(from_type))
        {
            if (const auto * type = checkAndGetDataType<DataTypeEnum8>(to_type))
                return monotonicityForType(type);
            if (const auto * type = checkAndGetDataType<DataTypeEnum16>(to_type))
                return monotonicityForType(type);
        }
        /// other types like Null, FixedString, Array and Tuple have no monotonicity defined
        return {};
    }
};

}
