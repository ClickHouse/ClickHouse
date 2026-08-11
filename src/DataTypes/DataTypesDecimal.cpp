#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/Serializations/SerializationDecimal.h>

#include <Common/typeid_cast.h>
#include <Common/NaNUtils.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/readDecimalText.h>
#include <Parsers/ASTLiteral.h>

#include <type_traits>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int DECIMAL_OVERFLOW;
    extern const int NOT_IMPLEMENTED;
}


template <is_decimal T>
std::string DataTypeDecimal<T>::doGetName() const
{
    return fmt::format("Decimal({}, {})", this->precision, this->scale);
}

template <is_decimal T>
bool DataTypeDecimal<T>::equals(const IDataType & rhs) const
{
    if (auto * ptype = typeid_cast<const DataTypeDecimal<T> *>(&rhs))
        return this->scale == ptype->getScale();
    return false;
}

template <is_decimal T>
DataTypePtr DataTypeDecimal<T>::promoteNumericType() const
{
    if (sizeof(T) <= sizeof(Decimal128))
        return std::make_shared<DataTypeDecimal<Decimal128>>(DataTypeDecimal<Decimal128>::maxPrecision(), this->scale);
    return std::make_shared<DataTypeDecimal<Decimal256>>(DataTypeDecimal<Decimal256>::maxPrecision(), this->scale);
}

template <is_decimal T>
T DataTypeDecimal<T>::parseFromString(const String & str) const
{
    ReadBufferFromMemory buf(str);
    T x;
    UInt32 unread_scale = this->scale;
    readDecimalText(buf, x, this->precision, unread_scale, true);

    if (common::mulOverflow(x.value, DecimalUtils::scaleMultiplier<T>(unread_scale), x.value))
        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal math overflow");

    return x;
}

template <is_decimal T>
SerializationPtr DataTypeDecimal<T>::doGetSerialization(const SerializationInfoSettings &) const
{
    return std::make_shared<SerializationDecimal<T>>(this->precision, this->scale);
}


static DataTypePtr create(const ASTPtr & arguments)
{
    UInt64 precision = 10;
    UInt64 scale = 0;
    if (arguments)
    {
        if (arguments->children.empty() || arguments->children.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Decimal data type family must have precision and optional scale arguments");

        const auto * precision_arg = arguments->children[0]->as<ASTLiteral>();
        if (!precision_arg || precision_arg->value.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Decimal argument precision is invalid");
        precision = precision_arg->value.safeGet<UInt64>();

        if (arguments->children.size() == 2)
        {
            const auto * scale_arg = arguments->children[1]->as<ASTLiteral>();
            if (!scale_arg || !isInt64OrUInt64FieldType(scale_arg->value.getType()))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Decimal argument scale is invalid");
            scale = scale_arg->value.safeGet<UInt64>();
        }
    }

    return createDecimal<DataTypeDecimal>(precision, scale);
}

template <typename T>
static DataTypePtr createExact(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
        "Decimal32 | Decimal64 | Decimal128 | Decimal256 data type family must have exactly one arguments: scale");
    const auto * scale_arg = arguments->children[0]->as<ASTLiteral>();

    if (!scale_arg || !(scale_arg->value.getType() == Field::Types::Int64 || scale_arg->value.getType() == Field::Types::UInt64))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Decimal32 | Decimal64 | Decimal128 | Decimal256 data type family must have a one number as its argument");

    UInt64 precision = DecimalUtils::max_precision<T>;
    UInt64 scale = scale_arg->value.safeGet<UInt64>();

    return createDecimal<DataTypeDecimal>(precision, scale);
}

template <typename FromDataType, typename ToDataType, typename ReturnType>
requires (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
ReturnType convertDecimalsImpl(const typename FromDataType::FieldType & value, UInt32 scale_from, UInt32 scale_to, typename ToDataType::FieldType & result)
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using MaxFieldType = std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)), FromFieldType, ToFieldType>;
    using MaxNativeType = typename MaxFieldType::NativeType;

    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    MaxNativeType converted_value;
    if (scale_to > scale_from)
    {
        converted_value = DecimalUtils::scaleMultiplier<MaxNativeType>(scale_to - scale_from);
        if (common::mulOverflow(static_cast<MaxNativeType>(value.value), converted_value, converted_value))
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow while multiplying {} by scale {}",
                                std::string(ToDataType::family_name), toString(value.value), toString(converted_value));
            else
                return ReturnType(false);
        }
    }
    else if (scale_to == scale_from)
    {
        converted_value = value.value;
    }
    else
    {
        converted_value = value.value / DecimalUtils::scaleMultiplier<MaxNativeType>(scale_from - scale_to);
    }

    if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType))
    {
        if (converted_value < std::numeric_limits<typename ToFieldType::NativeType>::min() ||
            converted_value > std::numeric_limits<typename ToFieldType::NativeType>::max())
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow: {} is not in range ({}, {})",
                                std::string(ToDataType::family_name), toString(converted_value),
                                toString(std::numeric_limits<typename ToFieldType::NativeType>::min()),
                                toString(std::numeric_limits<typename ToFieldType::NativeType>::max()));
            else
                return ReturnType(false);
        }
    }

    result = static_cast<typename ToFieldType::NativeType>(converted_value);

    return ReturnType(true);
}

template <typename FromDataType, typename ToDataType, typename ReturnType>
requires (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
NO_SANITIZE_UNDEFINED void convertDecimalsBatch(
    const typename FromDataType::FieldType * __restrict from,
    typename ToDataType::FieldType * __restrict to,
    size_t size,
    UInt32 scale_from,
    UInt32 scale_to,
    ReturnType * __restrict nullmap)
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using MaxFieldType = std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)), FromFieldType, ToFieldType>;
    using MaxNativeType = typename MaxFieldType::NativeType;
    using ToNativeType = typename ToFieldType::NativeType;

    static constexpr bool has_nullmap = !std::is_same_v<ReturnType, void>;
    static constexpr bool check_overflow = sizeof(FromFieldType) > sizeof(ToFieldType);

    if (scale_to > scale_from)
    {
        const MaxNativeType multiplier = DecimalUtils::scaleMultiplier<MaxNativeType>(scale_to - scale_from);
        for (size_t i = 0; i < size; ++i)
        {
            MaxNativeType converted_value;
            bool mul_overflow = common::mulOverflow(static_cast<MaxNativeType>(from[i].value), multiplier, converted_value);

            bool range_overflow = false;
            if constexpr (check_overflow)
                range_overflow = converted_value < std::numeric_limits<ToNativeType>::min()
                              || converted_value > std::numeric_limits<ToNativeType>::max();

            bool overflow = mul_overflow | range_overflow;

            if constexpr (has_nullmap)
            {
                nullmap[i] = overflow;
                to[i] = overflow ? static_cast<ToNativeType>(0) : static_cast<ToNativeType>(converted_value);
            }
            else
            {
                if (mul_overflow)
                    throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow while multiplying {} by scale {}",
                                    std::string(ToDataType::family_name), toString(from[i].value), toString(multiplier));
                if (range_overflow)
                    throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow: {} is not in range ({}, {})",
                                    std::string(ToDataType::family_name), toString(converted_value),
                                    toString(std::numeric_limits<ToNativeType>::min()),
                                    toString(std::numeric_limits<ToNativeType>::max()));
                to[i] = static_cast<ToNativeType>(converted_value);
            }
        }
    }
    else if (scale_to == scale_from)
    {
        for (size_t i = 0; i < size; ++i)
        {
            MaxNativeType converted_value = from[i].value;

            if constexpr (check_overflow)
            {
                bool overflow = converted_value < std::numeric_limits<ToNativeType>::min()
                             || converted_value > std::numeric_limits<ToNativeType>::max();

                if constexpr (has_nullmap)
                {
                    nullmap[i] = overflow;
                    to[i] = overflow ? static_cast<ToNativeType>(0) : static_cast<ToNativeType>(converted_value);
                }
                else
                {
                    if (overflow)
                        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow: {} is not in range ({}, {})",
                                        std::string(ToDataType::family_name), toString(converted_value),
                                        toString(std::numeric_limits<ToNativeType>::min()),
                                        toString(std::numeric_limits<ToNativeType>::max()));
                    to[i] = static_cast<ToNativeType>(converted_value);
                }
            }
            else
            {
                if constexpr (has_nullmap)
                    nullmap[i] = false;
                to[i] = static_cast<ToNativeType>(converted_value);
            }
        }
    }
    else
    {
        const MaxNativeType divisor = DecimalUtils::scaleMultiplier<MaxNativeType>(scale_from - scale_to);
        for (size_t i = 0; i < size; ++i)
        {
            MaxNativeType converted_value = static_cast<MaxNativeType>(from[i].value) / divisor;

            if constexpr (check_overflow)
            {
                bool overflow = converted_value < std::numeric_limits<ToNativeType>::min()
                             || converted_value > std::numeric_limits<ToNativeType>::max();

                if constexpr (has_nullmap)
                {
                    nullmap[i] = overflow;
                    to[i] = overflow ? static_cast<ToNativeType>(0) : static_cast<ToNativeType>(converted_value);
                }
                else
                {
                    if (overflow)
                        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow: {} is not in range ({}, {})",
                                        std::string(ToDataType::family_name), toString(converted_value),
                                        toString(std::numeric_limits<ToNativeType>::min()),
                                        toString(std::numeric_limits<ToNativeType>::max()));
                    to[i] = static_cast<ToNativeType>(converted_value);
                }
            }
            else
            {
                if constexpr (has_nullmap)
                    nullmap[i] = false;
                to[i] = static_cast<ToNativeType>(converted_value);
            }
        }
    }
}

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    template void convertDecimalsImpl<FROM_DATA_TYPE, TO_DATA_TYPE, void>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale_from, UInt32 scale_to, typename TO_DATA_TYPE::FieldType & result); \
    template bool convertDecimalsImpl<FROM_DATA_TYPE, TO_DATA_TYPE, bool>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale_from, UInt32 scale_to, typename TO_DATA_TYPE::FieldType & result);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    template void convertDecimalsBatch<FROM_DATA_TYPE, TO_DATA_TYPE, void>(const typename FROM_DATA_TYPE::FieldType * __restrict, typename TO_DATA_TYPE::FieldType * __restrict, size_t, UInt32, UInt32, void *); \
    template void convertDecimalsBatch<FROM_DATA_TYPE, TO_DATA_TYPE, UInt8>(const typename FROM_DATA_TYPE::FieldType * __restrict, typename TO_DATA_TYPE::FieldType * __restrict, size_t, UInt32, UInt32, UInt8 *);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename FromDataType, typename ToDataType>
requires (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
typename ToDataType::FieldType convertDecimals(const typename FromDataType::FieldType & value, UInt32 scale_from, UInt32 scale_to)
{
    using ToFieldType = typename ToDataType::FieldType;
    ToFieldType result;

    convertDecimalsImpl<FromDataType, ToDataType, void>(value, scale_from, scale_to, result);

    return result;
}

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    template typename TO_DATA_TYPE::FieldType convertDecimals<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale_from, UInt32 scale_to);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef DISPATCH


template <typename FromDataType, typename ToDataType>
requires (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
bool tryConvertDecimals(const typename FromDataType::FieldType & value, UInt32 scale_from, UInt32 scale_to, typename ToDataType::FieldType & result)
{
    return convertDecimalsImpl<FromDataType, ToDataType, bool>(value, scale_from, scale_to, result);
}

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    template bool tryConvertDecimals<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale_from, UInt32 scale_to, typename TO_DATA_TYPE::FieldType & result);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef DISPATCH


template <typename FromDataType, typename ToDataType, typename ReturnType>
requires (IsDataTypeDecimal<FromDataType> && is_arithmetic_v<typename ToDataType::FieldType>)
ReturnType convertFromDecimalImpl(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType & result)
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    return DecimalUtils::convertToImpl<ToFieldType, FromFieldType, ReturnType>(value, scale, result);
}

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    template void convertFromDecimalImpl<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale, typename TO_DATA_TYPE::FieldType & result); \
    template bool convertFromDecimalImpl<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale, typename TO_DATA_TYPE::FieldType & result);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_ARITHMETIC_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename FromDataType, typename ToDataType>
requires (IsDataTypeDecimal<FromDataType> && is_arithmetic_v<typename ToDataType::FieldType>)
inline typename ToDataType::FieldType convertFromDecimal(const typename FromDataType::FieldType & value, UInt32 scale)
{
    typename ToDataType::FieldType result;
    convertFromDecimalImpl<FromDataType, ToDataType, void>(value, scale, result);
    return result;
}

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    template typename TO_DATA_TYPE::FieldType convertFromDecimal<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_ARITHMETIC_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename FromDataType, typename ToDataType>
requires (IsDataTypeDecimal<FromDataType> && is_arithmetic_v<typename ToDataType::FieldType>)
inline bool tryConvertFromDecimal(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType& result)
{
    return convertFromDecimalImpl<FromDataType, ToDataType, bool>(value, scale, result);
}

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    template bool tryConvertFromDecimal<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale, typename TO_DATA_TYPE::FieldType& result);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_ARITHMETIC_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename FromDataType, typename ToDataType, typename ReturnType>
requires (is_arithmetic_v<typename FromDataType::FieldType> && IsDataTypeDecimal<ToDataType>)
ReturnType convertToDecimalImpl(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType & result)
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using ToNativeType = typename ToFieldType::NativeType;

    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    if constexpr (std::is_same_v<typename FromDataType::FieldType, BFloat16>)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Conversion from BFloat16 to Decimal is not implemented");
    }
    else if constexpr (is_floating_point<FromFieldType>)
    {
        if (!isFinite(value))
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow. Cannot convert infinity or NaN to decimal", ToDataType::family_name);
            else
                return ReturnType(false);
        }

        auto out = value * static_cast<FromFieldType>(DecimalUtils::scaleMultiplier<ToNativeType>(scale));

        if (out <= static_cast<FromFieldType>(std::numeric_limits<ToNativeType>::min()) ||
            out >= static_cast<FromFieldType>(std::numeric_limits<ToNativeType>::max()))
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow. Float is out of Decimal range", ToDataType::family_name);
            else
                return ReturnType(false);
        }

        result = static_cast<ToNativeType>(out);
        return ReturnType(true);
    }
    else
    {
        if constexpr (is_big_int_v<FromFieldType>)
            return ReturnType(convertDecimalsImpl<DataTypeDecimal<Decimal256>, ToDataType, ReturnType>(static_cast<Int256>(value), 0, scale, result));
        else if constexpr (std::is_same_v<FromFieldType, UInt64>)
            return ReturnType(convertDecimalsImpl<DataTypeDecimal<Decimal128>, ToDataType, ReturnType>(static_cast<Int128>(value), 0, scale, result));
        else
            return ReturnType(convertDecimalsImpl<DataTypeDecimal<Decimal64>, ToDataType, ReturnType>(static_cast<Int64>(value), 0, scale, result));
    }
}

template <typename FromDataType, typename ToDataType, typename ReturnType>
requires (is_arithmetic_v<typename FromDataType::FieldType> && IsDataTypeDecimal<ToDataType>)
NO_SANITIZE_UNDEFINED void convertToDecimalBatch(
    const typename FromDataType::FieldType * __restrict from,
    typename ToDataType::FieldType * __restrict to,
    size_t size,
    UInt32 scale,
    ReturnType * __restrict nullmap)
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToNativeType = typename ToDataType::FieldType::NativeType;

    static constexpr bool has_nullmap = !std::is_same_v<ReturnType, void>;

    if constexpr (std::is_same_v<FromFieldType, BFloat16>)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Conversion from BFloat16 to Decimal is not implemented");
    }
    else if constexpr (is_floating_point<FromFieldType>)
    {
        const auto multiplier = static_cast<FromFieldType>(DecimalUtils::scaleMultiplier<ToNativeType>(scale));
        for (size_t i = 0; i < size; ++i)
        {
            bool overflow = !isFinite(from[i]);
            FromFieldType out = from[i] * multiplier;

            overflow |= out <= static_cast<FromFieldType>(std::numeric_limits<ToNativeType>::min())
                     || out >= static_cast<FromFieldType>(std::numeric_limits<ToNativeType>::max());

            if constexpr (has_nullmap)
            {
                nullmap[i] = overflow;
                to[i] = overflow ? static_cast<ToNativeType>(0) : static_cast<ToNativeType>(out);
            }
            else
            {
                if (overflow)
                {
                    if (!isFinite(from[i]))
                        throw Exception(ErrorCodes::DECIMAL_OVERFLOW,
                            "{} convert overflow. Cannot convert infinity or NaN to decimal",
                            ToDataType::family_name);
                    else
                        throw Exception(ErrorCodes::DECIMAL_OVERFLOW,
                            "{} convert overflow. Float is out of Decimal range",
                            ToDataType::family_name);
                }
                to[i] = static_cast<ToNativeType>(out);
            }
        }
    }
    else
    {
        /// For integer types, widen to match convertToDecimalImpl which delegates to
        /// convertDecimalsImpl. The intermediate type must be at least as wide as both:
        ///   1. The source intermediate: big ints → Int256, UInt64 → Int128, else → Int64
        ///   2. The target ToNativeType (e.g. Int128 for Decimal128)
        /// convertDecimalsImpl picks MaxNativeType = max(sizeof(From), sizeof(To)).
        using FromIntermediate = std::conditional_t<is_big_int_v<FromFieldType>, Int256,
                                 std::conditional_t<std::is_same_v<FromFieldType, UInt64>, Int128, Int64>>;
        using WideType = std::conditional_t<(sizeof(FromIntermediate) > sizeof(ToNativeType)),
                                            FromIntermediate, ToNativeType>;

        if (scale == 0)
        {
            /// Fast path: scale 0 means multiplier is 1, just widen and bounds-check.
            /// This avoids expensive wide multiplication (especially for Int256).
            for (size_t i = 0; i < size; ++i)
            {
                WideType converted_value = static_cast<WideType>(from[i]);
                bool overflow = converted_value < std::numeric_limits<ToNativeType>::min()
                             || converted_value > std::numeric_limits<ToNativeType>::max();

                if constexpr (has_nullmap)
                {
                    nullmap[i] = overflow;
                    to[i] = overflow ? static_cast<ToNativeType>(0) : static_cast<ToNativeType>(converted_value);
                }
                else
                {
                    if (overflow)
                        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow", std::string(ToDataType::family_name));
                    to[i] = static_cast<ToNativeType>(converted_value);
                }
            }
        }
        else
        {
            const WideType multiplier = DecimalUtils::scaleMultiplier<WideType>(scale);
            for (size_t i = 0; i < size; ++i)
            {
                WideType converted_value;
                bool overflow = common::mulOverflow(static_cast<WideType>(from[i]), multiplier, converted_value);

                overflow |= converted_value < std::numeric_limits<ToNativeType>::min()
                         || converted_value > std::numeric_limits<ToNativeType>::max();

                if constexpr (has_nullmap)
                {
                    nullmap[i] = overflow;
                    to[i] = overflow ? static_cast<ToNativeType>(0) : static_cast<ToNativeType>(converted_value);
                }
                else
                {
                    if (overflow)
                        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow", std::string(ToDataType::family_name));
                    to[i] = static_cast<ToNativeType>(converted_value);
                }
            }
        }
    }
}

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    template void convertToDecimalImpl<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale, typename TO_DATA_TYPE::FieldType & result);  \
    template bool convertToDecimalImpl<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale, typename TO_DATA_TYPE::FieldType & result);
#define INVOKE(X) FOR_EACH_ARITHMETIC_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    template void convertToDecimalBatch<FROM_DATA_TYPE, TO_DATA_TYPE, void>(const typename FROM_DATA_TYPE::FieldType * __restrict, typename TO_DATA_TYPE::FieldType * __restrict, size_t, UInt32, void *); \
    template void convertToDecimalBatch<FROM_DATA_TYPE, TO_DATA_TYPE, UInt8>(const typename FROM_DATA_TYPE::FieldType * __restrict, typename TO_DATA_TYPE::FieldType * __restrict, size_t, UInt32, UInt8 *);
#define INVOKE(X) FOR_EACH_ARITHMETIC_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename FromDataType, typename ToDataType>
requires (is_arithmetic_v<typename FromDataType::FieldType> && IsDataTypeDecimal<ToDataType>)
inline typename ToDataType::FieldType convertToDecimal(const typename FromDataType::FieldType & value, UInt32 scale)
{
    typename ToDataType::FieldType result;
    convertToDecimalImpl<FromDataType, ToDataType, void>(value, scale, result);
    return result;
}

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    template typename TO_DATA_TYPE::FieldType convertToDecimal<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale);
#define INVOKE(X) FOR_EACH_ARITHMETIC_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename FromDataType, typename ToDataType>
requires (is_arithmetic_v<typename FromDataType::FieldType> && IsDataTypeDecimal<ToDataType>)
inline bool tryConvertToDecimal(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType& result)
{
    return convertToDecimalImpl<FromDataType, ToDataType, bool>(value, scale, result);
}

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    template bool tryConvertToDecimal<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale, typename TO_DATA_TYPE::FieldType& result);
#define INVOKE(X) FOR_EACH_ARITHMETIC_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename T>
DataTypePtr createDecimalMaxPrecision(UInt64 scale)
{
    return std::make_shared<DataTypeDecimal<T>>(DecimalUtils::max_precision<T>, scale);
}

template DataTypePtr createDecimalMaxPrecision<Decimal32>(UInt64 scale);
template DataTypePtr createDecimalMaxPrecision<Decimal64>(UInt64 scale);
template DataTypePtr createDecimalMaxPrecision<Decimal128>(UInt64 scale);
template DataTypePtr createDecimalMaxPrecision<Decimal256>(UInt64 scale);

/// Explicit template instantiations.
template class DataTypeDecimal<Decimal32>;
template class DataTypeDecimal<Decimal64>;
template class DataTypeDecimal<Decimal128>;
template class DataTypeDecimal<Decimal256>;

void registerDataTypeDecimal(DataTypeFactory & factory)
{
    factory.registerDataType("Decimal32", createExact<Decimal32>, DataTypeFactory::Case::Insensitive);
    factory.registerDataType("Decimal64", createExact<Decimal64>, DataTypeFactory::Case::Insensitive);
    factory.registerDataType("Decimal128", createExact<Decimal128>, DataTypeFactory::Case::Insensitive);
    factory.registerDataType("Decimal256", createExact<Decimal256>, DataTypeFactory::Case::Insensitive);

    factory.registerDataType("Decimal", create, DataTypeFactory::Case::Insensitive);
    factory.registerAlias("DEC", "Decimal", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("NUMERIC", "Decimal", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("FIXED", "Decimal", DataTypeFactory::Case::Insensitive);
}

}
