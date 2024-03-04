#pragma once

#include <base/arithmeticOverflow.h>
#include <base/extended_types.h>
#include <Common/typeid_cast.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeDateTime64.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
    extern const int LOGICAL_ERROR;
}

/// Implements Decimal(P, S), where P is precision, S is scale.
/// Maximum precisions for underlying types are:
/// Int32    9
/// Int64   18
/// Int128  38
/// Int256  76
/// Operation between two decimals leads to Decimal(P, S), where
///     P is one of (9, 18, 38, 76); equals to the maximum precision for the biggest underlying type of operands.
///     S is maximum scale of operands. The allowed valuas are [0, precision]
template <is_decimal T>
class DataTypeDecimal final : public DataTypeDecimalBase<T>
{
    using Base = DataTypeDecimalBase<T>;

public:
    using typename Base::FieldType;
    using typename Base::ColumnType;
    using Base::Base;

    static constexpr auto family_name = "Decimal";

    const char * getFamilyName() const override { return family_name; }

    std::string doGetName() const override;
    TypeIndex getTypeId() const override { return TypeToTypeIndex<T>; }
    bool canBePromoted() const override { return true; }
    DataTypePtr promoteNumericType() const override;

    bool equals(const IDataType & rhs) const override;
    T parseFromString(const String & str) const;
    SerializationPtr doGetDefaultSerialization() const override;
};

using DataTypeDecimal32 = DataTypeDecimal<Decimal32>;
using DataTypeDecimal64 = DataTypeDecimal<Decimal64>;
using DataTypeDecimal128 = DataTypeDecimal<Decimal128>;
using DataTypeDecimal256 = DataTypeDecimal<Decimal256>;

template <typename T>
inline const DataTypeDecimal<T> * checkDecimal(const IDataType & data_type)
{
    return typeid_cast<const DataTypeDecimal<T> *>(&data_type);
}

inline UInt32 getDecimalScale(const IDataType & data_type)
{
    if (const auto * decimal_type = checkDecimal<Decimal32>(data_type))
        return decimal_type->getScale();
    if (const auto * decimal_type = checkDecimal<Decimal64>(data_type))
        return decimal_type->getScale();
    if (const auto * decimal_type = checkDecimal<Decimal128>(data_type))
        return decimal_type->getScale();
    if (const auto * decimal_type = checkDecimal<Decimal256>(data_type))
        return decimal_type->getScale();
    if (const auto * date_time_type = typeid_cast<const DataTypeDateTime64 *>(&data_type))
        return date_time_type->getScale();

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get decimal scale from type {}", data_type.getName());
}

inline UInt32 getDecimalPrecision(const IDataType & data_type)
{
    if (const auto * decimal_type = checkDecimal<Decimal32>(data_type))
        return decimal_type->getPrecision();
    if (const auto * decimal_type = checkDecimal<Decimal64>(data_type))
        return decimal_type->getPrecision();
    if (const auto * decimal_type = checkDecimal<Decimal128>(data_type))
        return decimal_type->getPrecision();
    if (const auto * decimal_type = checkDecimal<Decimal256>(data_type))
        return decimal_type->getPrecision();
    if (const auto * date_time_type = typeid_cast<const DataTypeDateTime64 *>(&data_type))
        return date_time_type->getPrecision();

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get decimal precision from type {}", data_type.getName());
}

template <typename T>
inline UInt32 getDecimalScale(const DataTypeDecimal<T> & data_type)
{
    return data_type.getScale();
}

#define FOR_EACH_DECIMAL_TYPE(M) \
    M(DataTypeDateTime64) \
    M(DataTypeDecimal32) \
    M(DataTypeDecimal64) \
    M(DataTypeDecimal128) \
    M(DataTypeDecimal256)

#define FOR_EACH_DECIMAL_TYPE_PASS(M, X) \
    M(DataTypeDateTime64, X) \
    M(DataTypeDecimal32,  X) \
    M(DataTypeDecimal64,  X) \
    M(DataTypeDecimal128, X) \
    M(DataTypeDecimal256, X)


template <typename FromDataType, typename ToDataType, typename ReturnType = void>
requires (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
ReturnType convertDecimalsImpl(const typename FromDataType::FieldType & value, UInt32 scale_from, UInt32 scale_to, typename ToDataType::FieldType & result);

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    extern template void convertDecimalsImpl<FROM_DATA_TYPE, TO_DATA_TYPE, void>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale_from, UInt32 scale_to, typename TO_DATA_TYPE::FieldType & result); \
    extern template bool convertDecimalsImpl<FROM_DATA_TYPE, TO_DATA_TYPE, bool>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale_from, UInt32 scale_to, typename TO_DATA_TYPE::FieldType & result);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef DISPATCH


template <typename FromDataType, typename ToDataType>
requires (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
typename ToDataType::FieldType convertDecimals(const typename FromDataType::FieldType & value, UInt32 scale_from, UInt32 scale_to);

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    extern template typename TO_DATA_TYPE::FieldType convertDecimals<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale_from, UInt32 scale_to);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef DISPATCH


template <typename FromDataType, typename ToDataType>
requires (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
bool tryConvertDecimals(const typename FromDataType::FieldType & value, UInt32 scale_from, UInt32 scale_to, typename ToDataType::FieldType & result);

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    extern template bool tryConvertDecimals<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale_from, UInt32 scale_to, typename TO_DATA_TYPE::FieldType & result);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef DISPATCH


template <typename FromDataType, typename ToDataType, typename ReturnType>
requires (IsDataTypeDecimal<FromDataType> && is_arithmetic_v<typename ToDataType::FieldType>)
inline ReturnType convertFromDecimalImpl(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType& result)
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    return DecimalUtils::convertToImpl<ToFieldType, FromFieldType, ReturnType>(value, scale, result);
}

template <typename FromDataType, typename ToDataType>
requires (IsDataTypeDecimal<FromDataType> && is_arithmetic_v<typename ToDataType::FieldType>)
inline typename ToDataType::FieldType convertFromDecimal(const typename FromDataType::FieldType & value, UInt32 scale)
{
    typename ToDataType::FieldType result;

    convertFromDecimalImpl<FromDataType, ToDataType, void>(value, scale, result);

    return result;
}

template <typename FromDataType, typename ToDataType>
requires (IsDataTypeDecimal<FromDataType> && is_arithmetic_v<typename ToDataType::FieldType>)
inline bool tryConvertFromDecimal(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType& result)
{
    return convertFromDecimalImpl<FromDataType, ToDataType, bool>(value, scale, result);
}

template <typename FromDataType, typename ToDataType, typename ReturnType>
requires (is_arithmetic_v<typename FromDataType::FieldType> && IsDataTypeDecimal<ToDataType>)
inline ReturnType convertToDecimalImpl(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType& result)
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using ToNativeType = typename ToFieldType::NativeType;

    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    if constexpr (std::is_floating_point_v<FromFieldType>)
    {
        if (!std::isfinite(value))
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

template <typename FromDataType, typename ToDataType>
requires (is_arithmetic_v<typename FromDataType::FieldType> && IsDataTypeDecimal<ToDataType>)
inline typename ToDataType::FieldType convertToDecimal(const typename FromDataType::FieldType & value, UInt32 scale)
{
    typename ToDataType::FieldType result;
    convertToDecimalImpl<FromDataType, ToDataType, void>(value, scale, result);
    return result;
}

template <typename FromDataType, typename ToDataType>
requires (is_arithmetic_v<typename FromDataType::FieldType> && IsDataTypeDecimal<ToDataType>)
inline bool tryConvertToDecimal(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType& result)
{
    return convertToDecimalImpl<FromDataType, ToDataType, bool>(value, scale, result);
}

template <typename T>
inline DataTypePtr createDecimalMaxPrecision(UInt64 scale)
{
    return std::make_shared<DataTypeDecimal<T>>(DecimalUtils::max_precision<T>, scale);
}

extern template class DataTypeDecimal<Decimal32>;
extern template class DataTypeDecimal<Decimal64>;
extern template class DataTypeDecimal<Decimal128>;
extern template class DataTypeDecimal<Decimal256>;

}
