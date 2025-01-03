#pragma once

#include <base/arithmeticOverflow.h>
#include <base/extended_types.h>
#include <Common/typeid_cast.h>
#include <base/Decimal.h>
#include <base/Decimal_fwd.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeDateTime64.h>


namespace DB
{

namespace ErrorCodes
{
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
    M(DataTypeDecimal<DateTime64>) \
    M(DataTypeDateTime64) \
    M(DataTypeDecimal32) \
    M(DataTypeDecimal64) \
    M(DataTypeDecimal128) \
    M(DataTypeDecimal256)

#define FOR_EACH_DECIMAL_TYPE_PASS(M, X) \
    M(DataTypeDecimal<DateTime64>, X) \
    M(DataTypeDateTime64, X) \
    M(DataTypeDecimal32, X) \
    M(DataTypeDecimal64, X) \
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
#undef INVOKE
#undef DISPATCH


template <typename FromDataType, typename ToDataType>
requires (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
typename ToDataType::FieldType convertDecimals(const typename FromDataType::FieldType & value, UInt32 scale_from, UInt32 scale_to);

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    extern template typename TO_DATA_TYPE::FieldType convertDecimals<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale_from, UInt32 scale_to);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename FromDataType, typename ToDataType>
requires (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
bool tryConvertDecimals(const typename FromDataType::FieldType & value, UInt32 scale_from, UInt32 scale_to, typename ToDataType::FieldType & result);

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    extern template bool tryConvertDecimals<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale_from, UInt32 scale_to, typename TO_DATA_TYPE::FieldType & result);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename FromDataType, typename ToDataType, typename ReturnType>
requires (IsDataTypeDecimal<FromDataType> && is_arithmetic_v<typename ToDataType::FieldType>)
ReturnType convertFromDecimalImpl(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType & result);

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    extern template void convertFromDecimalImpl<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale, typename TO_DATA_TYPE::FieldType & result); \
    extern template bool convertFromDecimalImpl<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale, typename TO_DATA_TYPE::FieldType & result);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_ARITHMETIC_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename FromDataType, typename ToDataType>
requires (IsDataTypeDecimal<FromDataType> && is_arithmetic_v<typename ToDataType::FieldType>)
typename ToDataType::FieldType convertFromDecimal(const typename FromDataType::FieldType & value, UInt32 scale);

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    extern template typename TO_DATA_TYPE::FieldType convertFromDecimal<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_ARITHMETIC_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename FromDataType, typename ToDataType>
requires (IsDataTypeDecimal<FromDataType> && is_arithmetic_v<typename ToDataType::FieldType>)
bool tryConvertFromDecimal(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType& result);

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    extern template bool tryConvertFromDecimal<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale, typename TO_DATA_TYPE::FieldType& result);
#define INVOKE(X) FOR_EACH_DECIMAL_TYPE_PASS(DISPATCH, X)
FOR_EACH_ARITHMETIC_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename FromDataType, typename ToDataType, typename ReturnType>
requires (is_arithmetic_v<typename FromDataType::FieldType> && IsDataTypeDecimal<ToDataType>)
ReturnType convertToDecimalImpl(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType& result);

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    extern template void convertToDecimalImpl<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale, typename TO_DATA_TYPE::FieldType & result);  \
    extern template bool convertToDecimalImpl<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale, typename TO_DATA_TYPE::FieldType & result);
#define INVOKE(X) FOR_EACH_ARITHMETIC_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename FromDataType, typename ToDataType>
requires (is_arithmetic_v<typename FromDataType::FieldType> && IsDataTypeDecimal<ToDataType>)
typename ToDataType::FieldType convertToDecimal(const typename FromDataType::FieldType & value, UInt32 scale);

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    extern template typename TO_DATA_TYPE::FieldType convertToDecimal<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale);
#define INVOKE(X) FOR_EACH_ARITHMETIC_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename FromDataType, typename ToDataType>
requires (is_arithmetic_v<typename FromDataType::FieldType> && IsDataTypeDecimal<ToDataType>)
bool tryConvertToDecimal(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType& result);

#define DISPATCH(FROM_DATA_TYPE, TO_DATA_TYPE) \
    extern template bool tryConvertToDecimal<FROM_DATA_TYPE, TO_DATA_TYPE>(const typename FROM_DATA_TYPE::FieldType & value, UInt32 scale, typename TO_DATA_TYPE::FieldType& result);
#define INVOKE(X) FOR_EACH_ARITHMETIC_TYPE_PASS(DISPATCH, X)
FOR_EACH_DECIMAL_TYPE(INVOKE);
#undef INVOKE
#undef DISPATCH


template <typename T>
DataTypePtr createDecimalMaxPrecision(UInt64 scale);

extern template DataTypePtr createDecimalMaxPrecision<Decimal32>(UInt64 scale);
extern template DataTypePtr createDecimalMaxPrecision<Decimal64>(UInt64 scale);
extern template DataTypePtr createDecimalMaxPrecision<Decimal128>(UInt64 scale);
extern template DataTypePtr createDecimalMaxPrecision<Decimal256>(UInt64 scale);

extern template class DataTypeDecimal<Decimal32>;
extern template class DataTypeDecimal<Decimal64>;
extern template class DataTypeDecimal<Decimal128>;
extern template class DataTypeDecimal<Decimal256>;

}
