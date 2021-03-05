#pragma once

#include <common/arithmeticOverflow.h>
#include <Common/typeid_cast.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeDecimalBase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
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
template <typename T>
class DataTypeDecimal final : public DataTypeDecimalBase<T>
{
    using Base = DataTypeDecimalBase<T>;
    static_assert(IsDecimalNumber<T>);

public:
    using typename Base::FieldType;
    using typename Base::ColumnType;
    using Base::Base;

    static constexpr auto family_name = "Decimal";

    const char * getFamilyName() const override { return family_name; }
    std::string doGetName() const override;
    TypeIndex getTypeId() const override { return TypeId<T>::value; }
    bool canBePromoted() const override { return true; }
    DataTypePtr promoteNumericType() const override;

    bool equals(const IDataType & rhs) const override;
    T parseFromString(const String & str) const;
    SerializationPtr doGetDefaultSerialization() const override;
};

template <typename T>
inline const DataTypeDecimal<T> * checkDecimal(const IDataType & data_type)
{
    return typeid_cast<const DataTypeDecimal<T> *>(&data_type);
}

inline UInt32 getDecimalScale(const IDataType & data_type, UInt32 default_value = std::numeric_limits<UInt32>::max())
{
    if (auto * decimal_type = checkDecimal<Decimal32>(data_type))
        return decimal_type->getScale();
    if (auto * decimal_type = checkDecimal<Decimal64>(data_type))
        return decimal_type->getScale();
    if (auto * decimal_type = checkDecimal<Decimal128>(data_type))
        return decimal_type->getScale();
    if (auto * decimal_type = checkDecimal<Decimal256>(data_type))
        return decimal_type->getScale();
    return default_value;
}

inline UInt32 getDecimalPrecision(const IDataType & data_type)
{
    if (auto * decimal_type = checkDecimal<Decimal32>(data_type))
        return decimal_type->getPrecision();
    if (auto * decimal_type = checkDecimal<Decimal64>(data_type))
        return decimal_type->getPrecision();
    if (auto * decimal_type = checkDecimal<Decimal128>(data_type))
        return decimal_type->getPrecision();
    if (auto * decimal_type = checkDecimal<Decimal256>(data_type))
        return decimal_type->getPrecision();
    return 0;
}

template <typename T>
inline UInt32 getDecimalScale(const DataTypeDecimal<T> & data_type)
{
    return data_type.getScale();
}

template <typename FromDataType, typename ToDataType, typename ReturnType = void>
inline std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>, ReturnType>
convertDecimalsImpl(const typename FromDataType::FieldType & value, UInt32 scale_from, UInt32 scale_to, typename ToDataType::FieldType& result)
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
                throw Exception(std::string(ToDataType::family_name) + " convert overflow",
                                ErrorCodes::DECIMAL_OVERFLOW);
            else
                return ReturnType(false);
        }
    }
    else
        converted_value = value.value / DecimalUtils::scaleMultiplier<MaxNativeType>(scale_from - scale_to);

    if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType))
    {
        if (converted_value < std::numeric_limits<typename ToFieldType::NativeType>::min() ||
            converted_value > std::numeric_limits<typename ToFieldType::NativeType>::max())
        {
            if constexpr (throw_exception)
                throw Exception(std::string(ToDataType::family_name) + " convert overflow",
                                ErrorCodes::DECIMAL_OVERFLOW);
            else
                return ReturnType(false);
        }
    }

    result = static_cast<typename ToFieldType::NativeType>(converted_value);

    return ReturnType(true);
}

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>, typename ToDataType::FieldType>
convertDecimals(const typename FromDataType::FieldType & value, UInt32 scale_from, UInt32 scale_to)
{
    using ToFieldType = typename ToDataType::FieldType;
    ToFieldType result;

    convertDecimalsImpl<FromDataType, ToDataType, void>(value, scale_from, scale_to, result);

    return result;
}

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>, bool>
tryConvertDecimals(const typename FromDataType::FieldType & value, UInt32 scale_from, UInt32 scale_to, typename ToDataType::FieldType& result)
{
    return convertDecimalsImpl<FromDataType, ToDataType, bool>(value, scale_from, scale_to, result);
}

template <typename FromDataType, typename ToDataType, typename ReturnType>
inline std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsNumber<typename ToDataType::FieldType>, ReturnType>
convertFromDecimalImpl(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType& result)
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    return DecimalUtils::convertToImpl<ToFieldType, FromFieldType, ReturnType>(value, scale, result);
}

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsNumber<typename ToDataType::FieldType>, typename ToDataType::FieldType>
convertFromDecimal(const typename FromDataType::FieldType & value, UInt32 scale)
{
    typename ToDataType::FieldType result;

    convertFromDecimalImpl<FromDataType, ToDataType, void>(value, scale, result);

    return result;
}

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsNumber<typename ToDataType::FieldType>, bool>
tryConvertFromDecimal(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType& result)
{
    return convertFromDecimalImpl<FromDataType, ToDataType, bool>(value, scale, result);
}

template <typename FromDataType, typename ToDataType, typename ReturnType>
inline std::enable_if_t<IsNumber<typename FromDataType::FieldType> && IsDataTypeDecimal<ToDataType>, ReturnType>
convertToDecimalImpl(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType& result)
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
                throw Exception(std::string(ToDataType::family_name) + " convert overflow. Cannot convert infinity or NaN to decimal",
                                ErrorCodes::DECIMAL_OVERFLOW);
            else
                return false;
        }

        auto out = value * static_cast<FromFieldType>(DecimalUtils::scaleMultiplier<ToNativeType>(scale));
        if constexpr (std::is_same_v<ToNativeType, Int128>)
        {
            static constexpr Int128 min_int128 = minInt128();
            static constexpr Int128 max_int128 = maxInt128();

            if (out <= static_cast<ToNativeType>(min_int128) || out >= static_cast<ToNativeType>(max_int128))
            {
                if constexpr (throw_exception)
                    throw Exception(std::string(ToDataType::family_name) + " convert overflow. Float is out of Decimal range",
                                    ErrorCodes::DECIMAL_OVERFLOW);
                else
                    return ReturnType(false);
            }
        }
        else
        {
            if (out <= static_cast<FromFieldType>(std::numeric_limits<ToNativeType>::min()) ||
                out >= static_cast<FromFieldType>(std::numeric_limits<ToNativeType>::max()))
            {
                if constexpr (throw_exception)
                    throw Exception(std::string(ToDataType::family_name) + " convert overflow. Float is out of Decimal range",
                                    ErrorCodes::DECIMAL_OVERFLOW);
                else
                    return ReturnType(false);
            }
        }

        result = static_cast<ToNativeType>(out);

        return ReturnType(true);
    }
    else
    {
        if constexpr (is_big_int_v<FromFieldType>)
            return ReturnType(convertDecimalsImpl<DataTypeDecimal<Decimal256>, ToDataType, ReturnType>(static_cast<Int256>(value), 0, scale, result));
        else if constexpr (std::is_same_v<FromFieldType, UInt64>)
            return ReturnType(convertDecimalsImpl<DataTypeDecimal<Decimal128>, ToDataType, ReturnType>(value, 0, scale, result));
        else
            return ReturnType(convertDecimalsImpl<DataTypeDecimal<Decimal64>, ToDataType, ReturnType>(value, 0, scale, result));
    }
}

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsNumber<typename FromDataType::FieldType> && IsDataTypeDecimal<ToDataType>, typename ToDataType::FieldType>
convertToDecimal(const typename FromDataType::FieldType & value, UInt32 scale)
{
    typename ToDataType::FieldType result;

    convertToDecimalImpl<FromDataType, ToDataType, void>(value, scale, result);

    return result;
}

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsNumber<typename FromDataType::FieldType> && IsDataTypeDecimal<ToDataType>, bool>
tryConvertToDecimal(const typename FromDataType::FieldType & value, UInt32 scale, typename ToDataType::FieldType& result)
{
    return convertToDecimalImpl<FromDataType, ToDataType, bool>(value, scale, result);
}

template <typename T>
inline DataTypePtr createDecimalMaxPrecision(UInt64 scale)
{
    return std::make_shared<DataTypeDecimal<T>>(DecimalUtils::max_precision<T>, scale);
}

}
