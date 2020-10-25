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
/// Operation between two decimals leads to Decimal(P, S), where
///     P is one of (9, 18, 38); equals to the maximum precision for the biggest underlying type of operands.
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

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const override;
    void deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const override;

    bool equals(const IDataType & rhs) const override;

    T parseFromString(const String & str) const;
    void readText(T & x, ReadBuffer & istr, bool csv = false) const { readText(x, istr, this->precision, this->scale, csv); }

    static void readText(T & x, ReadBuffer & istr, UInt32 precision_, UInt32 scale_, bool csv = false);
    static bool tryReadText(T & x, ReadBuffer & istr, UInt32 precision_, UInt32 scale_);
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
    return default_value;
}

template <typename T>
inline UInt32 getDecimalScale(const DataTypeDecimal<T> & data_type)
{
    return data_type.getScale();
}

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>, typename ToDataType::FieldType>
convertDecimals(const typename FromDataType::FieldType & value, UInt32 scale_from, UInt32 scale_to)
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using MaxFieldType = std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)), FromFieldType, ToFieldType>;
    using MaxNativeType = typename MaxFieldType::NativeType;

    MaxNativeType converted_value;
    if (scale_to > scale_from)
    {
        converted_value = MaxFieldType::getScaleMultiplier(scale_to - scale_from);
        if (common::mulOverflow(static_cast<MaxNativeType>(value), converted_value, converted_value))
            throw Exception(std::string(ToDataType::family_name) + " convert overflow",
                            ErrorCodes::DECIMAL_OVERFLOW);
    }
    else
        converted_value = value / MaxFieldType::getScaleMultiplier(scale_from - scale_to);

    if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType))
    {
        if (converted_value < std::numeric_limits<typename ToFieldType::NativeType>::min() ||
            converted_value > std::numeric_limits<typename ToFieldType::NativeType>::max())
            throw Exception(std::string(ToDataType::family_name) + " convert overflow",
                            ErrorCodes::DECIMAL_OVERFLOW);
    }

    return converted_value;
}

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsNumber<typename ToDataType::FieldType>, typename ToDataType::FieldType>
convertFromDecimal(const typename FromDataType::FieldType & value, UInt32 scale)
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    if constexpr (std::is_floating_point_v<ToFieldType>)
        return static_cast<ToFieldType>(value) / FromFieldType::getScaleMultiplier(scale);
    else
    {
        FromFieldType converted_value = convertDecimals<FromDataType, FromDataType>(value, scale, 0);

        if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType) || !std::numeric_limits<ToFieldType>::is_signed)
        {
            if constexpr (std::numeric_limits<ToFieldType>::is_signed)
            {
                if (converted_value < std::numeric_limits<ToFieldType>::min() ||
                    converted_value > std::numeric_limits<ToFieldType>::max())
                    throw Exception(std::string(FromDataType::family_name) + " convert overflow",
                                    ErrorCodes::DECIMAL_OVERFLOW);
            }
            else
            {
                using CastIntType = std::conditional_t<std::is_same_v<ToFieldType, UInt64>, Int128, Int64>;

                if (converted_value < 0 ||
                    converted_value > static_cast<CastIntType>(std::numeric_limits<ToFieldType>::max()))
                    throw Exception(std::string(FromDataType::family_name) + " convert overflow",
                                    ErrorCodes::DECIMAL_OVERFLOW);
            }
        }
        return converted_value;
    }
}

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsNumber<typename FromDataType::FieldType> && IsDataTypeDecimal<ToDataType>, typename ToDataType::FieldType>
convertToDecimal(const typename FromDataType::FieldType & value, UInt32 scale)
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using ToNativeType = typename ToFieldType::NativeType;

    if constexpr (std::is_floating_point_v<FromFieldType>)
    {
        if (!std::isfinite(value))
            throw Exception(std::string(ToDataType::family_name) + " convert overflow. Cannot convert infinity or NaN to decimal",
                            ErrorCodes::DECIMAL_OVERFLOW);

        auto out = value * ToFieldType::getScaleMultiplier(scale);
        if constexpr (std::is_same_v<ToNativeType, Int128>)
        {
            static constexpr __int128 min_int128 = __int128(0x8000000000000000ll) << 64;
            static constexpr __int128 max_int128 = (__int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;
            if (out <= static_cast<ToNativeType>(min_int128) || out >= static_cast<ToNativeType>(max_int128))
                throw Exception(std::string(ToDataType::family_name) + " convert overflow. Float is out of Decimal range",
                                ErrorCodes::DECIMAL_OVERFLOW);
        }
        else
        {
            if (out <= std::numeric_limits<ToNativeType>::min() || out >= std::numeric_limits<ToNativeType>::max())
                throw Exception(std::string(ToDataType::family_name) + " convert overflow. Float is out of Decimal range",
                                ErrorCodes::DECIMAL_OVERFLOW);
        }
        return out;
    }
    else
    {
        if constexpr (std::is_same_v<FromFieldType, UInt64>)
            if (value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                return convertDecimals<DataTypeDecimal<Decimal128>, ToDataType>(value, 0, scale);

        return convertDecimals<DataTypeDecimal<Decimal64>, ToDataType>(value, 0, scale);
    }
}

}
