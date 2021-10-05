#pragma once

#include <base/arithmeticOverflow.h>
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
template <Decimal T>
class DataTypeDecimal final : public DataTypeDecimalBase<T>
{
    using Base = DataTypeDecimalBase<T>;

public:
    using typename Base::FieldType;
    using typename Base::ColumnType;
    using Base::Base;

    static constexpr std::string_view family_name = "Decimal";

    const char * getFamilyName() const override { return family_name.data(); }
    std::string doGetName() const override { return fmt::to_string(*this); }
    TypeIndex getTypeId() const override { return TypeId<T>; }
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

class DataTypeDateTime64;

namespace dt
{
template <class T> concept DecimalStrict = is_any<T,
    DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimal128, DataTypeDecimal256>;

template <class T> concept Decimal = DecimalStrict<T> || std::is_same_v<T, DataTypeDateTime64>;

template <class T> concept DecimalOrArithmetic = Decimal<T> || Arithmetic<T>;
template <class T> concept DecimalStrictOrArithmetic = DecimalStrict<T> || Arithmetic<T>;
}

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

inline UInt32 getDecimalScale(const dt::Decimal auto & data_type)
{
    return data_type.getScale();
}

namespace detail
{
template <class From, class To, class Ret>
inline Ret convertDecimals(const FieldType<From>& value, UInt32 scale_from, UInt32 scale_to, FieldType<To> & result)
{
    using FromFieldType = FieldType<From>;
    using ToFieldType = FieldType<To>;

    constexpr size_t from_field_size = sizeof(FromFieldType);
    constexpr size_t to_field_size = sizeof(ToFieldType);
    constexpr bool throw_exception = std::is_void_v<Ret>;

    using MaxFieldType = std::conditional_t<(from_field_size > to_field_size), FromFieldType, ToFieldType>;
    using MaxNativeType = typename MaxFieldType::NativeType;

    MaxNativeType converted_value;

    if (scale_to > scale_from)
    {
        converted_value = DecimalUtils::scaleMultiplier<MaxNativeType>(scale_to - scale_from);

        if (common::mulOverflow(static_cast<MaxNativeType>(value.value), converted_value, converted_value))
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW,
                    "{} convert overflow", To::family_name);
            else
                return false;
        }
    }
    else
        converted_value = value.value / DecimalUtils::scaleMultiplier<MaxNativeType>(scale_from - scale_to);

    using ToNative = typename ToFieldType::NativeType;
    using Limits = std::numeric_limits<ToNative>;

    if constexpr (from_field_size > to_field_size)
    {
        if (converted_value < Limits::min() || converted_value > Limits::max())
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW,
                    "{} convert overflow", To::family_name);
            else
                return false;
        }
    }

    result = static_cast<ToNative>(converted_value);

    if constexpr (!throw_exception)
        return true;
}

template <class From, class To, class Ret>
inline Ret convertFromDecimal(const FieldType<From> & value, UInt32 scale, FieldType<To> & result)
{
    return DecimalUtils::convertToImpl<FieldType<To>, FieldType<From>, Ret>(value, scale, result);
}

template <class From, class To, class Ret>
inline Ret convertToDecimal(const FieldType<From> & value, UInt32 scale, FieldType<To> & result)
{
    using FromField = FieldType<From>;
    using ToField = FieldType<To>;
    using ToNative = typename ToField::NativeType;

    constexpr bool throw_exception = std::is_void_v<Ret>;

    if constexpr (Float<FromField>)
    {
        if (!std::isfinite(value))
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW,
                    "{} convert overflow. Cannot convert infinity or NaN to decimal",
                    To::family_name);
            else
                return false;
        }

        using Limits = std::numeric_limits<ToNative>;

        auto out = value * static_cast<FromField>(DecimalUtils::scaleMultiplier<ToNative>(scale));

        if (out <= static_cast<FromField>(Limits::min()) || out >= static_cast<FromField>(Limits::max()))
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW,
                    "{} convert overflow. Float is out of Decimal range",
                    To::family_name);
            else
                return false;
        }

        result = static_cast<ToNative>(out);

        if constexpr (throw_exception)
            return;
        else
            return true;
    }
    else if constexpr (ExtIntegral<FromField>)
        return convertDecimals<DataTypeDecimal256, To, Ret>(static_cast<Int256>(value), 0, scale, result);
    else if constexpr (std::is_same_v<FromField, UInt64>)
        return convertDecimals<DataTypeDecimal128, To, Ret>(static_cast<Int128>(value), 0, scale, result);
    else
        return convertDecimals<DataTypeDecimal64, To, Ret>(static_cast<Int64>(value), 0, scale, result);
}
}

template <dt::Decimal From, dt::Decimal To>
inline FieldType<To> convertDecimals(const FieldType<From> & value, UInt32 scale_from, UInt32 scale_to)
{
    FieldType<To> result;
    detail::convertDecimals<From, To, void>(value, scale_from, scale_to, result);
    return result;
}

template <dt::Decimal From, dt::Decimal To>
inline bool tryConvertDecimals(const FieldType<From> & value, UInt32 scale_from, UInt32 scale_to, FieldType<To> & result)
{
    return detail::convertDecimals<From, To, bool>(value, scale_from, scale_to, result);
}

template <dt::Decimal From, dt::Arithmetic To>
inline FieldType<To> convertFromDecimal(const FieldType<From> & value, UInt32 scale)
{
    FieldType<To> result;
    detail::convertFromDecimal<From, To, void>(value, scale, result);
    return result;
}

template <dt::Decimal From, dt::HasArithmeticField To>
inline bool tryConvertFromDecimal(const FieldType<From> & value, UInt32 scale, FieldType<To> & result)
{
    return detail::convertFromDecimal<From, To, bool>(value, scale, result);
}

template <dt::HasArithmeticField From, dt::Decimal To>
inline FieldType<To> convertToDecimal(const FieldType<From> & value, UInt32 scale)
{
    FieldType<To> result;
    detail::convertToDecimal<From, To, void>(value, scale, result);
    return result;
}

template <dt::HasArithmeticField From, dt::Decimal To>
inline bool tryConvertToDecimal(const FieldType<From> & value, UInt32 scale, FieldType<To> & result)
{
    return detail::convertToDecimal<From, To, bool>(value, scale, result);
}

template <typename T>
inline DataTypePtr createDecimalMaxPrecision(UInt64 scale)
{
    return std::make_shared<DataTypeDecimal<T>>(DecimalUtils::max_precision<T>, scale);
}

}

template <DB::dt::Decimal T>
struct fmt::formatter<T> : fmt::formatter<std::string_view>
{
    constexpr auto format(const T & p, auto & ctx) -> decltype(ctx.out())
    {
        return format_to(ctx.out(), "Decimal({}, {})", p.getPrecision(), p.getScale());
    }
};
