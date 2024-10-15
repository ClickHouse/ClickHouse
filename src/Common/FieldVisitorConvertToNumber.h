#pragma once

#include <Common/FieldVisitors.h>
#include <Common/NaNUtils.h>
#include <base/demangle.h>
#include <type_traits>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONVERT_TYPE;
}


/** Converts numeric value of any type to specified type. */
template <typename T>
class FieldVisitorConvertToNumber : public StaticVisitor<T>
{
public:
    T operator() (const Null &) const
    {
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert NULL to {}", demangle(typeid(T).name()));
    }

    T operator() (const String &) const
    {
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert String to {}", demangle(typeid(T).name()));
    }

    T operator() (const Array &) const
    {
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert Array to {}", demangle(typeid(T).name()));
    }

    T operator() (const Tuple &) const
    {
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert Tuple to {}", demangle(typeid(T).name()));
    }

    T operator() (const Map &) const
    {
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert Map to {}", demangle(typeid(T).name()));
    }

    T operator() (const Object &) const
    {
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert Object to {}", demangle(typeid(T).name()));
    }

    T operator() (const UInt64 & x) const { return T(x); }
    T operator() (const Int64 & x) const { return T(x); }
    T operator() (const UUID & x) const { return T(x.toUnderType()); }
    T operator() (const IPv4 & x) const { return T(x.toUnderType()); }
    T operator() (const IPv6 & x) const { return T(x.toUnderType()); }

    T operator() (const Float64 & x) const
    {
        if constexpr (!std::is_floating_point_v<T>)
        {
            if (!isFinite(x))
            {
                /// When converting to bool it's ok (non-zero converts to true, NaN including).
                if (std::is_same_v<T, bool>)
                    return true;

                /// Conversion of infinite values to integer is undefined.
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert infinite value to integer type");
            }
            if (x > Float64(std::numeric_limits<T>::max()) || x < Float64(std::numeric_limits<T>::lowest()))
            {
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert out of range floating point value to integer type");
            }
        }

        if constexpr (std::is_same_v<Decimal256, T>)
        {
            return Int256(x);
        }
        else
        {
            return T(x);
        }
    }

    template <typename U>
    T operator() (const DecimalField<U> & x) const
    {
        if constexpr (std::is_floating_point_v<T>)
            return x.getValue().template convertTo<T>() / x.getScaleMultiplier().template convertTo<T>();
        else
            return (x.getValue() / x.getScaleMultiplier()).template convertTo<T>();
    }

    T operator() (const AggregateFunctionStateData &) const
    {
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert AggregateFunctionStateData to {}", demangle(typeid(T).name()));
    }

    T operator() (const CustomType &) const
    {
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert CustomType to {}", demangle(typeid(T).name()));
    }

    template <typename U>
    requires is_big_int_v<U>
    T operator() (const U & x) const
    {
        if constexpr (is_decimal<T>)
            return static_cast<T>(static_cast<typename T::NativeType>(x));
        else
            return static_cast<T>(x);
    }

    T operator() (const bool & x) const { return T(x); }
};

extern template class FieldVisitorConvertToNumber<Int8>;
extern template class FieldVisitorConvertToNumber<UInt8>;
extern template class FieldVisitorConvertToNumber<Int16>;
extern template class FieldVisitorConvertToNumber<UInt16>;
extern template class FieldVisitorConvertToNumber<Int32>;
extern template class FieldVisitorConvertToNumber<UInt32>;
extern template class FieldVisitorConvertToNumber<Int64>;
extern template class FieldVisitorConvertToNumber<UInt64>;
extern template class FieldVisitorConvertToNumber<Int128>;
extern template class FieldVisitorConvertToNumber<UInt128>;
extern template class FieldVisitorConvertToNumber<Int256>;
extern template class FieldVisitorConvertToNumber<UInt256>;
extern template class FieldVisitorConvertToNumber<Float32>;
extern template class FieldVisitorConvertToNumber<Float64>;

}
