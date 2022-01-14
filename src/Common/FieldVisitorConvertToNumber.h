#pragma once

#include <Common/FieldVisitors.h>
#include <Common/NaNUtils.h>
#include <common/demangle.h>
#include <type_traits>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONVERT_TYPE;
    extern const int NOT_IMPLEMENTED;
}


/** Converts numeric value of any type to specified type. */
template <typename T>
class FieldVisitorConvertToNumber : public StaticVisitor<T>
{
public:
    T operator() (const Null &) const
    {
        throw Exception("Cannot convert NULL to " + demangle(typeid(T).name()), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    T operator() (const String &) const
    {
        throw Exception("Cannot convert String to " + demangle(typeid(T).name()), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    T operator() (const Array &) const
    {
        throw Exception("Cannot convert Array to " + demangle(typeid(T).name()), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    T operator() (const Tuple &) const
    {
        throw Exception("Cannot convert Tuple to " + demangle(typeid(T).name()), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    T operator() (const Map &) const
    {
        throw Exception("Cannot convert Map to " + demangle(typeid(T).name()), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    T operator() (const UInt64 & x) const { return T(x); }
    T operator() (const Int64 & x) const { return T(x); }
    T operator() (const Int128 & x) const { return T(x); }
    T operator() (const UUID & x) const { return T(x.toUnderType()); }

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
                throw Exception("Cannot convert infinite value to integer type", ErrorCodes::CANNOT_CONVERT_TYPE);
            }
            else if (x > std::numeric_limits<T>::max() || x < std::numeric_limits<T>::lowest())
            {
                throw Exception("Cannot convert out of range floating point value to integer type", ErrorCodes::CANNOT_CONVERT_TYPE);
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

    T operator() (const UInt128 &) const
    {
        throw Exception("Cannot convert UInt128 to " + demangle(typeid(T).name()), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    template <typename U>
    T operator() (const DecimalField<U> & x) const
    {
        if constexpr (std::is_floating_point_v<T>)
            return x.getValue(). template convertTo<T>() / x.getScaleMultiplier(). template convertTo<T>();
        else if constexpr (std::is_same_v<T, UInt128>)
        {
            if constexpr (sizeof(U) < 16)
            {
                return UInt128(0, (x.getValue() / x.getScaleMultiplier()).value);
            }
            else if constexpr (sizeof(U) == 16)
            {
                auto tmp = (x.getValue() / x.getScaleMultiplier()).value;
                return UInt128(tmp >> 64, UInt64(tmp));
            }
            else
                throw Exception("No conversion to old UInt128 from " + demangle(typeid(U).name()), ErrorCodes::NOT_IMPLEMENTED);
        }
        else
            return (x.getValue() / x.getScaleMultiplier()). template convertTo<T>();
    }

    T operator() (const AggregateFunctionStateData &) const
    {
        throw Exception("Cannot convert AggregateFunctionStateData to " + demangle(typeid(T).name()), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    template <typename U, typename = std::enable_if_t<is_big_int_v<U>> >
    T operator() (const U & x) const
    {
        if constexpr (IsDecimalNumber<T>)
            return static_cast<T>(static_cast<typename T::NativeType>(x));
        else if constexpr (std::is_same_v<T, UInt128>)
            throw Exception("No conversion to old UInt128 from " + demangle(typeid(U).name()), ErrorCodes::NOT_IMPLEMENTED);
        else
            return static_cast<T>(x);
    }
};

}

