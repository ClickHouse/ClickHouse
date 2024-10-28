#pragma once

#include <cassert>
#include <vector>
#include <algorithm>
#include <map>
#include <type_traits>
#include <functional>

#include <Core/CompareHelper.h>
#include <Core/Defines.h>
#include <Core/Types.h>
#include <base/DayNum.h>
#include <base/IPv4andIPv6.h>
#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int BAD_GET;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

constexpr Null NEGATIVE_INFINITY{Null::Value::NegativeInfinity};
constexpr Null POSITIVE_INFINITY{Null::Value::PositiveInfinity};

class Field;
using FieldVector = std::vector<Field, AllocatorWithMemoryTracking<Field>>;

/// Array and Tuple use the same storage type -- FieldVector, but we declare
/// distinct types for them, so that the caller can choose whether it wants to
/// construct a Field of Array or a Tuple type. An alternative approach would be
/// to construct both of these types from FieldVector, and have the caller
/// specify the desired Field type explicitly.
/// NOLINTBEGIN(modernize-type-traits)
#define DEFINE_FIELD_VECTOR(X) \
struct X : public FieldVector \
{ \
    using FieldVector::FieldVector; \
}

DEFINE_FIELD_VECTOR(Array);
DEFINE_FIELD_VECTOR(Tuple);
/// NOLINTEND(modernize-type-traits)

/// An array with the following structure: [(key1, value1), (key2, value2), ...]
DEFINE_FIELD_VECTOR(Map); /// TODO: use map instead of vector.

#undef DEFINE_FIELD_VECTOR

using FieldMap = std::map<String, Field, std::less<>, AllocatorWithMemoryTracking<std::pair<const String, Field>>>;

#define DEFINE_FIELD_MAP(X) \
struct X : public FieldMap \
{ \
    using FieldMap::FieldMap; \
}

DEFINE_FIELD_MAP(Object);

#undef DEFINE_FIELD_MAP

struct AggregateFunctionStateData
{
    String name; /// Name with arguments.
    String data;

    bool operator < (const AggregateFunctionStateData &) const
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Operator < is not implemented for AggregateFunctionStateData.");
    }

    bool operator <= (const AggregateFunctionStateData &) const
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Operator <= is not implemented for AggregateFunctionStateData.");
    }

    bool operator > (const AggregateFunctionStateData &) const
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Operator > is not implemented for AggregateFunctionStateData.");
    }

    bool operator >= (const AggregateFunctionStateData &) const
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Operator >= is not implemented for AggregateFunctionStateData.");
    }

    bool operator == (const AggregateFunctionStateData & rhs) const
    {
        if (name != rhs.name)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Comparing aggregate functions with different types: {} and {}",
                    name, rhs.name);

        return data == rhs.data;
    }
};

struct CustomType
{
    struct CustomTypeImpl
    {
        virtual ~CustomTypeImpl() = default;
        virtual const char * getTypeName() const = 0;
        virtual String toString(bool show_secrets) const = 0;
        virtual bool isSecret() const = 0;

        virtual bool operator < (const CustomTypeImpl &) const = 0;
        virtual bool operator <= (const CustomTypeImpl &) const = 0;
        virtual bool operator > (const CustomTypeImpl &) const = 0;
        virtual bool operator >= (const CustomTypeImpl &) const = 0;
        virtual bool operator == (const CustomTypeImpl &) const = 0;
    };

    CustomType() = default;
    explicit CustomType(std::shared_ptr<const CustomTypeImpl> impl_) : impl(impl_) {}

    bool isSecret() const { return impl->isSecret(); }
    const char * getTypeName() const { return impl->getTypeName(); }
    String toString(bool show_secrets = true) const { return impl->toString(show_secrets); }
    const CustomTypeImpl & getImpl() const { return *impl; }

    bool operator < (const CustomType & rhs) const { return *impl < *rhs.impl; }
    bool operator <= (const CustomType & rhs) const { return *impl <= *rhs.impl; }
    bool operator > (const CustomType & rhs) const { return *impl > *rhs.impl; }
    bool operator >= (const CustomType & rhs) const { return *impl >= *rhs.impl; }
    bool operator == (const CustomType & rhs) const { return *impl == *rhs.impl; }

    std::shared_ptr<const CustomTypeImpl> impl;
};

template <typename T> bool decimalEqual(T x, T y, UInt32 x_scale, UInt32 y_scale);
template <typename T> bool decimalLess(T x, T y, UInt32 x_scale, UInt32 y_scale);
template <typename T> bool decimalLessOrEqual(T x, T y, UInt32 x_scale, UInt32 y_scale);

template <is_decimal T>
class DecimalField
{
public:
    explicit DecimalField(T value = {}, UInt32 scale_ = 0)
    :   dec(value),
        scale(scale_)
    {}

    operator T() const { return dec; } /// NOLINT
    T getValue() const { return dec; }
    T getScaleMultiplier() const;
    UInt32 getScale() const { return scale; }

    template <typename U>
    bool operator < (const DecimalField<U> & r) const
    {
        using MaxType = std::conditional_t<(sizeof(T) > sizeof(U)), T, U>;
        return decimalLess<MaxType>(dec, r.getValue(), scale, r.getScale());
    }

    template <typename U>
    bool operator <= (const DecimalField<U> & r) const
    {
        using MaxType = std::conditional_t<(sizeof(T) > sizeof(U)), T, U>;
        return decimalLessOrEqual<MaxType>(dec, r.getValue(), scale, r.getScale());
    }

    template <typename U>
    bool operator == (const DecimalField<U> & r) const
    {
        using MaxType = std::conditional_t<(sizeof(T) > sizeof(U)), T, U>;
        return decimalEqual<MaxType>(dec, r.getValue(), scale, r.getScale());
    }

    template <typename U> bool operator > (const DecimalField<U> & r) const { return r < *this; }
    template <typename U> bool operator >= (const DecimalField<U> & r) const { return r <= * this; }
    template <typename U> bool operator != (const DecimalField<U> & r) const { return !(*this == r); }

    const DecimalField<T> & operator += (const DecimalField<T> & r)
    {
        if (scale != r.getScale())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Add different decimal fields");
        dec += r.getValue();
        return *this;
    }

    const DecimalField<T> & operator -= (const DecimalField<T> & r)
    {
        if (scale != r.getScale())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Sub different decimal fields");
        dec -= r.getValue();
        return *this;
    }

private:
    T dec;
    UInt32 scale;
};

extern template class DecimalField<Decimal32>;
extern template class DecimalField<Decimal64>;
extern template class DecimalField<Decimal128>;
extern template class DecimalField<Decimal256>;
extern template class DecimalField<DateTime64>;

template <typename T> constexpr bool is_decimal_field = false;
template <> constexpr inline bool is_decimal_field<DecimalField<Decimal32>> = true;
template <> constexpr inline bool is_decimal_field<DecimalField<Decimal64>> = true;
template <> constexpr inline bool is_decimal_field<DecimalField<Decimal128>> = true;
template <> constexpr inline bool is_decimal_field<DecimalField<Decimal256>> = true;

template <typename T, typename SFINAE = void>
struct NearestFieldTypeImpl;

template <typename T>
using NearestFieldType = typename NearestFieldTypeImpl<T>::Type;

/// char may be signed or unsigned, and behave identically to signed char or unsigned char,
///  but they are always three different types.
/// signedness of char is different in Linux on x86 and Linux on ARM.
template <> struct NearestFieldTypeImpl<char> { using Type = std::conditional_t<is_signed_v<char>, Int64, UInt64>; };
template <> struct NearestFieldTypeImpl<signed char> { using Type = Int64; };
template <> struct NearestFieldTypeImpl<unsigned char> { using Type = UInt64; };
template <> struct NearestFieldTypeImpl<char8_t> { using Type = UInt64; };
template <> struct NearestFieldTypeImpl<Int8> { using Type = Int64; };

template <> struct NearestFieldTypeImpl<UInt16> { using Type = UInt64; };
template <> struct NearestFieldTypeImpl<UInt32> { using Type = UInt64; };

template <> struct NearestFieldTypeImpl<DayNum> { using Type = UInt64; };
template <> struct NearestFieldTypeImpl<UUID> { using Type = UUID; };
template <> struct NearestFieldTypeImpl<IPv4> { using Type = IPv4; };
template <> struct NearestFieldTypeImpl<IPv6> { using Type = IPv6; };
template <> struct NearestFieldTypeImpl<Int16> { using Type = Int64; };
template <> struct NearestFieldTypeImpl<Int32> { using Type = Int64; };

/// long and long long are always different types that may behave identically or not.
/// This is different on Linux and Mac.
template <> struct NearestFieldTypeImpl<long> { using Type = Int64; }; /// NOLINT
template <> struct NearestFieldTypeImpl<long long> { using Type = Int64; }; /// NOLINT
template <> struct NearestFieldTypeImpl<unsigned long> { using Type = UInt64; }; /// NOLINT
template <> struct NearestFieldTypeImpl<unsigned long long> { using Type = UInt64; }; /// NOLINT

template <> struct NearestFieldTypeImpl<UInt256> { using Type = UInt256; };
template <> struct NearestFieldTypeImpl<Int256> { using Type = Int256; };
template <> struct NearestFieldTypeImpl<UInt128> { using Type = UInt128; };
template <> struct NearestFieldTypeImpl<Int128> { using Type = Int128; };

template <> struct NearestFieldTypeImpl<Decimal32> { using Type = DecimalField<Decimal32>; };
template <> struct NearestFieldTypeImpl<Decimal64> { using Type = DecimalField<Decimal64>; };
template <> struct NearestFieldTypeImpl<Decimal128> { using Type = DecimalField<Decimal128>; };
template <> struct NearestFieldTypeImpl<Decimal256> { using Type = DecimalField<Decimal256>; };
template <> struct NearestFieldTypeImpl<DateTime64> { using Type = DecimalField<DateTime64>; };
template <> struct NearestFieldTypeImpl<DecimalField<Decimal32>> { using Type = DecimalField<Decimal32>; };
template <> struct NearestFieldTypeImpl<DecimalField<Decimal64>> { using Type = DecimalField<Decimal64>; };
template <> struct NearestFieldTypeImpl<DecimalField<Decimal128>> { using Type = DecimalField<Decimal128>; };
template <> struct NearestFieldTypeImpl<DecimalField<Decimal256>> { using Type = DecimalField<Decimal256>; };
template <> struct NearestFieldTypeImpl<DecimalField<DateTime64>> { using Type = DecimalField<DateTime64>; };
template <> struct NearestFieldTypeImpl<Float32> { using Type = Float64; };
template <> struct NearestFieldTypeImpl<Float64> { using Type = Float64; };
template <> struct NearestFieldTypeImpl<const char *> { using Type = String; };
template <> struct NearestFieldTypeImpl<std::string_view> { using Type = String; };
template <> struct NearestFieldTypeImpl<String> { using Type = String; };
template <> struct NearestFieldTypeImpl<Array> { using Type = Array; };
template <> struct NearestFieldTypeImpl<Tuple> { using Type = Tuple; };
template <> struct NearestFieldTypeImpl<Map> { using Type = Map; };
template <> struct NearestFieldTypeImpl<Object> { using Type = Object; };
template <> struct NearestFieldTypeImpl<bool> { using Type = UInt64; };
template <> struct NearestFieldTypeImpl<Null> { using Type = Null; };

template <> struct NearestFieldTypeImpl<AggregateFunctionStateData> { using Type = AggregateFunctionStateData; };
template <> struct NearestFieldTypeImpl<CustomType> { using Type = CustomType; };

// For enum types, use the field type that corresponds to their underlying type.
template <typename T>
requires std::is_enum_v<T>
struct NearestFieldTypeImpl<T>
{
    using Type = NearestFieldType<std::underlying_type_t<T>>;
};

template <typename T>
decltype(auto) castToNearestFieldType(T && x)
{
    using U = NearestFieldType<std::decay_t<T>>;
    if constexpr (std::is_same_v<std::decay_t<T>, U>)
        return std::forward<T>(x);
    else
        return U(x);
}

template <typename T>
concept not_field_or_bool_or_stringlike
    = (!std::is_same_v<std::decay_t<T>, Field> && !std::is_same_v<std::decay_t<T>, bool>
       && !std::is_same_v<NearestFieldType<std::decay_t<T>>, String>);

/** 32 is enough. Round number is used for alignment and for better arithmetic inside std::vector.
  * NOTE: Actually, sizeof(std::string) is 32 when using libc++, so Field is 40 bytes.
  */
static constexpr auto DBMS_MIN_FIELD_SIZE = 32;


/** Discriminated union of several types.
  * Made for replacement of `boost::variant`
  *  is not generalized,
  *  but somewhat more efficient, and simpler.
  *
  * Used to represent a single value of one of several types in memory.
  * Warning! Prefer to use chunks of columns instead of single values. See IColumn.h
  */
class Field
{
public:
    struct Types
    {
        /// Type tag.
        enum Which
        {
            Null    = 0,
            UInt64  = 1,
            Int64   = 2,
            Float64 = 3,
            UInt128 = 4,
            Int128  = 5,

            String  = 16,
            Array   = 17,
            Tuple   = 18,
            Decimal32  = 19,
            Decimal64  = 20,
            Decimal128 = 21,
            AggregateFunctionState = 22,
            Decimal256 = 23,
            UInt256 = 24,
            Int256  = 25,
            Map = 26,
            UUID = 27,
            Bool = 28,
            Object = 29,
            IPv4 = 30,
            IPv6 = 31,
            CustomType = 32,
        };
    };


    /// Returns an identifier for the type or vice versa.
    template <typename T> struct TypeToEnum;
    template <Types::Which which> struct EnumToType;

    static bool isDecimal(Types::Which which)
    {
        return which == Types::Decimal32
            || which == Types::Decimal64
            || which == Types::Decimal128
            || which == Types::Decimal256;
    }

    Field() : Field(Null{}) {}

    /** Despite the presence of a template constructor, this constructor is still needed,
      *  since, in its absence, the compiler will still generate the default constructor.
      */
    Field(const Field & rhs)
    {
        create(rhs);
    }

    Field(Field && rhs) noexcept
    {
        create(std::move(rhs));
    }

    template <typename T>
    requires not_field_or_bool_or_stringlike<T>
    Field(T && rhs); /// NOLINT

    Field(bool rhs) : Field(castToNearestFieldType(rhs)) /// NOLINT
    {
        which = Types::Bool;
    }

    /// Create a string inplace.
    Field(std::string_view str) { create(str.data(), str.size()); } /// NOLINT
    Field(const String & str) { create(std::string_view{str}); } /// NOLINT
    Field(String && str) { create(std::move(str)); } /// NOLINT
    Field(const char * str) { create(std::string_view{str}); } /// NOLINT

    template <typename CharT>
    Field(const CharT * data, size_t size)
    {
        create(data, size);
    }

    Field & operator= (const Field & rhs)
    {
        if (this != &rhs)
        {
            if (which != rhs.which)
            {
                destroy();
                create(rhs);
            }
            else
                assign(rhs);    /// This assigns string or vector without deallocation of existing buffer.
        }
        return *this;
    }

    Field & operator= (Field && rhs) noexcept
    {
        if (this != &rhs)
        {
            if (which != rhs.which)
            {
                destroy();
                create(std::move(rhs));
            }
            else
                assign(std::move(rhs));
        }
        return *this;
    }

    /// Allows expressions like
    /// Field f = 1;
    /// Things to note:
    /// 1. float <--> int needs explicit cast
    /// 2. customized types needs explicit cast
    template <typename T>
    requires not_field_or_bool_or_stringlike<T>
    Field & /// NOLINT
    operator=(T && rhs);

    Field & operator= (bool rhs)
    {
        *this = castToNearestFieldType(rhs);
        which = Types::Bool;
        return *this;
    }

    Field & operator= (std::string_view str);
    Field & operator= (const String & str) { return *this = std::string_view{str}; }
    Field & operator= (String && str);
    Field & operator= (const char * str) { return *this = std::string_view{str}; }

    ~Field()
    {
        destroy();
    }


    Types::Which getType() const { return which; }

    std::string_view getTypeName() const;

    bool isNull() const { return which == Types::Null; }

    bool isNegativeInfinity() const { return which == Types::Null && get<Null>().isNegativeInfinity(); }
    bool isPositiveInfinity() const { return which == Types::Null && get<Null>().isPositiveInfinity(); }

    template <typename T> bool tryGet(T & result)
    {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        if (which != requested)
            return false;
        result = get<T>();
        return true;
    }

    template <typename T> bool tryGet(T & result) const
    {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        if (which != requested)
            return false;
        result = get<T>();
        return true;
    }

    template <typename T> auto & safeGet() const
    {
        return const_cast<Field *>(this)->safeGet<T>();
    }

    template <typename T> auto & safeGet();

    bool operator< (const Field & rhs) const
    {
        if (which < rhs.which)
            return true;
        if (which > rhs.which)
            return false;

        switch (which)
        {
            case Types::Null:    return get<Null>() < rhs.get<Null>();
            case Types::Bool:    [[fallthrough]];
            case Types::UInt64:  return get<UInt64>()  < rhs.get<UInt64>();
            case Types::UInt128: return get<UInt128>() < rhs.get<UInt128>();
            case Types::UInt256: return get<UInt256>() < rhs.get<UInt256>();
            case Types::Int64:   return get<Int64>()   < rhs.get<Int64>();
            case Types::Int128:  return get<Int128>()  < rhs.get<Int128>();
            case Types::Int256:  return get<Int256>()  < rhs.get<Int256>();
            case Types::UUID:    return get<UUID>()    < rhs.get<UUID>();
            case Types::IPv4:    return get<IPv4>()    < rhs.get<IPv4>();
            case Types::IPv6:    return get<IPv6>()    < rhs.get<IPv6>();
            case Types::Float64:
                static constexpr int nan_direction_hint = 1; /// Put NaN at the end
                return FloatCompareHelper<Float64>::less(get<Float64>(), rhs.get<Float64>(), nan_direction_hint);
            case Types::String:  return get<String>()  < rhs.get<String>();
            case Types::Array:   return get<Array>()   < rhs.get<Array>();
            case Types::Tuple:   return get<Tuple>()   < rhs.get<Tuple>();
            case Types::Map:     return get<Map>()     < rhs.get<Map>();
            case Types::Object:  return get<Object>()  < rhs.get<Object>();
            case Types::Decimal32:  return get<DecimalField<Decimal32>>()  < rhs.get<DecimalField<Decimal32>>();
            case Types::Decimal64:  return get<DecimalField<Decimal64>>()  < rhs.get<DecimalField<Decimal64>>();
            case Types::Decimal128: return get<DecimalField<Decimal128>>() < rhs.get<DecimalField<Decimal128>>();
            case Types::Decimal256: return get<DecimalField<Decimal256>>() < rhs.get<DecimalField<Decimal256>>();
            case Types::AggregateFunctionState:  return get<AggregateFunctionStateData>() < rhs.get<AggregateFunctionStateData>();
            case Types::CustomType:  return get<CustomType>() < rhs.get<CustomType>();
        }

        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Bad type of Field");
    }

    bool operator> (const Field & rhs) const
    {
        return rhs < *this;
    }

    bool operator<= (const Field & rhs) const
    {
        if (which < rhs.which)
            return true;
        if (which > rhs.which)
            return false;

        switch (which)
        {
            case Types::Null:    return get<Null>() <= rhs.get<Null>();
            case Types::Bool: [[fallthrough]];
            case Types::UInt64:  return get<UInt64>()  <= rhs.get<UInt64>();
            case Types::UInt128: return get<UInt128>() <= rhs.get<UInt128>();
            case Types::UInt256: return get<UInt256>() <= rhs.get<UInt256>();
            case Types::Int64:   return get<Int64>()   <= rhs.get<Int64>();
            case Types::Int128:  return get<Int128>()  <= rhs.get<Int128>();
            case Types::Int256:  return get<Int256>()  <= rhs.get<Int256>();
            case Types::UUID:    return get<UUID>().toUnderType() <= rhs.get<UUID>().toUnderType();
            case Types::IPv4:    return get<IPv4>()    <= rhs.get<IPv4>();
            case Types::IPv6:    return get<IPv6>()    <= rhs.get<IPv6>();
            case Types::Float64:
            {
                static constexpr int nan_direction_hint = 1; /// Put NaN at the end
                Float64 f1 = get<Float64>();
                Float64 f2 = get<Float64>();
                return FloatCompareHelper<Float64>::less(f1, f2, nan_direction_hint)
                    || FloatCompareHelper<Float64>::equals(f1, f2, nan_direction_hint);
            }
            case Types::String:  return get<String>()  <= rhs.get<String>();
            case Types::Array:   return get<Array>()   <= rhs.get<Array>();
            case Types::Tuple:   return get<Tuple>()   <= rhs.get<Tuple>();
            case Types::Map:     return get<Map>()     <= rhs.get<Map>();
            case Types::Object:  return get<Object>()  <= rhs.get<Object>();
            case Types::Decimal32:  return get<DecimalField<Decimal32>>()  <= rhs.get<DecimalField<Decimal32>>();
            case Types::Decimal64:  return get<DecimalField<Decimal64>>()  <= rhs.get<DecimalField<Decimal64>>();
            case Types::Decimal128: return get<DecimalField<Decimal128>>() <= rhs.get<DecimalField<Decimal128>>();
            case Types::Decimal256: return get<DecimalField<Decimal256>>() <= rhs.get<DecimalField<Decimal256>>();
            case Types::AggregateFunctionState:  return get<AggregateFunctionStateData>() <= rhs.get<AggregateFunctionStateData>();
            case Types::CustomType:  return get<CustomType>() <= rhs.get<CustomType>();
        }

        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Bad type of Field");
    }

    bool operator>= (const Field & rhs) const
    {
        return rhs <= *this;
    }

    // More like bitwise equality as opposed to semantic equality:
    // Null equals Null and NaN equals NaN.
    bool operator== (const Field & rhs) const
    {
        if (which != rhs.which)
            return false;

        switch (which)
        {
            case Types::Null: return get<Null>() == rhs.get<Null>();
            case Types::Bool: [[fallthrough]];
            case Types::UInt64: return get<UInt64>() == rhs.get<UInt64>();
            case Types::Int64:   return get<Int64>() == rhs.get<Int64>();
            case Types::Float64:
                static constexpr int nan_direction_hint = 1; /// Put NaN at the end
                return FloatCompareHelper<Float64>::equals(get<Float64>(), rhs.get<Float64>(), nan_direction_hint);
            case Types::UUID:    return get<UUID>()    == rhs.get<UUID>();
            case Types::IPv4:    return get<IPv4>()    == rhs.get<IPv4>();
            case Types::IPv6:    return get<IPv6>()    == rhs.get<IPv6>();
            case Types::String:  return get<String>()  == rhs.get<String>();
            case Types::Array:   return get<Array>()   == rhs.get<Array>();
            case Types::Tuple:   return get<Tuple>()   == rhs.get<Tuple>();
            case Types::Map:     return get<Map>()     == rhs.get<Map>();
            case Types::Object:  return get<Object>()  == rhs.get<Object>();
            case Types::UInt128: return get<UInt128>() == rhs.get<UInt128>();
            case Types::UInt256: return get<UInt256>() == rhs.get<UInt256>();
            case Types::Int128:  return get<Int128>()  == rhs.get<Int128>();
            case Types::Int256:  return get<Int256>()  == rhs.get<Int256>();
            case Types::Decimal32:  return get<DecimalField<Decimal32>>()  == rhs.get<DecimalField<Decimal32>>();
            case Types::Decimal64:  return get<DecimalField<Decimal64>>()  == rhs.get<DecimalField<Decimal64>>();
            case Types::Decimal128: return get<DecimalField<Decimal128>>() == rhs.get<DecimalField<Decimal128>>();
            case Types::Decimal256: return get<DecimalField<Decimal256>>() == rhs.get<DecimalField<Decimal256>>();
            case Types::AggregateFunctionState:  return get<AggregateFunctionStateData>() == rhs.get<AggregateFunctionStateData>();
            case Types::CustomType:  return get<CustomType>() == rhs.get<CustomType>();
        }

        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Bad type of Field");
    }

    bool operator!= (const Field & rhs) const
    {
        return !(*this == rhs);
    }

    /// Field is template parameter, to allow universal reference for field,
    /// that is useful for const and non-const .
    template <typename F, typename FieldRef>
    static auto dispatch(F && f, FieldRef && field)
    {
        switch (field.which)
        {
            case Types::Null:    return f(field.template get<Null>());
            case Types::UInt64:  return f(field.template get<UInt64>());
            case Types::UInt128: return f(field.template get<UInt128>());
            case Types::UInt256: return f(field.template get<UInt256>());
            case Types::Int64:   return f(field.template get<Int64>());
            case Types::Int128:  return f(field.template get<Int128>());
            case Types::Int256:  return f(field.template get<Int256>());
            case Types::UUID:    return f(field.template get<UUID>());
            case Types::IPv4:    return f(field.template get<IPv4>());
            case Types::IPv6:    return f(field.template get<IPv6>());
            case Types::Float64: return f(field.template get<Float64>());
            case Types::String:  return f(field.template get<String>());
            case Types::Array:   return f(field.template get<Array>());
            case Types::Tuple:   return f(field.template get<Tuple>());
            case Types::Map:     return f(field.template get<Map>());
            case Types::Bool:
            {
                bool value = bool(field.template get<UInt64>());
                return f(value);
            }
            case Types::Object:     return f(field.template get<Object>());
            case Types::Decimal32:  return f(field.template get<DecimalField<Decimal32>>());
            case Types::Decimal64:  return f(field.template get<DecimalField<Decimal64>>());
            case Types::Decimal128: return f(field.template get<DecimalField<Decimal128>>());
            case Types::Decimal256: return f(field.template get<DecimalField<Decimal256>>());
            case Types::AggregateFunctionState: return f(field.template get<AggregateFunctionStateData>());
            case Types::CustomType: return f(field.template get<CustomType>());
        }
    }

    String dump() const;
    static Field restoreFromDump(std::string_view dump_);

private:
    std::aligned_union_t<DBMS_MIN_FIELD_SIZE - sizeof(Types::Which),
        Null, UInt64, UInt128, UInt256, Int64, Int128, Int256, UUID, IPv4, IPv6, Float64, String, Array, Tuple, Map,
        DecimalField<Decimal32>, DecimalField<Decimal64>, DecimalField<Decimal128>, DecimalField<Decimal256>,
        AggregateFunctionStateData, CustomType
        > storage;

    Types::Which which;

    /// This function is prone to type punning and should never be used outside of Field class,
    /// whenever it is used within this class the stored type should be checked in advance.
    template <typename T>
    NearestFieldType<std::decay_t<T>> & get()
    {
        // Before storing the value in the Field, we static_cast it to the field
        // storage type, so here we return the value of storage type as well.
        // Otherwise, it is easy to make a mistake of reinterpret_casting the stored
        // value to a different and incompatible type.
        // For example, a Float32 value is stored as Float64, and it is incorrect to
        // return a reference to this value as Float32.
        return *reinterpret_cast<NearestFieldType<std::decay_t<T>>*>(&storage);
    }

    template <typename T>
    NearestFieldType<std::decay_t<T>> & get() const
    {
        return const_cast<Field *>(this)->get<T>();
    }

    /// Assuming there was no allocated state or it was deallocated (see destroy).
    template <typename T>
    void createConcrete(T && x)
    {
        using UnqualifiedType = std::decay_t<T>;

        // In both Field and PODArray, small types may be stored as wider types,
        // e.g. char is stored as UInt64. Field can return this extended value
        // with get<StorageType>(). To avoid uninitialized results from get(),
        // we must initialize the entire wide stored type, and not just the
        // nominal type.
        using StorageType = NearestFieldType<UnqualifiedType>;
        new (&storage) StorageType(std::forward<T>(x));
        which = TypeToEnum<UnqualifiedType>::value;
    }

    /// Assuming same types.
    template <typename T>
    void assignConcrete(T && x)
    {
        using JustT = std::decay_t<T>;
        assert(which == TypeToEnum<JustT>::value);
        JustT * MAY_ALIAS ptr = reinterpret_cast<JustT *>(&storage);
        *ptr = std::forward<T>(x);
    }

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    void assignString(const CharT * data, size_t size)
    {
        assert(which == Types::String);
        String * ptr = reinterpret_cast<String *>(&storage);
        ptr->assign(reinterpret_cast<const char *>(data), size);
    }

    void assignString(String && str)
    {
        assert(which == Types::String);
        String * ptr = reinterpret_cast<String *>(&storage);
        ptr->assign(std::move(str));
    }

    void create(const Field & x)
    {
        dispatch([this] (auto & value) { createConcrete(value); }, x);
    }

    void create(Field && x)
    {
        dispatch([this] (auto & value) { createConcrete(std::move(value)); }, x);
    }

    void assign(const Field & x)
    {
        dispatch([this] (auto & value) { assignConcrete(value); }, x);
    }

    void assign(Field && x)
    {
        dispatch([this] (auto & value) { assignConcrete(std::move(value)); }, x);
    }

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    void create(const CharT * data, size_t size)
    {
        new (&storage) String(reinterpret_cast<const char *>(data), size);
        which = Types::String;
    }

    void create(String && str)
    {
        new (&storage) String(std::move(str));
        which = Types::String;
    }

    ALWAYS_INLINE void destroy()
    {
        switch (which)
        {
            case Types::String:
                destroy<String>();
                break;
            case Types::Array:
                destroy<Array>();
                break;
            case Types::Tuple:
                destroy<Tuple>();
                break;
            case Types::Map:
                destroy<Map>();
                break;
            case Types::Object:
                destroy<Object>();
                break;
            case Types::AggregateFunctionState:
                destroy<AggregateFunctionStateData>();
                break;
            case Types::CustomType:
                destroy<CustomType>();
                break;
            default:
                 break;
        }

        which = Types::Null;    /// for exception safety in subsequent calls to destroy and create, when create fails.
    }

    template <typename T>
    void destroy()
    {
        T * MAY_ALIAS ptr = reinterpret_cast<T*>(&storage);
        ptr->~T();
    }
};

#undef DBMS_MIN_FIELD_SIZE


using Row = std::vector<Field>;


template <> struct Field::TypeToEnum<Null> { static constexpr Types::Which value = Types::Null; };
template <> struct Field::TypeToEnum<UInt64>  { static constexpr Types::Which value = Types::UInt64; };
template <> struct Field::TypeToEnum<UInt128> { static constexpr Types::Which value = Types::UInt128; };
template <> struct Field::TypeToEnum<UInt256> { static constexpr Types::Which value = Types::UInt256; };
template <> struct Field::TypeToEnum<Int64>   { static constexpr Types::Which value = Types::Int64; };
template <> struct Field::TypeToEnum<Int128>  { static constexpr Types::Which value = Types::Int128; };
template <> struct Field::TypeToEnum<Int256>  { static constexpr Types::Which value = Types::Int256; };
template <> struct Field::TypeToEnum<UUID>    { static constexpr Types::Which value = Types::UUID; };
template <> struct Field::TypeToEnum<IPv4>    { static constexpr Types::Which value = Types::IPv4; };
template <> struct Field::TypeToEnum<IPv6>    { static constexpr Types::Which value = Types::IPv6; };
template <> struct Field::TypeToEnum<Float64> { static constexpr Types::Which value = Types::Float64; };
template <> struct Field::TypeToEnum<String>  { static constexpr Types::Which value = Types::String; };
template <> struct Field::TypeToEnum<Array>   { static constexpr Types::Which value = Types::Array; };
template <> struct Field::TypeToEnum<Tuple>   { static constexpr Types::Which value = Types::Tuple; };
template <> struct Field::TypeToEnum<Map>     { static constexpr Types::Which value = Types::Map; };
template <> struct Field::TypeToEnum<Object>  { static constexpr Types::Which value = Types::Object; };
template <> struct Field::TypeToEnum<DecimalField<Decimal32>>{ static constexpr Types::Which value = Types::Decimal32; };
template <> struct Field::TypeToEnum<DecimalField<Decimal64>>{ static constexpr Types::Which value = Types::Decimal64; };
template <> struct Field::TypeToEnum<DecimalField<Decimal128>>{ static constexpr Types::Which value = Types::Decimal128; };
template <> struct Field::TypeToEnum<DecimalField<Decimal256>>{ static constexpr Types::Which value = Types::Decimal256; };
template <> struct Field::TypeToEnum<DecimalField<DateTime64>>{ static constexpr Types::Which value = Types::Decimal64; };
template <> struct Field::TypeToEnum<AggregateFunctionStateData>{ static constexpr Types::Which value = Types::AggregateFunctionState; };
template <> struct Field::TypeToEnum<CustomType>{ static constexpr Types::Which value = Types::CustomType; };
template <> struct Field::TypeToEnum<bool>{ static constexpr Types::Which value = Types::Bool; };

template <> struct Field::EnumToType<Field::Types::Null>    { using Type = Null; };
template <> struct Field::EnumToType<Field::Types::UInt64>  { using Type = UInt64; };
template <> struct Field::EnumToType<Field::Types::UInt128> { using Type = UInt128; };
template <> struct Field::EnumToType<Field::Types::UInt256> { using Type = UInt256; };
template <> struct Field::EnumToType<Field::Types::Int64>   { using Type = Int64; };
template <> struct Field::EnumToType<Field::Types::Int128>  { using Type = Int128; };
template <> struct Field::EnumToType<Field::Types::Int256>  { using Type = Int256; };
template <> struct Field::EnumToType<Field::Types::UUID>    { using Type = UUID; };
template <> struct Field::EnumToType<Field::Types::IPv4>    { using Type = IPv4; };
template <> struct Field::EnumToType<Field::Types::IPv6>    { using Type = IPv6; };
template <> struct Field::EnumToType<Field::Types::Float64> { using Type = Float64; };
template <> struct Field::EnumToType<Field::Types::String>  { using Type = String; };
template <> struct Field::EnumToType<Field::Types::Array>   { using Type = Array; };
template <> struct Field::EnumToType<Field::Types::Tuple>   { using Type = Tuple; };
template <> struct Field::EnumToType<Field::Types::Map>     { using Type = Map; };
template <> struct Field::EnumToType<Field::Types::Object>  { using Type = Object; };
template <> struct Field::EnumToType<Field::Types::Decimal32> { using Type = DecimalField<Decimal32>; };
template <> struct Field::EnumToType<Field::Types::Decimal64> { using Type = DecimalField<Decimal64>; };
template <> struct Field::EnumToType<Field::Types::Decimal128> { using Type = DecimalField<Decimal128>; };
template <> struct Field::EnumToType<Field::Types::Decimal256> { using Type = DecimalField<Decimal256>; };
template <> struct Field::EnumToType<Field::Types::AggregateFunctionState> { using Type = AggregateFunctionStateData; };
template <> struct Field::EnumToType<Field::Types::CustomType> { using Type = CustomType; };
template <> struct Field::EnumToType<Field::Types::Bool> { using Type = UInt64; };

constexpr bool isInt64OrUInt64FieldType(Field::Types::Which t)
{
    return t == Field::Types::Int64
        || t == Field::Types::UInt64;
}

constexpr bool isInt64OrUInt64orBoolFieldType(Field::Types::Which t)
{
    return t == Field::Types::Int64
        || t == Field::Types::UInt64
        || t == Field::Types::Bool;
}

template <typename T>
auto & Field::safeGet()
{
    const Types::Which target = TypeToEnum<NearestFieldType<std::decay_t<T>>>::value;

    /// bool is stored as uint64, will be returned as UInt64 when requested as bool or UInt64, as Int64 when requested as Int64
    /// also allow UInt64 <-> Int64 conversion
    if (target != which &&
        !(which == Field::Types::Bool && (target == Field::Types::UInt64 || target == Field::Types::Int64)) &&
        !(isInt64OrUInt64FieldType(which) && isInt64OrUInt64FieldType(target)))
            throw Exception(ErrorCodes::BAD_GET, "Bad get: has {}, requested {}", getTypeName(), target);

    return get<T>();
}

template <typename T>
requires not_field_or_bool_or_stringlike<T>
Field::Field(T && rhs)
{
    auto && val = castToNearestFieldType(std::forward<T>(rhs));
    createConcrete(std::forward<decltype(val)>(val));
}

template <typename T>
requires not_field_or_bool_or_stringlike<T>
Field & /// NOLINT
Field::operator=(T && rhs)
{
    auto && val = castToNearestFieldType(std::forward<T>(rhs));
    using U = decltype(val);
    if (which != TypeToEnum<std::decay_t<U>>::value)
    {
        destroy();
        createConcrete(std::forward<U>(val));
    }
    else
        assignConcrete(std::forward<U>(val));
    return *this;
}

inline Field & Field::operator=(std::string_view str)
{
    if (which != Types::String)
    {
        destroy();
        create(str.data(), str.size());
    }
    else
        assignString(str.data(), str.size());
    return *this;
}

inline Field & Field::operator=(String && str)
{
    if (which != Types::String)
    {
        destroy();
        create(std::move(str));
    }
    else
        assignString(std::move(str));
    return *this;
}

class ReadBuffer;
class WriteBuffer;

/// Binary serialization of generic field.
void writeFieldBinary(const Field & x, WriteBuffer & buf);
Field readFieldBinary(ReadBuffer & buf);

void readBinaryArray(Array & x, ReadBuffer & buf);
[[noreturn]] inline void readText(Array &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Array."); }
[[noreturn]] inline void readQuoted(Array &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Array."); }

void writeBinaryArray(const Array & x, WriteBuffer & buf);
void writeText(const Array & x, WriteBuffer & buf);
[[noreturn]] inline void writeQuoted(const Array &, WriteBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot write Array quoted."); }

void readBinary(Tuple & x, ReadBuffer & buf);
[[noreturn]] inline void readText(Tuple &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Tuple."); }
[[noreturn]] inline void readQuoted(Tuple &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Tuple."); }

void writeBinary(const Tuple & x, WriteBuffer & buf);
void writeText(const Tuple & x, WriteBuffer & buf);
[[noreturn]] inline void writeQuoted(const Tuple &, WriteBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot write Tuple quoted."); }

void readBinary(Map & x, ReadBuffer & buf);
[[noreturn]] inline void readText(Map &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Map."); }
[[noreturn]] inline void readQuoted(Map &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Map."); }

void writeBinary(const Map & x, WriteBuffer & buf);
void writeText(const Map & x, WriteBuffer & buf);
[[noreturn]] inline void writeQuoted(const Map &, WriteBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot write Map quoted."); }

void readBinary(Object & x, ReadBuffer & buf);
[[noreturn]] inline void readText(Object &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Object."); }
[[noreturn]] inline void readQuoted(Object &, ReadBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot read Object."); }

void writeBinary(const Object & x, WriteBuffer & buf);
void writeText(const Object & x, WriteBuffer & buf);

void writeBinary(const CustomType & x, WriteBuffer & buf);
void writeText(const CustomType & x, WriteBuffer & buf);

[[noreturn]] inline void writeQuoted(const Object &, WriteBuffer &) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot write Object quoted."); }

__attribute__ ((noreturn)) inline void writeText(const AggregateFunctionStateData &, WriteBuffer &)
{
    // This probably doesn't make any sense, but we have to have it for
    // completeness, so that we can use toString(field_value) in field visitors.
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot convert a Field of type AggregateFunctionStateData to human-readable text");
}

template <typename T>
inline void writeText(const DecimalField<T> & value, WriteBuffer & buf, bool trailing_zeros = false)
{
    writeText(value.getValue(), value.getScale(), buf, trailing_zeros);
}

template <typename T>
void readQuoted(DecimalField<T> & x, ReadBuffer & buf);

void writeFieldText(const Field & x, WriteBuffer & buf);

String toString(const Field & x);

std::string_view fieldTypeToString(Field::Types::Which type);
}

template <>
struct fmt::formatter<DB::Field>
{
    static constexpr auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw fmt::format_error("Invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::Field & x, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", toString(x));
    }
};
