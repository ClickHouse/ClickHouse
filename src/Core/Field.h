#pragma once

#include <cassert>
#include <vector>
#include <algorithm>
#include <type_traits>
#include <functional>

#include <Common/Exception.h>
#include <Common/UInt128.h>
#include <Core/Types.h>
#include <Core/Defines.h>
#include <Core/DecimalFunctions.h>
#include <Core/UUID.h>
#include <common/DayNum.h>
#include <common/strong_typedef.h>


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

template <typename T, typename SFINAE = void>
struct NearestFieldTypeImpl;

template <typename T>
using NearestFieldType = typename NearestFieldTypeImpl<T>::Type;

class Field;
using FieldVector = std::vector<Field>;

/// Array and Tuple use the same storage type -- FieldVector, but we declare
/// distinct types for them, so that the caller can choose whether it wants to
/// construct a Field of Array or a Tuple type. An alternative approach would be
/// to construct both of these types from FieldVector, and have the caller
/// specify the desired Field type explicitly.
#define DEFINE_FIELD_VECTOR(X) \
struct X : public FieldVector \
{ \
    using FieldVector::FieldVector; \
}

DEFINE_FIELD_VECTOR(Array);
DEFINE_FIELD_VECTOR(Tuple);

#undef DEFINE_FIELD_VECTOR

struct AggregateFunctionStateData
{
    String name; /// Name with arguments.
    String data;

    bool operator < (const AggregateFunctionStateData &) const
    {
        throw Exception("Operator < is not implemented for AggregateFunctionStateData.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    bool operator <= (const AggregateFunctionStateData &) const
    {
        throw Exception("Operator <= is not implemented for AggregateFunctionStateData.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    bool operator > (const AggregateFunctionStateData &) const
    {
        throw Exception("Operator > is not implemented for AggregateFunctionStateData.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    bool operator >= (const AggregateFunctionStateData &) const
    {
        throw Exception("Operator >= is not implemented for AggregateFunctionStateData.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    bool operator == (const AggregateFunctionStateData & rhs) const
    {
        if (name != rhs.name)
            throw Exception("Comparing aggregate functions with different types: " + name + " and " + rhs.name,
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return data == rhs.data;
    }
};

template <typename T> bool decimalEqual(T x, T y, UInt32 x_scale, UInt32 y_scale);
template <typename T> bool decimalLess(T x, T y, UInt32 x_scale, UInt32 y_scale);
template <typename T> bool decimalLessOrEqual(T x, T y, UInt32 x_scale, UInt32 y_scale);

#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
template <typename T>
class DecimalField
{
public:
    DecimalField(T value = {}, UInt32 scale_ = 0)
    :   dec(value),
        scale(scale_)
    {}

    operator T() const { return dec; }
    T getValue() const { return dec; }
    T getScaleMultiplier() const { return DecimalUtils::scaleMultiplier<T>(scale); }
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
            throw Exception("Add different decimal fields", ErrorCodes::LOGICAL_ERROR);
        dec += r.getValue();
        return *this;
    }

    const DecimalField<T> & operator -= (const DecimalField<T> & r)
    {
        if (scale != r.getScale())
            throw Exception("Sub different decimal fields", ErrorCodes::LOGICAL_ERROR);
        dec -= r.getValue();
        return *this;
    }

private:
    T dec;
    UInt32 scale;
};
#if !__clang__
#pragma GCC diagnostic pop
#endif

/// char may be signed or unsigned, and behave identically to signed char or unsigned char,
///  but they are always three different types.
/// signedness of char is different in Linux on x86 and Linux on ARM.
template <> struct NearestFieldTypeImpl<char> { using Type = std::conditional_t<is_signed_v<char>, Int64, UInt64>; };
template <> struct NearestFieldTypeImpl<signed char> { using Type = Int64; };
template <> struct NearestFieldTypeImpl<unsigned char> { using Type = UInt64; };
#ifdef __cpp_char8_t
template <> struct NearestFieldTypeImpl<char8_t> { using Type = UInt64; };
#endif

template <> struct NearestFieldTypeImpl<UInt16> { using Type = UInt64; };
template <> struct NearestFieldTypeImpl<UInt32> { using Type = UInt64; };

template <> struct NearestFieldTypeImpl<DayNum> { using Type = UInt64; };
template <> struct NearestFieldTypeImpl<UInt128> { using Type = UInt128; };
template <> struct NearestFieldTypeImpl<UUID> { using Type = UInt128; };
template <> struct NearestFieldTypeImpl<Int16> { using Type = Int64; };
template <> struct NearestFieldTypeImpl<Int32> { using Type = Int64; };

/// long and long long are always different types that may behave identically or not.
/// This is different on Linux and Mac.
template <> struct NearestFieldTypeImpl<long> { using Type = Int64; };
template <> struct NearestFieldTypeImpl<long long> { using Type = Int64; };
template <> struct NearestFieldTypeImpl<unsigned long> { using Type = UInt64; };
template <> struct NearestFieldTypeImpl<unsigned long long> { using Type = UInt64; };

template <> struct NearestFieldTypeImpl<UInt256> { using Type = UInt256; };
template <> struct NearestFieldTypeImpl<Int256> { using Type = Int256; };

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
template <> struct NearestFieldTypeImpl<bool> { using Type = UInt64; };
template <> struct NearestFieldTypeImpl<Null> { using Type = Null; };

template <> struct NearestFieldTypeImpl<AggregateFunctionStateData> { using Type = AggregateFunctionStateData; };

// For enum types, use the field type that corresponds to their underlying type.
template <typename T>
struct NearestFieldTypeImpl<T, std::enable_if_t<std::is_enum_v<T>>>
{
    using Type = NearestFieldType<std::underlying_type_t<T>>;
};

/** 32 is enough. Round number is used for alignment and for better arithmetic inside std::vector.
  * NOTE: Actually, sizeof(std::string) is 32 when using libc++, so Field is 40 bytes.
  */
#define DBMS_MIN_FIELD_SIZE 32


/** Discriminated union of several types.
  * Made for replacement of `boost::variant`
  *  is not generalized,
  *  but somewhat more efficient, and simpler.
  *
  * Used to represent a single value of one of several types in memory.
  * Warning! Prefer to use chunks of columns instead of single values. See Column.h
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

            /// Non-POD types.

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
        };

        static const int MIN_NON_POD = 16;

        static const char * toString(Which which)
        {
            switch (which)
            {
                case Null:    return "Null";
                case UInt64:  return "UInt64";
                case UInt128: return "UInt128";
                case Int64:   return "Int64";
                case Int128:  return "Int128";
                case Float64: return "Float64";
                case String:  return "String";
                case Array:   return "Array";
                case Tuple:   return "Tuple";
                case Decimal32:  return "Decimal32";
                case Decimal64:  return "Decimal64";
                case Decimal128: return "Decimal128";
                case Decimal256: return "Decimal256";
                case AggregateFunctionState: return "AggregateFunctionState";
                case UInt256: return "UInt256";
                case Int256:  return "Int256";
            }

            throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
        }
    };


    /// Returns an identifier for the type or vice versa.
    template <typename T> struct TypeToEnum;
    template <Types::Which which> struct EnumToType;

    static bool IsDecimal(Types::Which which)
    {
        return (which >= Types::Decimal32 && which <= Types::Decimal128) || which == Types::Decimal256;
    }

    /// Templates to avoid ambiguity.
    template <typename T, typename Z = void *>
    using enable_if_not_field_or_stringlike_t = std::enable_if_t<!std::is_same_v<std::decay_t<T>, Field> && !std::is_same_v<NearestFieldType<std::decay_t<T>>, String>, Z>;

    Field()
        : which(Types::Null)
    {
    }

    /** Despite the presence of a template constructor, this constructor is still needed,
      *  since, in its absence, the compiler will still generate the default constructor.
      */
    Field(const Field & rhs)
    {
        create(rhs);
    }

    Field(Field && rhs)
    {
        create(std::move(rhs));
    }

    template <typename T>
    Field(T && rhs, enable_if_not_field_or_stringlike_t<T> = nullptr);

    /// Create a string inplace.
    Field(const std::string_view & str) { create(str.data(), str.size()); }
    Field(const String & str) { create(std::string_view{str}); }
    Field(String && str) { create(std::move(str)); }
    Field(const char * str) { create(std::string_view{str}); }

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

    Field & operator= (Field && rhs)
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
    enable_if_not_field_or_stringlike_t<T, Field> &
    operator=(T && rhs);

    Field & operator =(const std::string_view & str);
    Field & operator =(const String & str) { return *this = std::string_view{str}; }
    Field & operator =(String && str);
    Field & operator =(const char * str) { return *this = std::string_view{str}; }

    ~Field()
    {
        destroy();
    }


    Types::Which getType() const { return which; }
    const char * getTypeName() const { return Types::toString(which); }

    bool isNull() const { return which == Types::Null; }


    template <typename T>
    T & get();

    template <typename T>
    const T & get() const
    {
        auto mutable_this = const_cast<std::decay_t<decltype(*this)> *>(this);
        return mutable_this->get<T>();
    }

    template <typename T>
    T & reinterpret();

    template <typename T>
    const T & reinterpret() const
    {
        auto mutable_this = const_cast<std::decay_t<decltype(*this)> *>(this);
        return mutable_this->reinterpret<T>();
    }

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

    template <typename T> T & safeGet()
    {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        if (which != requested)
            throw Exception("Bad get: has " + std::string(getTypeName()) + ", requested " + std::string(Types::toString(requested)), ErrorCodes::BAD_GET);
        return get<T>();
    }

    template <typename T> const T & safeGet() const
    {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        if (which != requested)
            throw Exception("Bad get: has " + std::string(getTypeName()) + ", requested " + std::string(Types::toString(requested)), ErrorCodes::BAD_GET);
        return get<T>();
    }

    bool operator< (const Field & rhs) const
    {
        if (which < rhs.which)
            return true;
        if (which > rhs.which)
            return false;

        switch (which)
        {
            case Types::Null:    return false;
            case Types::UInt64:  return get<UInt64>()  < rhs.get<UInt64>();
            case Types::UInt128: return get<UInt128>() < rhs.get<UInt128>();
            case Types::Int64:   return get<Int64>()   < rhs.get<Int64>();
            case Types::Int128:  return get<Int128>()  < rhs.get<Int128>();
            case Types::Float64: return get<Float64>() < rhs.get<Float64>();
            case Types::String:  return get<String>()  < rhs.get<String>();
            case Types::Array:   return get<Array>()   < rhs.get<Array>();
            case Types::Tuple:   return get<Tuple>()   < rhs.get<Tuple>();
            case Types::Decimal32:  return get<DecimalField<Decimal32>>()  < rhs.get<DecimalField<Decimal32>>();
            case Types::Decimal64:  return get<DecimalField<Decimal64>>()  < rhs.get<DecimalField<Decimal64>>();
            case Types::Decimal128: return get<DecimalField<Decimal128>>() < rhs.get<DecimalField<Decimal128>>();
            case Types::Decimal256: return get<DecimalField<Decimal256>>() < rhs.get<DecimalField<Decimal256>>();
            case Types::AggregateFunctionState:  return get<AggregateFunctionStateData>() < rhs.get<AggregateFunctionStateData>();
            case Types::UInt256: return get<UInt256>() < rhs.get<UInt256>();
            case Types::Int256: return get<Int256>() < rhs.get<Int256>();
        }

        throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
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
            case Types::Null:    return true;
            case Types::UInt64:  return get<UInt64>()  <= rhs.get<UInt64>();
            case Types::UInt128: return get<UInt128>() <= rhs.get<UInt128>();
            case Types::Int64:   return get<Int64>()   <= rhs.get<Int64>();
            case Types::Int128:  return get<Int128>()  <= rhs.get<Int128>();
            case Types::Float64: return get<Float64>() <= rhs.get<Float64>();
            case Types::String:  return get<String>()  <= rhs.get<String>();
            case Types::Array:   return get<Array>()   <= rhs.get<Array>();
            case Types::Tuple:   return get<Tuple>()   <= rhs.get<Tuple>();
            case Types::Decimal32:  return get<DecimalField<Decimal32>>()  <= rhs.get<DecimalField<Decimal32>>();
            case Types::Decimal64:  return get<DecimalField<Decimal64>>()  <= rhs.get<DecimalField<Decimal64>>();
            case Types::Decimal128: return get<DecimalField<Decimal128>>() <= rhs.get<DecimalField<Decimal128>>();
            case Types::Decimal256: return get<DecimalField<Decimal256>>() <= rhs.get<DecimalField<Decimal256>>();
            case Types::AggregateFunctionState:  return get<AggregateFunctionStateData>() <= rhs.get<AggregateFunctionStateData>();
            case Types::UInt256: return get<UInt256>() <= rhs.get<UInt256>();
            case Types::Int256: return get<Int256>() <= rhs.get<Int256>();
        }

        throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
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
            case Types::Null:    return true;
            case Types::UInt64:  return get<UInt64>() == rhs.get<UInt64>();
            case Types::Int64:   return get<Int64>() == rhs.get<Int64>();
            case Types::Float64:
            {
                // Compare as UInt64 so that NaNs compare as equal.
                return reinterpret<UInt64>() == rhs.reinterpret<UInt64>();
            }
            case Types::String:  return get<String>()  == rhs.get<String>();
            case Types::Array:   return get<Array>()   == rhs.get<Array>();
            case Types::Tuple:   return get<Tuple>()   == rhs.get<Tuple>();
            case Types::UInt128: return get<UInt128>() == rhs.get<UInt128>();
            case Types::Int128:  return get<Int128>()  == rhs.get<Int128>();
            case Types::Decimal32:  return get<DecimalField<Decimal32>>()  == rhs.get<DecimalField<Decimal32>>();
            case Types::Decimal64:  return get<DecimalField<Decimal64>>()  == rhs.get<DecimalField<Decimal64>>();
            case Types::Decimal128: return get<DecimalField<Decimal128>>() == rhs.get<DecimalField<Decimal128>>();
            case Types::Decimal256: return get<DecimalField<Decimal256>>() == rhs.get<DecimalField<Decimal256>>();
            case Types::AggregateFunctionState:  return get<AggregateFunctionStateData>() == rhs.get<AggregateFunctionStateData>();
            case Types::UInt256: return get<UInt256>() == rhs.get<UInt256>();
            case Types::Int256:  return get<Int256>()  == rhs.get<Int256>();
        }

        throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
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
// gcc 8.2.1
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
            case Types::UInt64:  return f(field.template get<UInt64>());
            case Types::UInt128: return f(field.template get<UInt128>());
            case Types::Int64:   return f(field.template get<Int64>());
            case Types::Float64: return f(field.template get<Float64>());
            case Types::String:  return f(field.template get<String>());
            case Types::Array:   return f(field.template get<Array>());
            case Types::Tuple:   return f(field.template get<Tuple>());
            case Types::Decimal32:  return f(field.template get<DecimalField<Decimal32>>());
            case Types::Decimal64:  return f(field.template get<DecimalField<Decimal64>>());
            case Types::Decimal128: return f(field.template get<DecimalField<Decimal128>>());
            case Types::Decimal256: return f(field.template get<DecimalField<Decimal256>>());
            case Types::AggregateFunctionState: return f(field.template get<AggregateFunctionStateData>());
            case Types::Int128: return f(field.template get<Int128>());
            case Types::UInt256: return f(field.template get<UInt256>());
            case Types::Int256: return f(field.template get<Int256>());
#if !__clang__
#pragma GCC diagnostic pop
#endif
        }

        // GCC 9 complains that control reaches the end, despite that we handle
        // all the cases above (maybe because of throw?). Return something to
        // silence it.
        Null null{};
        return f(null);
    }

    String dump() const;
    static Field restoreFromDump(const std::string_view & dump_);

private:
    std::aligned_union_t<DBMS_MIN_FIELD_SIZE - sizeof(Types::Which),
        Null, UInt64, UInt128, Int64, Int128, Float64, String, Array, Tuple,
        DecimalField<Decimal32>, DecimalField<Decimal64>, DecimalField<Decimal128>, DecimalField<Decimal256>,
        AggregateFunctionStateData,
        UInt256, Int256
        > storage;

    Types::Which which;


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
    std::enable_if_t<sizeof(CharT) == 1> assignString(const CharT * data, size_t size)
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
    std::enable_if_t<sizeof(CharT) == 1> create(const CharT * data, size_t size)
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
        if (which < Types::MIN_NON_POD)
            return;

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
            case Types::AggregateFunctionState:
                destroy<AggregateFunctionStateData>();
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


template <> struct Field::TypeToEnum<Null>    { static const Types::Which value = Types::Null; };
template <> struct Field::TypeToEnum<UInt64>  { static const Types::Which value = Types::UInt64; };
template <> struct Field::TypeToEnum<UInt128> { static const Types::Which value = Types::UInt128; };
template <> struct Field::TypeToEnum<Int64>   { static const Types::Which value = Types::Int64; };
template <> struct Field::TypeToEnum<Int128>  { static const Types::Which value = Types::Int128; };
template <> struct Field::TypeToEnum<Float64> { static const Types::Which value = Types::Float64; };
template <> struct Field::TypeToEnum<String>  { static const Types::Which value = Types::String; };
template <> struct Field::TypeToEnum<Array>   { static const Types::Which value = Types::Array; };
template <> struct Field::TypeToEnum<Tuple>   { static const Types::Which value = Types::Tuple; };
template <> struct Field::TypeToEnum<DecimalField<Decimal32>>{ static const Types::Which value = Types::Decimal32; };
template <> struct Field::TypeToEnum<DecimalField<Decimal64>>{ static const Types::Which value = Types::Decimal64; };
template <> struct Field::TypeToEnum<DecimalField<Decimal128>>{ static const Types::Which value = Types::Decimal128; };
template <> struct Field::TypeToEnum<DecimalField<Decimal256>>{ static const Types::Which value = Types::Decimal256; };
template <> struct Field::TypeToEnum<DecimalField<DateTime64>>{ static const Types::Which value = Types::Decimal64; };
template <> struct Field::TypeToEnum<AggregateFunctionStateData>{ static const Types::Which value = Types::AggregateFunctionState; };
template <> struct Field::TypeToEnum<UInt256> { static const Types::Which value = Types::UInt256; };
template <> struct Field::TypeToEnum<Int256> { static const Types::Which value = Types::Int256; };

template <> struct Field::EnumToType<Field::Types::Null>    { using Type = Null; };
template <> struct Field::EnumToType<Field::Types::UInt64>  { using Type = UInt64; };
template <> struct Field::EnumToType<Field::Types::UInt128> { using Type = UInt128; };
template <> struct Field::EnumToType<Field::Types::Int64>   { using Type = Int64; };
template <> struct Field::EnumToType<Field::Types::Int128>  { using Type = Int128; };
template <> struct Field::EnumToType<Field::Types::Float64> { using Type = Float64; };
template <> struct Field::EnumToType<Field::Types::String>  { using Type = String; };
template <> struct Field::EnumToType<Field::Types::Array>   { using Type = Array; };
template <> struct Field::EnumToType<Field::Types::Tuple>   { using Type = Tuple; };
template <> struct Field::EnumToType<Field::Types::Decimal32> { using Type = DecimalField<Decimal32>; };
template <> struct Field::EnumToType<Field::Types::Decimal64> { using Type = DecimalField<Decimal64>; };
template <> struct Field::EnumToType<Field::Types::Decimal128> { using Type = DecimalField<Decimal128>; };
template <> struct Field::EnumToType<Field::Types::Decimal256> { using Type = DecimalField<Decimal256>; };
template <> struct Field::EnumToType<Field::Types::AggregateFunctionState> { using Type = DecimalField<AggregateFunctionStateData>; };
template <> struct Field::EnumToType<Field::Types::UInt256> { using Type = UInt256; };
template <> struct Field::EnumToType<Field::Types::Int256>  { using Type = Int256; };

inline constexpr bool isInt64FieldType(Field::Types::Which t)
{
    return t == Field::Types::Int64
        || t == Field::Types::UInt64;
}

// Field value getter with type checking in debug builds.
template <typename T>
T & Field::get()
{
    using ValueType = std::decay_t<T>;

#ifndef NDEBUG
    // Disregard signedness when converting between int64 types.
    constexpr Field::Types::Which target = TypeToEnum<NearestFieldType<ValueType>>::value;
    if (target != which
           && (!isInt64FieldType(target) || !isInt64FieldType(which)))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid Field get from type {} to type {}", Types::toString(which), Types::toString(target));
#endif

    ValueType * MAY_ALIAS ptr = reinterpret_cast<ValueType *>(&storage);
    return *ptr;
}

template <typename T>
T & Field::reinterpret()
{
    using ValueType = std::decay_t<T>;
    ValueType * MAY_ALIAS ptr = reinterpret_cast<ValueType *>(&storage);
    return *ptr;
}

template <typename T>
T get(const Field & field)
{
    return field.template get<T>();
}

template <typename T>
T get(Field & field)
{
    return field.template get<T>();
}

template <typename T>
T safeGet(const Field & field)
{
    return field.template safeGet<T>();
}

template <typename T>
T safeGet(Field & field)
{
    return field.template safeGet<T>();
}


template <> struct TypeName<Array> { static std::string get() { return "Array"; } };
template <> struct TypeName<Tuple> { static std::string get() { return "Tuple"; } };
template <> struct TypeName<AggregateFunctionStateData> { static std::string get() { return "AggregateFunctionState"; } };

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
Field::Field(T && rhs, enable_if_not_field_or_stringlike_t<T>)
{
    auto && val = castToNearestFieldType(std::forward<T>(rhs));
    createConcrete(std::forward<decltype(val)>(val));
}

template <typename T>
Field::enable_if_not_field_or_stringlike_t<T, Field> &
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


inline Field & Field::operator=(const std::string_view & str)
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

/// It is assumed that all elements of the array have the same type.
void readBinary(Array & x, ReadBuffer & buf);

[[noreturn]] inline void readText(Array &, ReadBuffer &) { throw Exception("Cannot read Array.", ErrorCodes::NOT_IMPLEMENTED); }
[[noreturn]] inline void readQuoted(Array &, ReadBuffer &) { throw Exception("Cannot read Array.", ErrorCodes::NOT_IMPLEMENTED); }

/// It is assumed that all elements of the array have the same type.
/// Also write size and type into buf. UInt64 and Int64 is written in variadic size form
void writeBinary(const Array & x, WriteBuffer & buf);

void writeText(const Array & x, WriteBuffer & buf);

[[noreturn]] inline void writeQuoted(const Array &, WriteBuffer &) { throw Exception("Cannot write Array quoted.", ErrorCodes::NOT_IMPLEMENTED); }

void readBinary(Tuple & x, ReadBuffer & buf);

[[noreturn]] inline void readText(Tuple &, ReadBuffer &) { throw Exception("Cannot read Tuple.", ErrorCodes::NOT_IMPLEMENTED); }
[[noreturn]] inline void readQuoted(Tuple &, ReadBuffer &) { throw Exception("Cannot read Tuple.", ErrorCodes::NOT_IMPLEMENTED); }

void writeBinary(const Tuple & x, WriteBuffer & buf);

void writeText(const Tuple & x, WriteBuffer & buf);


__attribute__ ((noreturn)) inline void writeText(const AggregateFunctionStateData &, WriteBuffer &)
{
    // This probably doesn't make any sense, but we have to have it for
    // completeness, so that we can use toString(field_value) in field visitors.
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot convert a Field of type AggregateFunctionStateData to human-readable text");
}

template <typename T>
inline void writeText(const DecimalField<T> & value, WriteBuffer & buf)
{
    writeText(value.getValue(), value.getScale(), buf);
}

template <typename T>
void readQuoted(DecimalField<T> & x, ReadBuffer & buf);

void writeFieldText(const Field & x, WriteBuffer & buf);

[[noreturn]] inline void writeQuoted(const Tuple &, WriteBuffer &) { throw Exception("Cannot write Tuple quoted.", ErrorCodes::NOT_IMPLEMENTED); }

String toString(const Field & x);

}
