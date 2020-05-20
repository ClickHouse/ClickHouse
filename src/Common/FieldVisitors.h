#pragma once

#include <Core/Field.h>
#include <Core/AccurateComparison.h>
#include <common/demangle.h>


class SipHash;


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONVERT_TYPE;
    extern const int BAD_TYPE_OF_FIELD;
    extern const int LOGICAL_ERROR;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wredundant-decls"
// Just dont mess with it. If the redundant redeclaration is removed then ReaderHelpers.h should be included.
// This leads to Arena.h inclusion which has a problem with ASAN stuff included properly and messing macro definition
// which intefrers with... You dont want to know, really.
UInt128 stringToUUID(const String & str);
#pragma GCC diagnostic pop


/** StaticVisitor (and its descendants) - class with overloaded operator() for all types of fields.
  * You could call visitor for field using function 'applyVisitor'.
  * Also "binary visitor" is supported - its operator() takes two arguments.
  */
template <typename R = void>
struct StaticVisitor
{
    using ResultType = R;
};


/// F is template parameter, to allow universal reference for field, that is useful for const and non-const values.
template <typename Visitor, typename F>
auto applyVisitor(Visitor && visitor, F && field)
{
    return Field::dispatch(visitor, field);
}

template <typename Visitor, typename F1, typename F2>
auto applyVisitor(Visitor && visitor, F1 && field1, F2 && field2)
{
    return Field::dispatch([&](auto & field1_value)
        {
            return Field::dispatch([&](auto & field2_value)
                {
                    return visitor(field1_value, field2_value);
                },
                field2);
        },
        field1);
}


/** Prints Field as literal in SQL query */
class FieldVisitorToString : public StaticVisitor<String>
{
public:
    String operator() (const Null & x) const;
    String operator() (const UInt64 & x) const;
    String operator() (const UInt128 & x) const;
    String operator() (const Int64 & x) const;
    String operator() (const Float64 & x) const;
    String operator() (const String & x) const;
    String operator() (const Array & x) const;
    String operator() (const Tuple & x) const;
    String operator() (const DecimalField<Decimal32> & x) const;
    String operator() (const DecimalField<Decimal64> & x) const;
    String operator() (const DecimalField<Decimal128> & x) const;
    String operator() (const AggregateFunctionStateData & x) const;
};


/** Print readable and unique text dump of field type and value. */
class FieldVisitorDump : public StaticVisitor<String>
{
public:
    String operator() (const Null & x) const;
    String operator() (const UInt64 & x) const;
    String operator() (const UInt128 & x) const;
    String operator() (const Int64 & x) const;
    String operator() (const Float64 & x) const;
    String operator() (const String & x) const;
    String operator() (const Array & x) const;
    String operator() (const Tuple & x) const;
    String operator() (const DecimalField<Decimal32> & x) const;
    String operator() (const DecimalField<Decimal64> & x) const;
    String operator() (const DecimalField<Decimal128> & x) const;
    String operator() (const AggregateFunctionStateData & x) const;
};


/** Converts numberic value of any type to specified type. */
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

    T operator() (const UInt64 & x) const { return x; }
    T operator() (const Int64 & x) const { return x; }
    T operator() (const Float64 & x) const { return x; }

    T operator() (const UInt128 &) const
    {
        throw Exception("Cannot convert UInt128 to " + demangle(typeid(T).name()), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    template <typename U>
    T operator() (const DecimalField<U> & x) const
    {
        if constexpr (std::is_floating_point_v<T>)
            return static_cast<T>(x.getValue()) / x.getScaleMultiplier();
        else
            return x.getValue() / x.getScaleMultiplier();
    }

    T operator() (const AggregateFunctionStateData &) const
    {
        throw Exception("Cannot convert AggregateFunctionStateData to " + demangle(typeid(T).name()), ErrorCodes::CANNOT_CONVERT_TYPE);
    }
};


/** Updates SipHash by type and value of Field */
class FieldVisitorHash : public StaticVisitor<>
{
private:
    SipHash & hash;
public:
    FieldVisitorHash(SipHash & hash_);

    void operator() (const Null & x) const;
    void operator() (const UInt64 & x) const;
    void operator() (const UInt128 & x) const;
    void operator() (const Int64 & x) const;
    void operator() (const Float64 & x) const;
    void operator() (const String & x) const;
    void operator() (const Array & x) const;
    void operator() (const Tuple & x) const;
    void operator() (const DecimalField<Decimal32> & x) const;
    void operator() (const DecimalField<Decimal64> & x) const;
    void operator() (const DecimalField<Decimal128> & x) const;
    void operator() (const AggregateFunctionStateData & x) const;
};


template <typename T> constexpr bool isDecimalField() { return false; }
template <> constexpr bool isDecimalField<DecimalField<Decimal32>>() { return true; }
template <> constexpr bool isDecimalField<DecimalField<Decimal64>>() { return true; }
template <> constexpr bool isDecimalField<DecimalField<Decimal128>>() { return true; }


/** More precise comparison, used for index.
  * Differs from Field::operator< and Field::operator== in that it also compares values of different types.
  * Comparison rules are same as in FunctionsComparison (to be consistent with expression evaluation in query).
  */
class FieldVisitorAccurateEquals : public StaticVisitor<bool>
{
public:
    bool operator() (const UInt64 &, const Null &)          const { return false; }
    bool operator() (const UInt64 & l, const UInt64 & r)    const { return l == r; }
    bool operator() (const UInt64 & l, const UInt128 & r)   const { return cantCompare(l, r); }
    bool operator() (const UInt64 & l, const Int64 & r)     const { return accurate::equalsOp(l, r); }
    bool operator() (const UInt64 & l, const Float64 & r)   const { return accurate::equalsOp(l, r); }
    bool operator() (const UInt64 & l, const String & r)    const { return cantCompare(l, r); }
    bool operator() (const UInt64 & l, const Array & r)     const { return cantCompare(l, r); }
    bool operator() (const UInt64 & l, const Tuple & r)     const { return cantCompare(l, r); }
    bool operator() (const UInt64 & l, const AggregateFunctionStateData & r) const { return cantCompare(l, r); }

    bool operator() (const Int64 &, const Null &)           const { return false; }
    bool operator() (const Int64 & l, const UInt64 & r)     const { return accurate::equalsOp(l, r); }
    bool operator() (const Int64 & l, const UInt128 & r)    const { return cantCompare(l, r); }
    bool operator() (const Int64 & l, const Int64 & r)      const { return l == r; }
    bool operator() (const Int64 & l, const Float64 & r)    const { return accurate::equalsOp(l, r); }
    bool operator() (const Int64 & l, const String & r)     const { return cantCompare(l, r); }
    bool operator() (const Int64 & l, const Array & r)      const { return cantCompare(l, r); }
    bool operator() (const Int64 & l, const Tuple & r)      const { return cantCompare(l, r); }
    bool operator() (const Int64 & l, const AggregateFunctionStateData & r) const { return cantCompare(l, r); }

    bool operator() (const Float64 &, const Null &)         const { return false; }
    bool operator() (const Float64 & l, const UInt64 & r)   const { return accurate::equalsOp(l, r); }
    bool operator() (const Float64 & l, const UInt128 & r)  const { return cantCompare(l, r); }
    bool operator() (const Float64 & l, const Int64 & r)    const { return accurate::equalsOp(l, r); }
    bool operator() (const Float64 & l, const Float64 & r)  const { return l == r; }
    bool operator() (const Float64 & l, const String & r)   const { return cantCompare(l, r); }
    bool operator() (const Float64 & l, const Array & r)    const { return cantCompare(l, r); }
    bool operator() (const Float64 & l, const Tuple & r)    const { return cantCompare(l, r); }
    bool operator() (const Float64 & l, const AggregateFunctionStateData & r) const { return cantCompare(l, r); }

    template <typename T>
    bool operator() (const Null &, const T &) const
    {
        return std::is_same_v<T, Null>;
    }

    template <typename T>
    bool operator() (const String & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, String>)
            return l == r;
        if constexpr (std::is_same_v<T, UInt128>)
            return stringToUUID(l) == r;
        if constexpr (std::is_same_v<T, Null>)
            return false;
        return cantCompare(l, r);
    }

    template <typename T>
    bool operator() (const UInt128 & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, UInt128>)
            return l == r;
        if constexpr (std::is_same_v<T, String>)
            return l == stringToUUID(r);
        if constexpr (std::is_same_v<T, Null>)
            return false;
        return cantCompare(l, r);
    }

    template <typename T>
    bool operator() (const Array & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, Array>)
            return l == r;
        if constexpr (std::is_same_v<T, Null>)
            return false;
        return cantCompare(l, r);
    }

    template <typename T>
    bool operator() (const Tuple & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, Tuple>)
            return l == r;
        if constexpr (std::is_same_v<T, Null>)
            return false;
        return cantCompare(l, r);
    }

    template <typename T, typename U>
    bool operator() (const DecimalField<T> & l, const U & r) const
    {
        if constexpr (isDecimalField<U>())
            return l == r;
        if constexpr (std::is_same_v<U, Int64> || std::is_same_v<U, UInt64>)
            return l == DecimalField<Decimal128>(r, 0);
        if constexpr (std::is_same_v<U, Null>)
            return false;
        return cantCompare(l, r);
    }

    template <typename T> bool operator() (const UInt64 & l, const DecimalField<T> & r) const { return DecimalField<Decimal128>(l, 0) == r; }
    template <typename T> bool operator() (const Int64 & l, const DecimalField<T> & r) const { return DecimalField<Decimal128>(l, 0) == r; }
    template <typename T> bool operator() (const Float64 & l, const DecimalField<T> & r) const { return cantCompare(l, r); }

    template <typename T>
    bool operator() (const AggregateFunctionStateData & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, AggregateFunctionStateData>)
            return l == r;
        return cantCompare(l, r);
    }

private:
    template <typename T, typename U>
    bool cantCompare(const T &, const U &) const
    {
        if constexpr (std::is_same_v<U, Null>)
            return false;
        throw Exception("Cannot compare " + demangle(typeid(T).name()) + " with " + demangle(typeid(U).name()),
                        ErrorCodes::BAD_TYPE_OF_FIELD);
    }
};

class FieldVisitorAccurateLess : public StaticVisitor<bool>
{
public:
    bool operator() (const UInt64 &, const Null &)          const { return false; }
    bool operator() (const UInt64 & l, const UInt64 & r)    const { return l < r; }
    bool operator() (const UInt64 & l, const UInt128 & r)   const { return cantCompare(l, r); }
    bool operator() (const UInt64 & l, const Int64 & r)     const { return accurate::lessOp(l, r); }
    bool operator() (const UInt64 & l, const Float64 & r)   const { return accurate::lessOp(l, r); }
    bool operator() (const UInt64 & l, const String & r)    const { return cantCompare(l, r); }
    bool operator() (const UInt64 & l, const Array & r)     const { return cantCompare(l, r); }
    bool operator() (const UInt64 & l, const Tuple & r)     const { return cantCompare(l, r); }
    bool operator() (const UInt64 & l, const AggregateFunctionStateData & r) const { return cantCompare(l, r); }

    bool operator() (const Int64 &, const Null &)           const { return false; }
    bool operator() (const Int64 & l, const UInt64 & r)     const { return accurate::lessOp(l, r); }
    bool operator() (const Int64 & l, const UInt128 & r)    const { return cantCompare(l, r); }
    bool operator() (const Int64 & l, const Int64 & r)      const { return l < r; }
    bool operator() (const Int64 & l, const Float64 & r)    const { return accurate::lessOp(l, r); }
    bool operator() (const Int64 & l, const String & r)     const { return cantCompare(l, r); }
    bool operator() (const Int64 & l, const Array & r)      const { return cantCompare(l, r); }
    bool operator() (const Int64 & l, const Tuple & r)      const { return cantCompare(l, r); }
    bool operator() (const Int64 & l, const AggregateFunctionStateData & r) const { return cantCompare(l, r); }

    bool operator() (const Float64 &, const Null &)         const { return false; }
    bool operator() (const Float64 & l, const UInt64 & r)   const { return accurate::lessOp(l, r); }
    bool operator() (const Float64 & l, const UInt128 & r)  const { return cantCompare(l, r); }
    bool operator() (const Float64 & l, const Int64 & r)    const { return accurate::lessOp(l, r); }
    bool operator() (const Float64 & l, const Float64 & r)  const { return l < r; }
    bool operator() (const Float64 & l, const String & r)   const { return cantCompare(l, r); }
    bool operator() (const Float64 & l, const Array & r)    const { return cantCompare(l, r); }
    bool operator() (const Float64 & l, const Tuple & r)    const { return cantCompare(l, r); }
    bool operator() (const Float64 & l, const AggregateFunctionStateData & r) const { return cantCompare(l, r); }

    template <typename T>
    bool operator() (const Null &, const T &) const
    {
        return !std::is_same_v<T, Null>;
    }

    template <typename T>
    bool operator() (const String & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, String>)
            return l < r;
        if constexpr (std::is_same_v<T, UInt128>)
            return stringToUUID(l) < r;
        if constexpr (std::is_same_v<T, Null>)
            return false;
        return cantCompare(l, r);
    }

    template <typename T>
    bool operator() (const UInt128 & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, UInt128>)
            return l < r;
        if constexpr (std::is_same_v<T, String>)
            return l < stringToUUID(r);
        if constexpr (std::is_same_v<T, Null>)
            return false;
        return cantCompare(l, r);
    }

    template <typename T>
    bool operator() (const Array & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, Array>)
            return l < r;
        if constexpr (std::is_same_v<T, Null>)
            return false;
        return cantCompare(l, r);
    }

    template <typename T>
    bool operator() (const Tuple & l, const T & r) const
    {
        if constexpr (std::is_same_v<T, Tuple>)
            return l < r;
        if constexpr (std::is_same_v<T, Null>)
            return false;
        return cantCompare(l, r);
    }

    template <typename T, typename U>
    bool operator() (const DecimalField<T> & l, const U & r) const
    {
        if constexpr (isDecimalField<U>())
            return l < r;
        if constexpr (std::is_same_v<U, Int64> || std::is_same_v<U, UInt64>)
            return l < DecimalField<Decimal128>(r, 0);
        if constexpr (std::is_same_v<U, Null>)
            return false;
        return cantCompare(l, r);
    }

    template <typename T> bool operator() (const UInt64 & l, const DecimalField<T> & r) const { return DecimalField<Decimal128>(l, 0) < r; }
    template <typename T> bool operator() (const Int64 & l, const DecimalField<T> & r) const { return DecimalField<Decimal128>(l, 0) < r; }
    template <typename T> bool operator() (const Float64 &, const DecimalField<T> &) const { return false; }

    template <typename T>
    bool operator() (const AggregateFunctionStateData & l, const T & r) const
    {
        return cantCompare(l, r);
    }

private:
    template <typename T, typename U>
    bool cantCompare(const T &, const U &) const
    {
        throw Exception("Cannot compare " + demangle(typeid(T).name()) + " with " + demangle(typeid(U).name()),
                        ErrorCodes::BAD_TYPE_OF_FIELD);
    }
};


/** Implements `+=` operation.
 *  Returns false if the result is zero.
 */
class FieldVisitorSum : public StaticVisitor<bool>
{
private:
    const Field & rhs;
public:
    explicit FieldVisitorSum(const Field & rhs_) : rhs(rhs_) {}

    // We can add all ints as unsigned regardless of their actual signedness.
    bool operator() (Int64 & x) const { return this->operator()(reinterpret_cast<UInt64 &>(x)); }
    bool operator() (UInt64 & x) const
    {
        x += rhs.reinterpret<UInt64>();
        return x != 0;
    }

    bool operator() (Float64 & x) const { x += get<Float64>(rhs); return x != 0; }

    bool operator() (Null &) const { throw Exception("Cannot sum Nulls", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (String &) const { throw Exception("Cannot sum Strings", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (Array &) const { throw Exception("Cannot sum Arrays", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (Tuple &) const { throw Exception("Cannot sum Tuples", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (UInt128 &) const { throw Exception("Cannot sum UUIDs", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (AggregateFunctionStateData &) const { throw Exception("Cannot sum AggregateFunctionStates", ErrorCodes::LOGICAL_ERROR); }

    template <typename T>
    bool operator() (DecimalField<T> & x) const
    {
        x += get<DecimalField<T>>(rhs);
        return x.getValue() != 0;
    }
};

}
