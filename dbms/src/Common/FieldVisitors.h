#pragma once

#include <Core/Field.h>
#include <Core/AccurateComparison.h>
#include <common/DateLUT.h>
#include <common/demangle.h>


class SipHash;


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONVERT_TYPE;
    extern const int LOGICAL_ERROR;
}


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
typename std::decay_t<Visitor>::ResultType applyVisitor(Visitor && visitor, F && field)
{
    switch (field.getType())
    {
        case Field::Types::Null:    return visitor(field.template get<Null>());
        case Field::Types::UInt64:  return visitor(field.template get<UInt64>());
        case Field::Types::UInt128: return visitor(field.template get<UInt128>());
        case Field::Types::Int64:   return visitor(field.template get<Int64>());
        case Field::Types::Float64: return visitor(field.template get<Float64>());
        case Field::Types::String:  return visitor(field.template get<String>());
        case Field::Types::Array:   return visitor(field.template get<Array>());
        case Field::Types::Tuple:   return visitor(field.template get<Tuple>());

        default:
            throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}


template <typename Visitor, typename F1, typename F2>
static typename std::decay_t<Visitor>::ResultType applyBinaryVisitorImpl(Visitor && visitor, F1 && field1, F2 && field2)
{
    switch (field2.getType())
    {
        case Field::Types::Null:    return visitor(field1, field2.template get<Null>());
        case Field::Types::UInt64:  return visitor(field1, field2.template get<UInt64>());
        case Field::Types::UInt128: return visitor(field1, field2.template get<UInt128>());
        case Field::Types::Int64:   return visitor(field1, field2.template get<Int64>());
        case Field::Types::Float64: return visitor(field1, field2.template get<Float64>());
        case Field::Types::String:  return visitor(field1, field2.template get<String>());
        case Field::Types::Array:   return visitor(field1, field2.template get<Array>());
        case Field::Types::Tuple:   return visitor(field1, field2.template get<Tuple>());

        default:
            throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}

template <typename Visitor, typename F1, typename F2>
typename std::decay_t<Visitor>::ResultType applyVisitor(Visitor && visitor, F1 && field1, F2 && field2)
{
    switch (field1.getType())
    {
        case Field::Types::Null:
            return applyBinaryVisitorImpl(
                std::forward<Visitor>(visitor), field1.template get<Null>(), std::forward<F2>(field2));
        case Field::Types::UInt64:
            return applyBinaryVisitorImpl(
                std::forward<Visitor>(visitor), field1.template get<UInt64>(), std::forward<F2>(field2));
        case Field::Types::UInt128:
            return applyBinaryVisitorImpl(
                std::forward<Visitor>(visitor), field1.template get<UInt128>(), std::forward<F2>(field2));
        case Field::Types::Int64:
            return applyBinaryVisitorImpl(
                std::forward<Visitor>(visitor), field1.template get<Int64>(), std::forward<F2>(field2));
        case Field::Types::Float64:
            return applyBinaryVisitorImpl(
                std::forward<Visitor>(visitor), field1.template get<Float64>(), std::forward<F2>(field2));
        case Field::Types::String:
            return applyBinaryVisitorImpl(
                std::forward<Visitor>(visitor), field1.template get<String>(), std::forward<F2>(field2));
        case Field::Types::Array:
            return applyBinaryVisitorImpl(
                std::forward<Visitor>(visitor), field1.template get<Array>(), std::forward<F2>(field2));
        case Field::Types::Tuple:
            return applyBinaryVisitorImpl(
                std::forward<Visitor>(visitor), field1.template get<Tuple>(), std::forward<F2>(field2));

        default:
            throw Exception("Bad type of Field", ErrorCodes::BAD_TYPE_OF_FIELD);
    }
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
};


/** Updates SipHash by type and value of Field */
class FieldVisitorHash : public StaticVisitor<>
{
private:
    SipHash & hash;
public:
    FieldVisitorHash(SipHash & hash);

    void operator() (const Null & x) const;
    void operator() (const UInt64 & x) const;
    void operator() (const UInt128 & x) const;
    void operator() (const Int64 & x) const;
    void operator() (const Float64 & x) const;
    void operator() (const String & x) const;
    void operator() (const Array & x) const;
};


/** More precise comparison, used for index.
  * Differs from Field::operator< and Field::operator== in that it also compares values of different types.
  * Comparison rules are same as in FunctionsComparison (to be consistent with expression evaluation in query).
  *
  * TODO Comparisons of UInt128 with different type are incorrect.
  */
class FieldVisitorAccurateEquals : public StaticVisitor<bool>
{
public:
    bool operator() (const Null &, const Null &)        const { return true; }
    bool operator() (const Null &, const UInt64 &)      const { return false; }
    bool operator() (const Null &, const UInt128 &)     const { return false; }
    bool operator() (const Null &, const Int64 &)       const { return false; }
    bool operator() (const Null &, const Float64 &)     const { return false; }
    bool operator() (const Null &, const String &)      const { return false; }
    bool operator() (const Null &, const Array &)       const { return false; }
    bool operator() (const Null &, const Tuple &)       const { return false; }

    bool operator() (const UInt64 &, const Null &)          const { return false; }
    bool operator() (const UInt64 & l, const UInt64 & r)    const { return l == r; }
    bool operator() (const UInt64 &, const UInt128)         const { return true; }
    bool operator() (const UInt64 & l, const Int64 & r)     const { return accurate::equalsOp(l, r); }
    bool operator() (const UInt64 & l, const Float64 & r)   const { return accurate::equalsOp(l, r); }
    bool operator() (const UInt64 &, const String &)        const { return false; }
    bool operator() (const UInt64 &, const Array &)         const { return false; }
    bool operator() (const UInt64 &, const Tuple &)         const { return false; }

    bool operator() (const UInt128 &, const Null &)          const { return false; }
    bool operator() (const UInt128 &, const UInt64)          const { return false; }
    bool operator() (const UInt128 & l, const UInt128 & r)   const { return l == r; }
    bool operator() (const UInt128 &, const Int64)           const { return false; }
    bool operator() (const UInt128 &, const Float64)         const { return false; }
    bool operator() (const UInt128 &, const String &)        const { return false; }
    bool operator() (const UInt128 &, const Array &)         const { return false; }
    bool operator() (const UInt128 &, const Tuple &)         const { return false; }

    bool operator() (const Int64 &, const Null &)           const { return false; }
    bool operator() (const Int64 & l, const UInt64 & r)     const { return accurate::equalsOp(l, r); }
    bool operator() (const Int64 &, const UInt128)          const { return false; }
    bool operator() (const Int64 & l, const Int64 & r)      const { return l == r; }
    bool operator() (const Int64 & l, const Float64 & r)    const { return accurate::equalsOp(l, r); }
    bool operator() (const Int64 &, const String &)         const { return false; }
    bool operator() (const Int64 &, const Array &)          const { return false; }
    bool operator() (const Int64 &, const Tuple &)          const { return false; }

    bool operator() (const Float64 &, const Null &)         const { return false; }
    bool operator() (const Float64 & l, const UInt64 & r)   const { return accurate::equalsOp(l, r); }
    bool operator() (const Float64 &, const UInt128)        const { return false; }
    bool operator() (const Float64 & l, const Int64 & r)    const { return accurate::equalsOp(l, r); }
    bool operator() (const Float64 & l, const Float64 & r)  const { return l == r; }
    bool operator() (const Float64 &, const String &)       const { return false; }
    bool operator() (const Float64 &, const Array &)        const { return false; }
    bool operator() (const Float64 &, const Tuple &)        const { return false; }

    bool operator() (const String &, const Null &)      const { return false; }
    bool operator() (const String &, const UInt64 &)    const { return false; }
    bool operator() (const String &, const UInt128 &)   const { return false; }
    bool operator() (const String &, const Int64 &)     const { return false; }
    bool operator() (const String &, const Float64 &)   const { return false; }
    bool operator() (const String & l, const String & r)    const { return l == r; }
    bool operator() (const String &, const Array &)     const { return false; }
    bool operator() (const String &, const Tuple &)     const { return false; }

    bool operator() (const Array &, const Null &)       const { return false; }
    bool operator() (const Array &, const UInt64 &)     const { return false; }
    bool operator() (const Array &, const UInt128 &)    const { return false; }
    bool operator() (const Array &, const Int64 &)      const { return false; }
    bool operator() (const Array &, const Float64 &)    const { return false; }
    bool operator() (const Array &, const String &)     const { return false; }
    bool operator() (const Array & l, const Array & r)      const { return l == r; }
    bool operator() (const Array &, const Tuple &)      const { return false; }

    bool operator() (const Tuple &, const Null &)       const { return false; }
    bool operator() (const Tuple &, const UInt64 &)     const { return false; }
    bool operator() (const Tuple &, const UInt128 &)    const { return false; }
    bool operator() (const Tuple &, const Int64 &)      const { return false; }
    bool operator() (const Tuple &, const Float64 &)    const { return false; }
    bool operator() (const Tuple &, const String &)     const { return false; }
    bool operator() (const Tuple &, const Array &)      const { return false; }
    bool operator() (const Tuple & l, const Tuple & r)      const { return l == r; }
};

class FieldVisitorAccurateLess : public StaticVisitor<bool>
{
public:
    bool operator() (const Null &, const Null &)        const { return false; }
    bool operator() (const Null &, const UInt64 &)      const { return true; }
    bool operator() (const Null &, const Int64 &)       const { return true; }
    bool operator() (const Null &, const UInt128 &)     const { return true; }
    bool operator() (const Null &, const Float64 &)     const { return true; }
    bool operator() (const Null &, const String &)      const { return true; }
    bool operator() (const Null &, const Array &)       const { return true; }
    bool operator() (const Null &, const Tuple &)       const { return true; }

    bool operator() (const UInt64 &, const Null &)        const { return false; }
    bool operator() (const UInt64 & l, const UInt64 & r)  const { return l < r; }
    bool operator() (const UInt64 &, const UInt128 &)     const { return true; }
    bool operator() (const UInt64 & l, const Int64 & r)   const { return accurate::lessOp(l, r); }
    bool operator() (const UInt64 & l, const Float64 & r) const { return accurate::lessOp(l, r); }
    bool operator() (const UInt64 &, const String &)      const { return true; }
    bool operator() (const UInt64 &, const Array &)       const { return true; }
    bool operator() (const UInt64 &, const Tuple &)       const { return true; }

    bool operator() (const UInt128 &, const Null &)          const { return false; }
    bool operator() (const UInt128 &, const UInt64)          const { return false; }
    bool operator() (const UInt128 & l, const UInt128 & r)   const { return l < r; }
    bool operator() (const UInt128 &, const Int64)           const { return false; }
    bool operator() (const UInt128 &, const Float64)         const { return false; }
    bool operator() (const UInt128 &, const String &)        const { return false; }
    bool operator() (const UInt128 &, const Array &)         const { return false; }
    bool operator() (const UInt128 &, const Tuple &)         const { return false; }

    bool operator() (const Int64 &, const Null &)        const { return false; }
    bool operator() (const Int64 & l, const UInt64 & r)  const { return accurate::lessOp(l, r); }
    bool operator() (const Int64 &, const UInt128 &)     const { return false; }
    bool operator() (const Int64 & l, const Int64 & r)   const { return l < r; }
    bool operator() (const Int64 & l, const Float64 & r) const { return accurate::lessOp(l, r); }
    bool operator() (const Int64 &, const String &)      const { return true; }
    bool operator() (const Int64 &, const Array &)       const { return true; }
    bool operator() (const Int64 &, const Tuple &)       const { return true; }

    bool operator() (const Float64 &, const Null &)        const { return false; }
    bool operator() (const Float64 & l, const UInt64 & r)  const { return accurate::lessOp(l, r); }
    bool operator() (const Float64, const UInt128 &)       const { return false; }
    bool operator() (const Float64 & l, const Int64 & r)   const { return accurate::lessOp(l, r); }
    bool operator() (const Float64 & l, const Float64 & r) const { return l < r; }
    bool operator() (const Float64 &, const String &)      const { return true; }
    bool operator() (const Float64 &, const Array &)       const { return true; }
    bool operator() (const Float64 &, const Tuple &)       const { return true; }

    bool operator() (const String &, const Null &)       const { return false; }
    bool operator() (const String &, const UInt64 &)     const { return false; }
    bool operator() (const String &, const UInt128 &)    const { return false; }
    bool operator() (const String &, const Int64 &)      const { return false; }
    bool operator() (const String &, const Float64 &)    const { return false; }
    bool operator() (const String & l, const String & r) const { return l < r; }
    bool operator() (const String &, const Array &)      const { return true; }
    bool operator() (const String &, const Tuple &)      const { return true; }

    bool operator() (const Array &, const Null &)       const { return false; }
    bool operator() (const Array &, const UInt64 &)     const { return false; }
    bool operator() (const Array &, const UInt128 &)    const { return false; }
    bool operator() (const Array &, const Int64 &)      const { return false; }
    bool operator() (const Array &, const Float64 &)    const { return false; }
    bool operator() (const Array &, const String &)     const { return false; }
    bool operator() (const Array & l, const Array & r)  const { return l < r; }
    bool operator() (const Array &, const Tuple &)      const { return false; }

    bool operator() (const Tuple &, const Null &)       const { return false; }
    bool operator() (const Tuple &, const UInt64 &)     const { return false; }
    bool operator() (const Tuple &, const UInt128 &)    const { return false; }
    bool operator() (const Tuple &, const Int64 &)      const { return false; }
    bool operator() (const Tuple &, const Float64 &)    const { return false; }
    bool operator() (const Tuple &, const String &)     const { return false; }
    bool operator() (const Tuple &, const Array &)      const { return false; }
    bool operator() (const Tuple & l, const Tuple & r)  const { return l < r; }
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

    bool operator() (UInt64 & x) const { x += get<UInt64>(rhs); return x != 0; }
    bool operator() (Int64 & x) const { x += get<Int64>(rhs); return x != 0; }
    bool operator() (Float64 & x) const { x += get<Float64>(rhs); return x != 0; }

    bool operator() (Null &) const { throw Exception("Cannot sum Nulls", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (String &) const { throw Exception("Cannot sum Strings", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (Array &) const { throw Exception("Cannot sum Arrays", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (UInt128 &) const { throw Exception("Cannot sum UUIDs", ErrorCodes::LOGICAL_ERROR); }
};

}
