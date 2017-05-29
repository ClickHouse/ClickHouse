#pragma once

#include <Core/Field.h>
#include <Core/AccurateComparison.h>
#include <common/DateLUT.h>


class SipHash;


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONVERT_TYPE;
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
typename std::decay<Visitor>::type::ResultType applyVisitor(Visitor && visitor, F && field)
{
    switch (field.getType())
    {
        case Field::Types::Null:    return visitor(field.template get<Null>());
        case Field::Types::UInt64:  return visitor(field.template get<UInt64>());
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
static typename std::decay<Visitor>::type::ResultType applyBinaryVisitorImpl(Visitor && visitor, F1 && field1, F2 && field2)
{
    switch (field2.getType())
    {
        case Field::Types::Null:    return visitor(field1, field2.template get<Null>());
        case Field::Types::UInt64:  return visitor(field1, field2.template get<UInt64>());
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
typename std::decay<Visitor>::type::ResultType applyVisitor(Visitor && visitor, F1 && field1, F2 && field2)
{
    switch (field1.getType())
    {
        case Field::Types::Null:
            return applyBinaryVisitorImpl(
                std::forward<Visitor>(visitor), field1.template get<Null>(), std::forward<F2>(field2));
        case Field::Types::UInt64:
            return applyBinaryVisitorImpl(
                std::forward<Visitor>(visitor), field1.template get<UInt64>(), std::forward<F2>(field2));
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
    T operator() (const Null & x) const
    {
        throw Exception("Cannot convert NULL to " + TypeName<T>::get(), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    T operator() (const String & x) const
    {
        throw Exception("Cannot convert String to " + TypeName<T>::get(), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    T operator() (const Array & x) const
    {
        throw Exception("Cannot convert Array to " + TypeName<T>::get(), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    T operator() (const Tuple & x) const
    {
        throw Exception("Cannot convert Tuple to " + TypeName<T>::get(), ErrorCodes::CANNOT_CONVERT_TYPE);
    }

    T operator() (const UInt64 & x) const { return x; }
    T operator() (const Int64 & x) const { return x; }
    T operator() (const Float64 & x) const { return x; }
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
    void operator() (const Int64 & x) const;
    void operator() (const Float64 & x) const;
    void operator() (const String & x) const;
    void operator() (const Array & x) const;
};


/** More precise comparison, used for index.
  * Differs from Field::operator< and Field::operator== in that it also compares values of different types.
  * Comparison rules are same as in FunctionsComparison (to be consistent with expression evaluation in query).
  */
class FieldVisitorAccurateEquals : public StaticVisitor<bool>
{
public:
    bool operator() (const Null & l, const Null & r)        const { return true; }
    bool operator() (const Null & l, const UInt64 & r)      const { return false; }
    bool operator() (const Null & l, const Int64 & r)       const { return false; }
    bool operator() (const Null & l, const Float64 & r)     const { return false; }
    bool operator() (const Null & l, const String & r)      const { return false; }
    bool operator() (const Null & l, const Array & r)       const { return false; }
    bool operator() (const Null & l, const Tuple & r)       const { return false; }

    bool operator() (const UInt64 & l, const Null & r)      const { return false; }
    bool operator() (const UInt64 & l, const UInt64 & r)    const { return l == r; }
    bool operator() (const UInt64 & l, const Int64 & r)     const { return accurate::equalsOp(l, r); }
    bool operator() (const UInt64 & l, const Float64 & r)   const { return accurate::equalsOp(l, r); }
    bool operator() (const UInt64 & l, const String & r)    const { return false; }
    bool operator() (const UInt64 & l, const Array & r)     const { return false; }
    bool operator() (const UInt64 & l, const Tuple & r)     const { return false; }

    bool operator() (const Int64 & l, const Null & r)       const { return false; }
    bool operator() (const Int64 & l, const UInt64 & r)     const { return accurate::equalsOp(l, r); }
    bool operator() (const Int64 & l, const Int64 & r)      const { return l == r; }
    bool operator() (const Int64 & l, const Float64 & r)    const { return accurate::equalsOp(l, r); }
    bool operator() (const Int64 & l, const String & r)     const { return false; }
    bool operator() (const Int64 & l, const Array & r)      const { return false; }
    bool operator() (const Int64 & l, const Tuple & r)      const { return false; }

    bool operator() (const Float64 & l, const Null & r)     const { return false; }
    bool operator() (const Float64 & l, const UInt64 & r)   const { return accurate::equalsOp(l, r); }
    bool operator() (const Float64 & l, const Int64 & r)    const { return accurate::equalsOp(l, r); }
    bool operator() (const Float64 & l, const Float64 & r)  const { return l == r; }
    bool operator() (const Float64 & l, const String & r)   const { return false; }
    bool operator() (const Float64 & l, const Array & r)    const { return false; }
    bool operator() (const Float64 & l, const Tuple & r)    const { return false; }

    bool operator() (const String & l, const Null & r)      const { return false; }
    bool operator() (const String & l, const UInt64 & r)    const { return false; }
    bool operator() (const String & l, const Int64 & r)     const { return false; }
    bool operator() (const String & l, const Float64 & r)   const { return false; }
    bool operator() (const String & l, const String & r)    const { return l == r; }
    bool operator() (const String & l, const Array & r)     const { return false; }
    bool operator() (const String & l, const Tuple & r)     const { return false; }

    bool operator() (const Array & l, const Null & r)       const { return false; }
    bool operator() (const Array & l, const UInt64 & r)     const { return false; }
    bool operator() (const Array & l, const Int64 & r)      const { return false; }
    bool operator() (const Array & l, const Float64 & r)    const { return false; }
    bool operator() (const Array & l, const String & r)     const { return false; }
    bool operator() (const Array & l, const Array & r)      const { return l == r; }
    bool operator() (const Array & l, const Tuple & r)      const { return false; }

    bool operator() (const Tuple & l, const Null & r)       const { return false; }
    bool operator() (const Tuple & l, const UInt64 & r)     const { return false; }
    bool operator() (const Tuple & l, const Int64 & r)      const { return false; }
    bool operator() (const Tuple & l, const Float64 & r)    const { return false; }
    bool operator() (const Tuple & l, const String & r)     const { return false; }
    bool operator() (const Tuple & l, const Array & r)      const { return false; }
    bool operator() (const Tuple & l, const Tuple & r)      const { return l == r; }
};

class FieldVisitorAccurateLess : public StaticVisitor<bool>
{
public:
    bool operator() (const Null & l, const Null & r)        const { return false; }
    bool operator() (const Null & l, const UInt64 & r)      const { return true; }
    bool operator() (const Null & l, const Int64 & r)       const { return true; }
    bool operator() (const Null & l, const Float64 & r)     const { return true; }
    bool operator() (const Null & l, const String & r)      const { return true; }
    bool operator() (const Null & l, const Array & r)       const { return true; }
    bool operator() (const Null & l, const Tuple & r)       const { return true; }

    bool operator() (const UInt64 & l, const Null & r)      const { return false; }
    bool operator() (const UInt64 & l, const UInt64 & r)    const { return l < r; }
    bool operator() (const UInt64 & l, const Int64 & r)     const { return accurate::lessOp(l, r); }
    bool operator() (const UInt64 & l, const Float64 & r)   const { return accurate::lessOp(l, r); }
    bool operator() (const UInt64 & l, const String & r)    const { return true; }
    bool operator() (const UInt64 & l, const Array & r)     const { return true; }
    bool operator() (const UInt64 & l, const Tuple & r)     const { return true; }

    bool operator() (const Int64 & l, const Null & r)       const { return false; }
    bool operator() (const Int64 & l, const UInt64 & r)     const { return accurate::lessOp(l, r); }
    bool operator() (const Int64 & l, const Int64 & r)      const { return l < r; }
    bool operator() (const Int64 & l, const Float64 & r)    const { return accurate::lessOp(l, r); }
    bool operator() (const Int64 & l, const String & r)     const { return true; }
    bool operator() (const Int64 & l, const Array & r)      const { return true; }
    bool operator() (const Int64 & l, const Tuple & r)      const { return true; }

    bool operator() (const Float64 & l, const Null & r)     const { return false; }
    bool operator() (const Float64 & l, const UInt64 & r)   const { return accurate::lessOp(l, r); }
    bool operator() (const Float64 & l, const Int64 & r)    const { return accurate::lessOp(l, r); }
    bool operator() (const Float64 & l, const Float64 & r)  const { return l < r; }
    bool operator() (const Float64 & l, const String & r)   const { return true; }
    bool operator() (const Float64 & l, const Array & r)    const { return true; }
    bool operator() (const Float64 & l, const Tuple & r)    const { return true; }

    bool operator() (const String & l, const Null & r)      const { return false; }
    bool operator() (const String & l, const UInt64 & r)    const { return false; }
    bool operator() (const String & l, const Int64 & r)     const { return false; }
    bool operator() (const String & l, const Float64 & r)   const { return false; }
    bool operator() (const String & l, const String & r)    const { return l < r; }
    bool operator() (const String & l, const Array & r)     const { return true; }
    bool operator() (const String & l, const Tuple & r)     const { return true; }

    bool operator() (const Array & l, const Null & r)       const { return false; }
    bool operator() (const Array & l, const UInt64 & r)     const { return false; }
    bool operator() (const Array & l, const Int64 & r)      const { return false; }
    bool operator() (const Array & l, const Float64 & r)    const { return false; }
    bool operator() (const Array & l, const String & r)     const { return false; }
    bool operator() (const Array & l, const Array & r)      const { return l < r; }
    bool operator() (const Array & l, const Tuple & r)      const { return false; }

    bool operator() (const Tuple & l, const Null & r)       const { return false; }
    bool operator() (const Tuple & l, const UInt64 & r)     const { return false; }
    bool operator() (const Tuple & l, const Int64 & r)      const { return false; }
    bool operator() (const Tuple & l, const Float64 & r)    const { return false; }
    bool operator() (const Tuple & l, const String & r)     const { return false; }
    bool operator() (const Tuple & l, const Array & r)      const { return false; }
    bool operator() (const Tuple & l, const Tuple & r)      const { return l < r; }
};

}
