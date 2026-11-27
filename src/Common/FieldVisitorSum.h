#pragma once

#include <Common/FieldVisitors.h>
#include <Common/FieldVisitorConvertToNumber.h>


namespace DB
{

/** Implements `+=` operation.
 *  Returns false if the result is zero.
 */
class FieldVisitorSum : public StaticVisitor<bool>
{
private:
    const Field & rhs;
public:
    explicit FieldVisitorSum(const Field & rhs_);

    // We can add all ints as unsigned regardless of their actual signedness.
    bool operator() (Int64 & x) const;
    bool operator() (UInt64 & x) const;
    bool operator() (Float64 & x) const;
    bool operator() (Null &) const;
    bool operator() (String &) const;
    bool operator() (Array &) const;
    bool operator() (Tuple &) const;
    bool operator() (Map &) const;
    bool operator() (Object &) const;
    bool operator() (UUID &) const;
    bool operator() (IPv4 &) const;
    bool operator() (IPv6 &) const;
    bool operator() (AggregateFunctionStateData &) const;
    bool operator() (CustomType &) const;
    bool operator() (bool &) const;

    template <typename T>
    bool operator() (DecimalField<T> & x) const
    {
        x += rhs.safeGet<DecimalField<T>>();
        return x.getValue() != T(0);
    }

    template <typename T>
    requires is_big_int_v<T>
    bool operator() (T & x) const
    {
        x += applyVisitor(FieldVisitorConvertToNumber<T>(), rhs);
        return x != T(0);
    }
};

}
