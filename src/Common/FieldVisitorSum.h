#pragma once

#include <Common/FieldVisitors.h>


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
    bool operator() (UUID &) const;
    bool operator() (AggregateFunctionStateData &) const;

    template <typename T>
    bool operator() (DecimalField<T> & x) const
    {
        x += get<DecimalField<T>>(rhs);
        return x.getValue() != T(0);
    }

    template <typename T, typename = std::enable_if_t<is_big_int_v<T>> >
    bool operator() (T & x) const
    {
        x += rhs.reinterpret<T>();
        return x != T(0);
    }
};

}

