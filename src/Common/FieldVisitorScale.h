#pragma once

#include <Common/FieldVisitors.h>
#include <Common/FieldVisitorConvertToNumber.h>

namespace DB
{

/** Implements `*=` operation by number
 */
class FieldVisitorScale : public StaticVisitor<void>
{
private:
    Int32 rhs;

public:
    explicit FieldVisitorScale(Int32 rhs_);

    void operator() (Int64 & x) const;
    void operator() (UInt64 & x) const;
    void operator() (Float64 & x) const;
    void operator() (Null &) const;
    [[noreturn]] void operator() (String &) const;
    [[noreturn]] void operator() (Array &) const;
    [[noreturn]] void operator() (Tuple &) const;
    [[noreturn]] void operator() (Map &) const;
    [[noreturn]] void operator() (Object &) const;
    [[noreturn]] void operator() (UUID &) const;
    [[noreturn]] void operator() (IPv4 &) const;
    [[noreturn]] void operator() (IPv6 &) const;
    [[noreturn]] void operator() (AggregateFunctionStateData &) const;
    [[noreturn]] void operator() (CustomType &) const;
    [[noreturn]] void operator() (bool &) const;

    template <typename T>
    void operator() (DecimalField<T> & x) const { x = DecimalField<T>(x.getValue() * T(rhs), x.getScale()); }

    template <typename T>
    requires is_big_int_v<T>
    void operator() (T & x) const { x *= rhs; }
};

}
