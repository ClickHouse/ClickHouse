#pragma once

#include <Common/FieldVisitors.h>

class SipHash;

namespace DB
{

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
    void operator() (const UInt256 & x) const;
    void operator() (const Int64 & x) const;
    void operator() (const Int128 & x) const;
    void operator() (const Int256 & x) const;
    void operator() (const UUID & x) const;
    void operator() (const Float64 & x) const;
    void operator() (const String & x) const;
    void operator() (const Array & x) const;
    void operator() (const Tuple & x) const;
    void operator() (const Map & x) const;
    void operator() (const DecimalField<Decimal32> & x) const;
    void operator() (const DecimalField<Decimal64> & x) const;
    void operator() (const DecimalField<Decimal128> & x) const;
    void operator() (const DecimalField<Decimal256> & x) const;
    void operator() (const AggregateFunctionStateData & x) const;
};

}
