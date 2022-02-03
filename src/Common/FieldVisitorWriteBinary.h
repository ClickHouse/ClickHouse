#pragma once

#include <Common/FieldVisitors.h>

namespace DB
{

class FieldVisitorWriteBinary
{
public:
    void operator() (const Null & x, WriteBuffer & buf) const;
    void operator() (const UInt64 & x, WriteBuffer & buf) const;
    void operator() (const UInt128 & x, WriteBuffer & buf) const;
    void operator() (const UInt256 & x, WriteBuffer & buf) const;
    void operator() (const Int64 & x, WriteBuffer & buf) const;
    void operator() (const Int128 & x, WriteBuffer & buf) const;
    void operator() (const Int256 & x, WriteBuffer & buf) const;
    void operator() (const UUID & x, WriteBuffer & buf) const;
    void operator() (const Float64 & x, WriteBuffer & buf) const;
    void operator() (const String & x, WriteBuffer & buf) const;
    void operator() (const Array & x, WriteBuffer & buf) const;
    void operator() (const Tuple & x, WriteBuffer & buf) const;
    void operator() (const Map & x, WriteBuffer & buf) const;
    void operator() (const DecimalField<Decimal32> & x, WriteBuffer & buf) const;
    void operator() (const DecimalField<Decimal64> & x, WriteBuffer & buf) const;
    void operator() (const DecimalField<Decimal128> & x, WriteBuffer & buf) const;
    void operator() (const DecimalField<Decimal256> & x, WriteBuffer & buf) const;
    void operator() (const AggregateFunctionStateData & x, WriteBuffer & buf) const;
};

}

