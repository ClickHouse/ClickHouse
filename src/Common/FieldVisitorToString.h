#pragma once

#include <Common/FieldVisitors.h>

namespace DB
{

/** Prints Field as literal in SQL query */
class FieldVisitorToString : public StaticVisitor<String>
{
public:
    String operator() (const Null & x) const;
    String operator() (const UInt64 & x) const;
    String operator() (const UInt128 & x) const;
    String operator() (const UInt256 & x) const;
    String operator() (const Int64 & x) const;
    String operator() (const Int128 & x) const;
    String operator() (const Int256 & x) const;
    String operator() (const UUID & x) const;
    String operator() (const IPv4 & x) const;
    String operator() (const IPv6 & x) const;
    String operator() (const Float64 & x) const;
    String operator() (const String & x) const;
    String operator() (const Array & x) const;
    String operator() (const Tuple & x) const;
    String operator() (const Map & x) const;
    String operator() (const Object & x) const;
    String operator() (const DecimalField<Decimal32> & x) const;
    String operator() (const DecimalField<Decimal64> & x) const;
    String operator() (const DecimalField<Decimal128> & x) const;
    String operator() (const DecimalField<Decimal256> & x) const;
    String operator() (const Decimal32 & x, UInt32 scale) const;
    String operator() (const Decimal64 & x, UInt32 scale) const;
    String operator() (const Decimal128 & x, UInt32 scale) const;
    String operator() (const Decimal256 & x, UInt32 scale) const;
    String operator() (const AggregateFunctionStateData & x) const;
    String operator() (const CustomType & x) const;
    String operator() (const bool & x) const;
};

/// Get value from field and convert it to string.
/// Also remove quotes from strings.
String convertFieldToString(const Field & field);

/// Convert Object to String without quotes.
String convertObjectToString(const Object & object);

}
