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
    String operator() (const NumberLiteral & x) const;
    String operator() (const bool & x) const;
};

/** Same as `FieldVisitorToString`, but a wide integer is written without quotes.
  * A setting or config value is not a SQL literal: the parsers read bare digits back as the same
  * wide integer, while the quotes turn the value into a `String`.
  */
class FieldVisitorToSettingValueString : public FieldVisitorToString
{
public:
    using FieldVisitorToString::operator();
    String operator() (const UInt128 & x) const;
    String operator() (const UInt256 & x) const;
    String operator() (const Int128 & x) const;
    String operator() (const Int256 & x) const;
};

/// Get value from field and convert it to string.
/// Also remove quotes from strings.
String convertFieldToString(const Field & field);

/// Same as `convertFieldToString`, for a value stored as setting or config text.
String convertFieldToSettingValueString(const Field & field);

/// Convert Object to String without quotes.
String convertObjectToString(const Object & object);

}
