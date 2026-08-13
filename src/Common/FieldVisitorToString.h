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

/** Same as FieldVisitorToString, but escapes string literals for a PostgreSQL source and
  * recurses into containers with the same escaping. Strings are emitted as an E'...' escape-string
  * constant with both the single quote and the backslash doubled ('a''b', 'a\\b'). This is safe
  * irrespective of the remote session's standard_conforming_strings: in an E'' constant PostgreSQL
  * always treats backslash as an escape character, so no embedded byte can terminate the literal.
  * Used when pushing predicates down to a PostgreSQL source; a plain '...' literal with only the
  * quote doubled would be exploitable when standard_conforming_strings is off.
  */
class FieldVisitorToStringPostgreSQL : public StaticVisitor<String>
{
public:
    /// Only the scalar String and the container types that can carry a String into a
    /// predicate pushed down to PostgreSQL are overridden. Predicate pushdown
    /// (transformQueryForExternalDatabase) emits a scalar or a Tuple (an IN-list); Array and
    /// Map appear only nested inside such a Tuple, so they must recurse with PostgreSQL escaping too.
    /// Every other type falls through to `regular` (backslash escaping), which is safe because it
    /// cannot appear as a pushed-down literal: there is no Object literal syntax, and a PostgreSQL
    /// source exposes no Object/JSON/Map/AggregateFunction columns (json/jsonb map to String), so no
    /// Object/CustomType/AggregateFunctionStateData value ever reaches this visitor from a pushdown.
    template <typename T>
    String operator() (const T & x) const { return regular(x); }

    String operator() (const String & x) const;
    String operator() (const Array & x) const;
    String operator() (const Tuple & x) const;
    String operator() (const Map & x) const;

private:
    FieldVisitorToString regular;
};

/// Get value from field and convert it to string.
/// Also remove quotes from strings.
String convertFieldToString(const Field & field);

/// Convert Object to String without quotes.
String convertObjectToString(const Object & object);

}
