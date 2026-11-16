#pragma once

#include <Common/FieldVisitors.h>


namespace DB
{

/// Similar to FieldVisitorToString, but in ambiguous cases appends an explicit SQL-like
/// ::Type suffix to the printed value, so that parsing the result back produces a Field
/// of the exact same type as the original.
///
/// For example, '1734004810' on its own parses as a String, while '1734004810'::Decimal64(3)
/// parses as a Decimal64 with scale=3. The suffix is required to recover the field type.
///
/// Two modes:
///
/// skip_unambiguous_cast = true:
///   Prints the ::Type suffix only when the value would otherwise re-parse as a different
///   Field type than the original.
///   Examples: '1734004810'::Decimal64(3), 1234::Int64, '123e4567-e89b-12d3-a456-426614174000'::UUID
///
/// skip_unambiguous_cast = false:
///   Prints the ::Type suffix for every value that has a SQL type to attach.
///   Examples: 1234::UInt64, true::Bool, 5.3::Float64, 'abc'::String

class FieldVisitorToCastedLiteral : public StaticVisitor<String>
{
public:
    explicit FieldVisitorToCastedLiteral(bool skip_unambiguous_cast_ = true);

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

private:
    bool skip_unambiguous_cast;
};

}
