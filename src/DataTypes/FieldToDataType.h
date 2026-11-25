#pragma once

#include <memory>
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/FieldVisitors.h>
#include <DataTypes/getLeastSupertype.h>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;


/** For a given Field returns the minimum data type that allows this value to be stored.
  * Note that you still have to convert Field to corresponding data type before inserting to columns
  *  (for example, this is necessary to convert elements of Array to common type).
  */
template <LeastSupertypeOnError on_error = LeastSupertypeOnError::Throw>
class FieldToDataType : public StaticVisitor<DataTypePtr>
{
public:
    DataTypePtr operator() (const Null & x) const;
    DataTypePtr operator() (const UInt64 & x) const;
    DataTypePtr operator() (const UInt128 & x) const;
    DataTypePtr operator() (const Int64 & x) const;
    DataTypePtr operator() (const Int128 & x) const;
    DataTypePtr operator() (const UUID & x) const;
    DataTypePtr operator() (const IPv4 & x) const;
    DataTypePtr operator() (const IPv6 & x) const;
    DataTypePtr operator() (const Float64 & x) const;
    DataTypePtr operator() (const String & x) const;
    DataTypePtr operator() (const Array & x) const;
    DataTypePtr operator() (const Tuple & tuple) const;
    DataTypePtr operator() (const Map & map) const;
    DataTypePtr operator() (const Object & map) const;
    DataTypePtr operator() (const DecimalField<Decimal32> & x) const;
    DataTypePtr operator() (const DecimalField<Decimal64> & x) const;
    DataTypePtr operator() (const DecimalField<Decimal128> & x) const;
    DataTypePtr operator() (const DecimalField<Decimal256> & x) const;
    DataTypePtr operator() (const Decimal32 & x, UInt32 scale) const;
    DataTypePtr operator() (const Decimal64 & x, UInt32 scale) const;
    DataTypePtr operator() (const Decimal128 & x, UInt32 scale) const;
    DataTypePtr operator() (const Decimal256 & x, UInt32 scale) const;
    DataTypePtr operator() (const AggregateFunctionStateData & x) const;
    DataTypePtr operator() (const CustomType & x) const;
    DataTypePtr operator() (const NumberLiteral & x) const;
    DataTypePtr operator() (const UInt256 & x) const;
    DataTypePtr operator() (const Int256 & x) const;
    DataTypePtr operator() (const bool & x) const;

private:
    /// Data types are immutable, so the elements of one value share one instance of each scalar type.
    /// A visitor is created per value and is not for concurrent use.
    mutable DataTypePtr uint8_type;
    mutable DataTypePtr uint16_type;
    mutable DataTypePtr uint32_type;
    mutable DataTypePtr uint64_can_be_signed_type;
    mutable DataTypePtr uint64_type;
    mutable DataTypePtr int8_type;
    mutable DataTypePtr int16_type;
    mutable DataTypePtr int32_type;
    mutable DataTypePtr int64_type;
    mutable DataTypePtr float64_type;
    mutable DataTypePtr string_type;
};

FieldToDataType() -> FieldToDataType<LeastSupertypeOnError::Throw>;

}
