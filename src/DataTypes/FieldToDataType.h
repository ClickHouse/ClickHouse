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
    DataTypePtr operator() (const AggregateFunctionStateData & x) const;
    DataTypePtr operator() (const CustomType & x) const;
    DataTypePtr operator() (const UInt256 & x) const;
    DataTypePtr operator() (const Int256 & x) const;
    DataTypePtr operator() (const bool & x) const;

private:
    // The conditions for converting UInt64 to Int64 are:
    // 1. The existence of Int.
    // 2. The existence of UInt64, and the UInt64 value must be <= Int64.max.
    void checkUInt64ToIn64Conversion(bool& has_signed_int, bool& uint64_convert_possible, const DataTypePtr & type, const Field & elem) const;

    // Convert the UInt64 type to Int64 in order to cover other signed_integer types
    // and obtain the least super type of all ints.
    void convertUInt64ToInt64IfPossible(DataTypes & data_types) const;
};

}
