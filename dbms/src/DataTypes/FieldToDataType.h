#pragma once

#include <Common/FieldVisitors.h>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;


/** For a given value, Field returns the minimum data type that allows this value to be stored.
  * In case Field is an array, converts all elements to a common type.
  */
class FieldToDataType : public StaticVisitor<DataTypePtr>
{
public:
    DataTypePtr operator() (Null & x) const;
    DataTypePtr operator() (UInt64 & x) const;
    DataTypePtr operator() (Int64 & x) const;
    DataTypePtr operator() (Float64 & x) const;
    DataTypePtr operator() (String & x) const;
    DataTypePtr operator() (Array & x) const;
    DataTypePtr operator() (Tuple & x) const;
};

}

