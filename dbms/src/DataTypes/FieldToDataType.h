#pragma once

#include <Common/FieldVisitors.h>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;


/** For a given Field returns the minimum data type that allows this value to be stored.
  * Note that you still have to convert Field to corresponding data type before inserting to columns
  *  (for example, this is necessary to convert elements of Array to common type).
  */
class FieldToDataType : public StaticVisitor<DataTypePtr>
{
public:
    DataTypePtr operator() (const Null & x) const;
    DataTypePtr operator() (const UInt64 & x) const;
    DataTypePtr operator() (const Int64 & x) const;
    DataTypePtr operator() (const Float64 & x) const;
    DataTypePtr operator() (const String & x) const;
    DataTypePtr operator() (const Array & x) const;
    DataTypePtr operator() (const Tuple & x) const;
};

}

