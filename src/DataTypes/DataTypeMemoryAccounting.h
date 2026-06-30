#pragma once

#include <DataTypes/IDataType_fwd.h>

#include <cstddef>

namespace DB
{

class IDataType;

size_t getDataTypeShallowBytesAllocated(const IDataType & type);
size_t getDataTypeBytesAllocated(const IDataType & type);

}
