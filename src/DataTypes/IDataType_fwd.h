#pragma once

#include <memory>
#include <vector>

#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = VectorWithMemoryTracking<DataTypePtr>;

struct DataTypeWithConstInfo
{
    DataTypePtr type;
    bool is_const;
};

using DataTypesWithConstInfo = VectorWithMemoryTracking<DataTypeWithConstInfo>;

}
