#pragma once

#include <functional>
#include <memory>
#include <vector>

#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = VectorWithMemoryTracking<DataTypePtr>;

/// Child-type replacement used by IDataType::transformChildren.
using ChildTransform = std::function<DataTypePtr(const DataTypePtr &)>;

struct DataTypeWithConstInfo
{
    DataTypePtr type;
    bool is_const;
};

using DataTypesWithConstInfo = VectorWithMemoryTracking<DataTypeWithConstInfo>;

}
