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

/// Maps a child data type to a replacement, used to rebuild a type tree generically.
/// See IDataType::transformChildren.
using ChildTransform = std::function<DataTypePtr(const DataTypePtr &)>;

struct DataTypeWithConstInfo
{
    DataTypePtr type;
    bool is_const;
};

using DataTypesWithConstInfo = VectorWithMemoryTracking<DataTypeWithConstInfo>;

}
