#pragma once

#include <memory>
#include <vector>

#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = VectorWithMemoryTracking<DataTypePtr>;

class ColumnConst;

struct DataTypeWithConstInfo
{
    DataTypePtr type;
    bool is_const;
    /// The column of a constant argument when its value is known, otherwise nullptr.
    const ColumnConst * column = nullptr;
};

using DataTypesWithConstInfo = VectorWithMemoryTracking<DataTypeWithConstInfo>;

}
