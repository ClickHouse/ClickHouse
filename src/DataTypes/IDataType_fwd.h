#pragma once

#include <memory>
#include <vector>

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

struct DataTypeWithConstInfo
{
    DataTypePtr type;
    bool is_const;
};

using DataTypesWithConstInfo = std::vector<DataTypeWithConstInfo>;

}
