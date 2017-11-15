#include <Storages/VirtualColumnFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}


DataTypePtr VirtualColumnFactory::getType(const String & name)
{
    auto res = tryGetType(name);
    if (!res)
        throw Exception("There is no column " + name + " in table.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    return res;
}

bool VirtualColumnFactory::hasColumn(const String & name)
{
    return !!tryGetType(name);
}

DataTypePtr VirtualColumnFactory::tryGetType(const String & name)
{
    if (name == "_table")         return std::make_shared<DataTypeString>();
    if (name == "_part")          return std::make_shared<DataTypeString>();
    if (name == "_part_index")    return std::make_shared<DataTypeUInt64>();
    if (name == "_sample_factor") return std::make_shared<DataTypeFloat64>();
    return nullptr;
}

}
