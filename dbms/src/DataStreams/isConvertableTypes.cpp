#include <DataStreams/isConvertableTypes.h>

#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Common/typeid_cast.h>

namespace DB
{

bool isConvertableTypes(const DataTypePtr & from, const DataTypePtr & to)
{
    auto from_nn = removeNullable(from);
    auto to_nn   = removeNullable(to);

    if ( dynamic_cast<const IDataTypeEnum *>(to_nn.get()) &&
        !dynamic_cast<const IDataTypeEnum *>(from_nn.get()))
    {
        if (from_nn->isString() || from_nn->isInteger())
            return true;
    }

    return from_nn->equals(*to_nn);
}

}
