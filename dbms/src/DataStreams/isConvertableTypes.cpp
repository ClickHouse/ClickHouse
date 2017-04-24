#include <DataStreams/isConvertableTypes.h>

#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

static DataTypePtr removeNullable(DataTypePtr type)
{
    while (type->isNullable())
        type = typeid_cast<DataTypeNullable *>(type.get())->getNestedType();
    return type;
}

bool isConvertableTypes(const DataTypePtr & from, const DataTypePtr & to)
{
    auto from_nn = removeNullable(from);
    auto to_nn   = removeNullable(to);

    if ( dynamic_cast<IDataTypeEnum*>(to_nn.get()) &&
        !dynamic_cast<IDataTypeEnum*>(from_nn.get()))
    {
        if (dynamic_cast<DataTypeString*>(from_nn.get()))
            return true;
        if (from_nn->isNumeric())
            return true;
    }

    return from_nn->getName() == to_nn->getName();
}

}
