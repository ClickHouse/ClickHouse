#include <DataStreams/isConvertableTypes.h>

#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

static DataTypePtr removeNullable(const DataTypePtr & type)
{
    if (type->isNullable())
        return typeid_cast<DataTypeNullable *>(type.get())->getNestedType();
    return type;
}

bool isConvertableTypes(const DataTypePtr & from, const DataTypePtr & to)
{
    auto from_nn = removeNullable(from);
    auto to_nn   = removeNullable(to);

    if ( dynamic_cast<const IDataTypeEnum *>(to_nn.get()) &&
        !dynamic_cast<const IDataTypeEnum *>(from_nn.get()))
    {
        if (typeid_cast<const DataTypeString *>(from_nn.get()))
            return true;
        if (from_nn->isNumeric())
            return true;
    }

    return from_nn->equals(*to_nn);
}

}
