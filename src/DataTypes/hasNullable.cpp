#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/hasNullable.h>

namespace DB
{

bool hasNullable(const DataTypePtr & type)
{
    if (type->isNullable() || type->isLowCardinalityNullable())
        return true;

    if (const DataTypeArray * type_array = typeid_cast<const DataTypeArray *>(type.get()))
        return hasNullable(type_array->getNestedType());
    else if (const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        for (const auto & subtype : type_tuple->getElements())
        {
            if (hasNullable(subtype))
                return true;
        }
        return false;
    }
    else if (const DataTypeMap * type_map = typeid_cast<const DataTypeMap *>(type.get()))
    {
        // Key type cannot be nullable. We only check value type.
        return hasNullable(type_map->getValueType());
    }
    return false;
}

}
