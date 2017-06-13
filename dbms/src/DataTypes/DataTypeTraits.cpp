#include <DataTypes/DataTypeTraits.h>

namespace DB { namespace DataTypeTraits {

const DataTypePtr & removeNullable(const DataTypePtr & type)
{
    if (type->isNullable())
    {
        const auto & nullable_type = static_cast<const DataTypeNullable &>(*type);
        return nullable_type.getNestedType();
    }
    else
        return type;
}

}}
