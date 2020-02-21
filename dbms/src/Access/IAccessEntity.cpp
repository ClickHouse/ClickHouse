#include <Access/IAccessEntity.h>
#include <Access/Quota.h>
#include <Access/RowPolicy.h>
#include <common/demangle.h>


namespace DB
{
String IAccessEntity::getTypeName(std::type_index type)
{
    if (type == typeid(Quota))
        return "Quota";
    if (type == typeid(RowPolicy))
        return "Row policy";
    return demangle(type.name());
}

bool IAccessEntity::equal(const IAccessEntity & other) const
{
    return (full_name == other.full_name) && (getType() == other.getType());
}
}
