#include <Access/IAccessEntity.h>
#include <Access/Quota.h>
#include <common/demangle.h>


namespace DB
{
String IAccessEntity::getTypeName(std::type_index type)
{
    if (type == typeid(Quota))
        return "Quota";
    return demangle(type.name());
}

bool IAccessEntity::equal(const IAccessEntity & other) const
{
    return (full_name == other.full_name) && (getType() == other.getType());
}
}
