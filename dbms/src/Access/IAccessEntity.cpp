#include <Access/IAccessEntity.h>
#include <common/demangle.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_ENTITY_NOT_FOUND;
}


String IAccessEntity::getTypeName(std::type_index type)
{
    return demangle(type.name());
}

bool IAccessEntity::equal(const IAccessEntity & other) const
{
    return (full_name == other.full_name) && (getType() == other.getType());
}
}
