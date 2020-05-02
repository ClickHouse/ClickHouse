#include <Access/IAccessEntity.h>
#include <Access/Quota.h>
#include <Access/RowPolicy.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/SettingsProfile.h>
#include <common/demangle.h>


namespace DB
{
String IAccessEntity::getTypeName(std::type_index type)
{
    if (type == typeid(User))
        return "User";
    if (type == typeid(Quota))
        return "Quota";
    if (type == typeid(RowPolicy))
        return "Row policy";
    if (type == typeid(Role))
        return "Role";
    if (type == typeid(SettingsProfile))
        return "Settings profile";
    return demangle(type.name());
}


const char * IAccessEntity::getKeyword(std::type_index type)
{
    if (type == typeid(User))
        return "USER";
    if (type == typeid(Quota))
        return "QUOTA";
    if (type == typeid(RowPolicy))
        return "ROW POLICY";
    if (type == typeid(Role))
        return "ROLE";
    if (type == typeid(SettingsProfile))
        return "SETTINGS PROFILE";
    __builtin_unreachable();
}


bool IAccessEntity::equal(const IAccessEntity & other) const
{
    return (name == other.name) && (getType() == other.getType());
}
}
