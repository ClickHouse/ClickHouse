#include <Account/User.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int USER_NOT_FOUND;
    extern const int USER_ALREADY_EXISTS;
    extern const int NOT_GRANTED_ROLE_SET;
}

using Operation = IAccessControlElement::Operation;


bool User2::Attributes::equal(const IAccessControlElement::Attributes & other) const
{
    if (!Role::Attributes::equal(other))
        return false;
    //const auto * o = dynamic_cast<const Attributes *>(&other);
    return true;
}


std::shared_ptr<IAccessControlElement::Attributes> User2::Attributes::clone() const
{
    auto result = std::make_shared<Attributes>();
    *result == *this;
    return result;
}


Operation User2::setDefaultRolesOp(const std::vector<Role> & roles) const
{
    return prepareOperation([roles](Attributes & attrs)
    {
        for (const auto & role : roles)
            if (!attrs.granted_roles.count(role.getID()))
                throw Exception("Role " + role.getName() + " is not granted, only granted roles can be set", ErrorCodes::NOT_GRANTED_ROLE_SET);
        for (auto & granted_id_with_settings : attrs.granted_roles)
        {
            auto & settings = granted_id_with_settings.second;
            settings.enabled_by_default = false;
        }
        for (const auto & role : roles)
            attrs.granted_roles[role.getID()].enabled_by_default = true;
    });
}


std::vector<Role> User2::getDefaultRoles() const
{
    auto attrs = getAttributes();
    std::vector<Role> result;
    result.reserve(attrs->granted_roles.size());
    for (const auto & [granted_id, settings] : attrs->granted_roles)
    {
        if (settings.enabled_by_default)
            result.push_back({granted_id, getManager()});
    }
    return result;
}


Operation User2::prepareOperation(const std::function<void(Attributes &)> & fn) const
{
    return prepareOperationImpl<Attributes>(fn);
}

const String & User2::getTypeName() const
{
    static const String type_name = "user";
    return type_name;
}

int User2::getNotFoundErrorCode() const
{
    return ErrorCodes::USER_NOT_FOUND;
}

int User2::getAlreadyExistsErrorCode() const
{
    return ErrorCodes::USER_ALREADY_EXISTS;
}
}
