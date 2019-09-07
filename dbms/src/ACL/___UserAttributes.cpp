#if 0
#include <ACL/UserAttributes.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_CAST;
}


void UserAttributes::authorize(const String & /*password_*/, const Poco::Net::IPAddress & /*address_*/)
{
    /*
    allowed_hosts.check(address_);
    password.check(password_);
    */
}


std::shared_ptr<IAccessAttributes> UserAttributes::clone() const
{
    auto result = std::make_shared<UserAttributes>();
    copyTo(*result);
    return result;
}


void UserAttributes::copyTo(RoleAttributes & dest) const
{
    RoleAttributes::copyTo(dest);
}


bool UserAttributes::isEqual(const IAccessAttributes & other) const
{
    if (!RoleAttributes::isEqual(other))
        return false;
    return true;
}


template <>
UserAttributes & IAccessAttributes::as<UserAttributes>()
{
    if (type == Type::USER)
        return static_cast<UserAttributes &>(*this);
    throw Exception("Access attributes has unexpected type", ErrorCodes::BAD_CAST);
}

template <>
const UserAttributes & IAccessAttributes::as<UserAttributes>() const
{
    if (type == Type::USER)
        return static_cast<const UserAttributes &>(*this);
    throw Exception("Access attributes has unexpected type", ErrorCodes::BAD_CAST);
}

}
#endif
