#if 0
#include <ACL/ACLAttributesType.h>
//#include <ACL/Role.h>
#include <cassert>


namespace DB
{
namespace ErrorCodes
{
    extern const int USER_NOT_FOUND;
    extern const int USER_ALREADY_EXISTS;
    extern const int ROLE_NOT_FOUND;
    extern const int ROLE_ALREADY_EXISTS;
    extern const int QUOTA_NOT_FOUND;
    extern const int QUOTA_ALREADY_EXISTS;
    extern const int ROW_FILTER_POLICY_NOT_FOUND;
    extern const int ROW_FILTER_POLICY_ALREADY_EXISTS;
}


const ACLAttributesType ACLAttributesType::USER
    = {"User", {}, &ACLAttributesType::ROLE, ErrorCodes::USER_NOT_FOUND, ErrorCodes::USER_ALREADY_EXISTS, 0};


bool ACLAttributesType::isDerived(const ACLAttributesType & base_type_) const
{
    const ACLAttributesType * type = this;
    while (*type != base_type_)
    {
        if (!type->base_type)
            return false;
        type = type->base_type;
    }
    return true;
}


/*
const ACLAttributesTypeInfo & ACLAttributesTypeInfo::get(ACLAttributesType type)
{
    static const ACLAttributesTypeInfo infos[] =
    {
        { ACLAttributesType::USER, "User", {}, ACLAttributesType::ROLE, ErrorCodes::USER_NOT_FOUND, ErrorCodes::USER_ALREADY_EXISTS },
        { ACLAttributesType::ROLE, "Role",
          []() -> std::shared_ptr<IAttributes> { return std::make_shared<Role::Attributes>(); },
          {}, ErrorCodes::ROLE_NOT_FOUND, ErrorCodes::ROLE_ALREADY_EXISTS },
        { ACLAttributesType::QUOTA, "Quota", {}, {}, ErrorCodes::QUOTA_NOT_FOUND, ErrorCodes::QUOTA_ALREADY_EXISTS },
        { ACLAttributesType::ROW_FILTER_POLICY, "Policy", {}, {}, ErrorCodes::ROW_FILTER_POLICY_NOT_FOUND, ErrorCodes::ROW_FILTER_POLICY_ALREADY_EXISTS },
    };
    assert(static_cast<size_t>(type) < std::size(infos));
    return infos[static_cast<size_t>(type)];
}


ACLAttributesType ACLAttributesTypeInfo::getRootType(ACLAttributesType type)
{
    while (true)
    {
        const auto & info = get(type);
        if (!info.base_type)
            return type;
        type = *info.base_type;
    }
}
*/
}
#endif
