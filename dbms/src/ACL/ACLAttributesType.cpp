#include <ACL/ACLAttributesType.h>
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


const ACLAttributesTypeInfo & ACLAttributesTypeInfo::get(ACLAttributesType type)
{
    static const ACLAttributesTypeInfo infos[] =
    {
        { ACLAttributesType::USER, "User", ErrorCodes::USER_NOT_FOUND, ErrorCodes::USER_ALREADY_EXISTS, ACLAttributesType::ROLE },
        { ACLAttributesType::ROLE, "Role", ErrorCodes::ROLE_NOT_FOUND, ErrorCodes::ROLE_ALREADY_EXISTS, {} },
        { ACLAttributesType::QUOTA, "Quota", ErrorCodes::QUOTA_NOT_FOUND, ErrorCodes::QUOTA_ALREADY_EXISTS, {} },
        { ACLAttributesType::ROW_FILTER_POLICY, "Policy", ErrorCodes::ROW_FILTER_POLICY_NOT_FOUND, ErrorCodes::ROW_FILTER_POLICY_ALREADY_EXISTS, {} },
    };
    assert(static_cast<size_t>(type) < std::size(infos));
    return infos[static_cast<size_t>(type)];
}
}
