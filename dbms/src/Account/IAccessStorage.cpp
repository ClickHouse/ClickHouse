#include <Account/IAccessStorage.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int USER_ALREADY_EXISTS;
    extern const int ROLE_ALREADY_EXISTS;
    extern const int RLS_POLICY_ALREADY_EXISTS;
    extern const int ACCESS_ATTRIBUTES_NOT_FOUND;
}


void IAccessStorage::throwAlreadyExists(const String & name, Type type)
{
    switch (type)
    {
        case Type::ROLE: throw Exception("Role '" + name + "' already exists", ErrorCodes::ROLE_ALREADY_EXISTS);
        case Type::USER: throw Exception("User '" + name + "' already exists", ErrorCodes::USER_ALREADY_EXISTS);
        case Type::RLS_POLICY: throw Exception("Row-level security policy '" + name + "' already exists", ErrorCodes::RLS_POLICY_ALREADY_EXISTS);
    }
    __builtin_unreachable();
}


void IAccessStorage::throwNotFound(const UUID & id)
{
    throw Exception("Access attributes " + toString(id) + " not found", ErrorCodes::ACCESS_ATTRIBUTES_NOT_FOUND);
}
}
