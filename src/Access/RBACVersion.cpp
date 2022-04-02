#include <Access/RBACVersion.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_RBAC_VERSION;
}

namespace RBACVersion
{
    extern const UInt64 INITIAL = 1;
    extern const UInt64 LATEST = 2;
    extern const UInt64 NO_FULL_SYSTEM_DB_GRANTS = 2;
    const char SETTING_NAME[] = "rbac_version";
}

void checkRBACVersion(UInt64 rbac_version)
{
    if (rbac_version < RBACVersion::INITIAL)
        throw Exception(ErrorCodes::INVALID_RBAC_VERSION, "{} {} is too old", RBACVersion::SETTING_NAME, rbac_version);
    if (rbac_version > RBACVersion::LATEST)
        throw Exception(ErrorCodes::INVALID_RBAC_VERSION, "{} {} is too new", RBACVersion::SETTING_NAME, rbac_version);
}

}
