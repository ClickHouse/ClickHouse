#pragma once

#include <base/types.h>

namespace DB
{

namespace RBACVersion
{
    /// The initial version of RBAC.
    extern const UInt64 INITIAL; // = 1

    /// The latest version of RBAC.
    extern const UInt64 LATEST; // = 2

    /// rbac_version < 2: Users have GRANT SELECT ON system.* by default
    /// rbac_version >= 2: Users have a reasonable set of grants on the system database by default.
    /// This includes grants for constant tables like system.one, system.contributors, system.settings, and so on.
    /// (for the full list see the function getDefaultSystemDBGrants()).
    extern const UInt64 NO_FULL_SYSTEM_DB_GRANTS; // = 2

    /// The name of the setting rbac_version (see Settings.h)
    extern const char SETTING_NAME[]; // = "rbac_version"
}

void checkRBACVersion(UInt64 rbac_version);

}
