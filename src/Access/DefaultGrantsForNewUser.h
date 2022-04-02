#pragma once

#include <Access/Common/AccessRightsElement.h>

namespace DB
{
    /// Returns the default grants for a new user. It depends on `rbac_version`.
    /// If `rbac_version < 2` the function returns "GRANT SELECT ON system.*"
    /// If `rbac_version >= 2` the function returns a sensible set of grants on the system database.
    const AccessRightsElements & getDefaultGrantsForNewUser(UInt64 rbac_version);
}
