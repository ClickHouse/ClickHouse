#pragma once

#include <Access/AccessRights.h>

namespace DB
{

/// Adds implicit grants for a user. Those grants are added to any user but they're hidden,
/// SHOW GRANTS doesn't show implicit grants. Those grants cannot be revoked.
AccessRights addImplicitGrants(const AccessRights & access);

/// Returns those implicit grants which are constant.
const AccessRightsElements & getConstImplicitGrants();

}
