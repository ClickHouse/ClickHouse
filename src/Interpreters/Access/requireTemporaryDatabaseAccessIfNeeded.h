#pragma once

#include <Interpreters/Context_fwd.h>
#include <Access/Common/AccessRightsElement.h>

namespace DB
{
bool requireTemporaryDatabaseAccessIfNeeded(AccessRightsElements & required_access, const String & db_name, const ContextPtr & context);
}
