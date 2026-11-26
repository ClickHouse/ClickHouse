#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

namespace DB
{

class AlterCommands;

/// throws if the alter drops or renames a column a row policy of the table mentions
void checkRowPoliciesBeforeAlter(const StorageID & table_id, const AlterCommands & commands, const ContextPtr & context);

}
