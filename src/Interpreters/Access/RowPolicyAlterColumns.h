#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

namespace DB
{

class AlterCommands;

/// Throws if the ALTER would break a row policy of the table (e.g. drops a column it uses).
void checkRowPoliciesBeforeAlter(const StorageID & table_id, const AlterCommands & commands, const ContextPtr & context);

/// Renames columns in the row policies of the table. Safe to run on every replica.
void renameColumnsInRowPolicies(const StorageID & table_id, const AlterCommands & commands, const ContextPtr & context);

}
