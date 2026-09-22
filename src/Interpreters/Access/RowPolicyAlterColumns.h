#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

namespace DB
{

class AlterCommands;

/// Refuses an ALTER that would break a row policy: dropping a column it uses, or renaming one
/// used by a `db.*` policy or a policy in a read-only storage.
void checkRowPoliciesBeforeAlter(const StorageID & table_id, const AlterCommands & commands, const ContextPtr & context);

/// Applies RENAME COLUMN to the table's row policies. Idempotent, so every replica may run it.
void renameColumnsInRowPolicies(const StorageID & table_id, const AlterCommands & commands, const ContextPtr & context);

}
