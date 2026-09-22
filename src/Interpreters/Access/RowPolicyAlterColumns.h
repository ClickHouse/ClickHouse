#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

namespace DB
{

class AlterCommands;

/// Row policies mention the columns of their table by name and live outside its metadata, so an
/// ALTER that drops or renames such a column would leave the policy unresolvable. Throws for the
/// commands that cannot be followed: dropping a mentioned column, and renaming one that is
/// mentioned by a database-wide (`db.*`) policy or by a policy in a read-only access storage.
void checkRowPoliciesBeforeAlter(const StorageID & table_id, const AlterCommands & commands, const ContextPtr & context);

/// Renames the columns inside the table's row policies after the ALTER has committed.
/// Idempotent: a rename whose source column is already gone changes nothing, so every replica may run it.
void renameColumnsInRowPolicies(const StorageID & table_id, const AlterCommands & commands, const ContextPtr & context);

}
