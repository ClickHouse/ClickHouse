#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class AlterCommands;

/// throws if the alter drops or renames a column a row policy of the table, or of an `Alias` related to it, mentions
void checkRowPoliciesBeforeAlter(const StoragePtr & table, const AlterCommands & commands, const ContextPtr & context);

}
