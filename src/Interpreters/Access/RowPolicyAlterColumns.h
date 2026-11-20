#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

namespace DB
{

class AlterCommands;
class ColumnsDescription;

/// throws if the alter drops or renames a column a row policy of the table reads
void checkRowPoliciesBeforeAlter(
    const StorageID & table_id, const ColumnsDescription & columns, const AlterCommands & commands, const ContextPtr & context);

}
