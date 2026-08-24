#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/SessionQueryIdsHistory.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/System/StorageSystemSessionQueryIds.h>
#include <Storages/System/SystemTableSourceRegistry.h>


namespace DB
{

ColumnsDescription StorageSystemSessionQueryIds::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"sequence_number", std::make_shared<DataTypeUInt64>(), "Position of the query within the session, monotonically increasing."},
        {"query_id", std::make_shared<DataTypeString>(), "The query id, can be joined with `system.query_log`."},
    };
}

void StorageSystemSessionQueryIds::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    if (!context->hasSessionContext())
        return;

    for (const auto & entry : context->getSessionQueryIdsHistory().getEntries())
    {
        res_columns[0]->insert(entry.sequence_number);
        res_columns[1]->insert(entry.query_id);
    }
}

void StorageSystemSessionQueryIds::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr context, TableExclusiveLockHolder &)
{
    /// Throws THERE_IS_NO_SESSION when there is no session whose history could be cleared.
    context->getSessionContext()->getSessionQueryIdsHistory().clear();
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemSessionQueryIds) }
