#include <Databases/UDT/AtomicCrossDatabaseGuard.h>

#include <Databases/DatabaseSchemaMutationTransaction.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace UDT
{

bool hasDatabaseOwnedTableExpectationForCrossDatabaseMove(const AtomicCrossDatabaseAuthorityView & view, UUID table_uuid)
{
    if (view.database_uuid == UUIDHelpers::Nil)
        throw DatabaseSchemaMutationReplayConflictError("cross-database move observed an invalid database identity");
    if (!view.durable_storage_present && (view.durable_state || view.recovery_required_transaction_id || view.durable_authority_marker))
    {
        throw DatabaseSchemaMutationReplayConflictError(
            "cross-database move observed durable user-defined type state without durable storage");
    }
    if (view.published_root && view.published_root->getDatabaseUUID() != view.database_uuid)
    {
        throw DatabaseSchemaMutationReplayConflictError("cross-database move observed a user-defined type authority for another database");
    }
    if (view.published_root && !view.durable_storage_present)
    {
        throw DatabaseSchemaMutationReplayConflictError(
            "cross-database move observed a published user-defined type authority without durable storage");
    }
    if (view.published_root && !view.durable_authority_marker)
    {
        throw DatabaseSchemaMutationReplayConflictError(
            "cross-database move observed a published user-defined type authority without its durable activation marker");
    }
    if (view.durable_storage_present)
    {
        if (view.recovery_required_transaction_id)
        {
            throw DatabaseSchemaMutationReplayConflictError("cross-database move is blocked by pending user-defined type schema recovery");
        }
        if (view.published_root && (!view.durable_state || *view.durable_state != view.published_root->getAuthorityState()))
        {
            throw DatabaseSchemaMutationReplayConflictError(
                "cross-database move observed different published and durable user-defined type authority states");
        }
        if (!view.published_root && static_cast<bool>(view.durable_state) != view.durable_authority_marker)
        {
            throw DatabaseSchemaMutationReplayConflictError(
                "cross-database move observed inconsistent unpublished durable user-defined type authority state");
        }
    }

    if (view.published_root)
    {
        if (table_uuid == UUIDHelpers::Nil)
            return view.published_root->getExpectationRecordCount() != 0;
        for (const auto kind : {SchemaObjectKind::Table, SchemaObjectKind::View, SchemaObjectKind::Dictionary})
        {
            const SchemaObjectID object{
                .kind = kind,
                .database_uuid = view.database_uuid,
                .object_uuid = table_uuid,
            };
            if (view.published_root->findExpectationRecord(object))
                return true;
        }
        return false;
    }

    /// A durable marker without a published root is not yet activated in this
    /// process. Do not let a move erase the evidence needed to recover it.
    return view.durable_storage_present;
}

void assertTableCanLeaveAtomicDatabase(bool has_database_owned_table, std::string_view table_name_for_logs)
{
    if (!has_database_owned_table)
        return;
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Cannot move table {} with database-owned user-defined column types; "
        "cross-database UDT authority transfer is not implemented",
        table_name_for_logs);
}

void assertTableCanEnterAtomicDatabase(
    bool has_database_owned_expectation, std::string_view table_name_for_logs, std::string_view target_database_name)
{
    if (!has_database_owned_expectation)
        return;
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Cannot move table {} into database {} because that database already owns a user-defined type expectation for its UUID",
        table_name_for_logs,
        backQuote(String(target_database_name)));
}

}
}
