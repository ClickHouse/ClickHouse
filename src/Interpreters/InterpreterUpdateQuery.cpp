#include <Interpreters/InterpreterUpdateQuery.h>
#include <Interpreters/InterpreterFactory.h>

#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/replaceLegacyToTime.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/MutationPredicateColumnsAccess.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Interpreters/executeDDLQueryOnCluster.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_PERMANENTLY_READ_ONLY;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int QUERY_IS_PROHIBITED;
}

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool enable_lightweight_update;
    extern const SettingsBool use_legacy_to_time;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

namespace ServerSetting
{
    extern const ServerSettingsBool disable_insertion_and_mutation;
}

InterpreterUpdateQuery::InterpreterUpdateQuery(ASTPtr query_ptr_, ContextPtr context_)
    : WithContext(context_), query_ptr(std::move(query_ptr_))
{
}

static MutationCommand createMutationCommand(const ASTUpdateQuery & update_query, const Settings & settings)
{
    auto alter_query = make_intrusive<ASTAlterCommand>();

    alter_query->type = ASTAlterCommand::UPDATE;
    alter_query->set(alter_query->predicate, update_query.predicate);
    alter_query->set(alter_query->update_assignments, update_query.assignments);

    if (update_query.partition)
        alter_query->set(alter_query->partition, update_query.partition);

    if (update_query.partitions)
        alter_query->set(alter_query->partitions, update_query.partitions);

    auto mutation_command = MutationCommand::parse(
        *alter_query,
        /* parse_alter_commands = */ false,
        /* with_pure_metadata_commands = */ false,
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);
    if (!mutation_command)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Failed to convert query '{}' to mutation command. It's a bug", update_query.formatForErrorMessage());

    return *mutation_command;
}

BlockIO InterpreterUpdateQuery::execute()
{
    const auto & settings = getContext()->getSettingsRef();
    if (!settings[Setting::enable_lightweight_update])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Lightweight updates are not allowed. Set 'enable_lightweight_update = 1' to allow them");

    FunctionNameNormalizer::visit(query_ptr.get());

    /// Inline the bodies of SQL user-defined functions before the database is filled in, otherwise an
    /// unqualified table inside a body is resolved later, in a context whose current database is not
    /// the database of the updated table. This also has to precede the read columns extracted below:
    /// a UDF body can reference a column of the updated table, which is a read of it that the call
    /// site alone does not show, and nothing checks access later on this path - a local lightweight
    /// update goes straight to `updateLightweight`.
    if (!UserDefinedSQLFunctionFactory::instance().empty())
        UserDefinedSQLFunctionVisitor::visit(query_ptr, getContext());

    /// The spelling must be canonical before the query is enqueued for ON CLUSTER or a Replicated
    /// database: the oldest DDL entry format carries no settings, so the replaying host would
    /// otherwise resolve `toTime` with its own default.
    if (settings[Setting::use_legacy_to_time])
        replaceLegacyToTime(*query_ptr);

    auto & update_query = query_ptr->as<ASTUpdateQuery &>();

    /// The WHERE predicate and the assignment expressions read columns, so they require SELECT on
    /// those columns (virtual columns excluded, as in a plain SELECT). Collected below, from the
    /// table resolved for the `_row_exists` check, and before any dispatch, so that the initiating
    /// user's read access is enforced on every path - including ON CLUSTER, where the remote DDL
    /// worker does not run as the initiating user.
    AccessRightsElements read_access;

    /// Reads hidden behind a subquery or a `dictGet`/`joinGet` name their own objects, so they are
    /// required on every path - including the ones where the updated table is not present locally,
    /// and when `validate_mutation_query` is disabled. The updated table is passed along to tell one
    /// of its columns from a table on the right of `IN`, and as the database an unqualified table in
    /// the expression is read from (see `AddDefaultDatabaseVisitor` below).
    auto add_indirect_reads = [&](const String & database, const String & table, const StorageInMemoryMetadata * metadata)
    {
        addExpressionIndirectReadsAccess(
            read_access, update_query.predicate.get(), getContext(), database, table, metadata);
        for (const ASTPtr & assignment : update_query.assignments->children)
            addExpressionIndirectReadsAccess(
                read_access, assignment->as<const ASTAssignment &>().expression().get(), getContext(),
                database, table, metadata);
    };

    /// Setting the `_row_exists` lightweight-delete marker to 0 is a delete, not an update
    /// (`DELETE FROM` may rewrite to `UPDATE ... SET _row_exists = 0`), so govern that exact form by
    /// ALTER DELETE. Any other assignment - including `_row_exists = <expr>` that edits the deletion
    /// mask - stays a real update requiring ALTER UPDATE. The shortcut applies only when `_row_exists`
    /// is the hidden virtual marker; on an engine where it is an ordinary physical column it is a normal
    /// update. Resolve the table best-effort (null for a non-local ON CLUSTER target) and fail closed.
    StoragePtr table_for_access;
    auto resolved_table_id = getContext()->tryResolveStorageID(update_query, Context::ResolveOrdinary);
    if (resolved_table_id)
    {
        /// The database has to be pinned before the access rights and the distributed dispatch are built
        /// from it: otherwise they are expanded to the configured default database of each host, so the
        /// rights that are checked and the table that is updated can name different databases.
        update_query.setDatabase(resolved_table_id.database_name);
        table_for_access = DatabaseCatalog::instance().tryGetTable(resolved_table_id, getContext());
    }
    const bool row_exists_is_marker = InterpreterAlterQuery::isRowExistsLightweightDeleteMarker(table_for_access, getContext());

    if (resolved_table_id)
    {
        if (table_for_access)
        {
            const auto metadata_snapshot = table_for_access->getInMemoryMetadataPtr(getContext(), false);
            const auto & metadata = *metadata_snapshot;
            addExpressionColumnsSelectAccess(
                read_access, update_query.predicate.get(),
                resolved_table_id.database_name, resolved_table_id.table_name, metadata);
            for (const ASTPtr & assignment : update_query.assignments->children)
                addExpressionColumnsSelectAccess(
                    read_access, assignment->as<const ASTAssignment &>().expression().get(),
                    resolved_table_id.database_name, resolved_table_id.table_name, metadata);

            add_indirect_reads(resolved_table_id.database_name, resolved_table_id.table_name, &*metadata_snapshot);
        }
        else
        {
            /// ON CLUSTER from a node without the table: columns cannot be resolved, so fail closed
            /// by requiring SELECT on the whole table.
            if (!update_query.cluster.empty())
                read_access.emplace_back(AccessType::SELECT, resolved_table_id.database_name, resolved_table_id.table_name);

            add_indirect_reads(resolved_table_id.database_name, resolved_table_id.table_name, nullptr);
        }
    }
    else
    {
        /// ON CLUSTER with no current database: the id stays unresolved here, but
        /// executeDDLQueryOnCluster expands empty-database access elements to each host's default
        /// database. Fail closed with the AST table name and an empty database so the predicate/RHS
        /// SELECT requirement is expanded together with ALTER_UPDATE instead of being dropped.
        if (!update_query.cluster.empty())
            read_access.emplace_back(AccessType::SELECT, update_query.getDatabase(), update_query.getTable());

        add_indirect_reads(update_query.getDatabase(), update_query.getTable(), nullptr);
    }

    bool deletes_via_row_exists = false;
    bool updates_columns = false;
    for (const ASTPtr & assignment_ast : update_query.assignments->children)
    {
        if (row_exists_is_marker && isLightweightDeleteAssignment(assignment_ast->as<const ASTAssignment &>()))
            deletes_via_row_exists = true;
        else
            updates_columns = true;
    }

    /// Built after `setDatabase` so the ALTER_UPDATE requirement uses the same (resolved) database as
    /// the dispatched query and the read requirements above.
    AccessRightsElements required_access;
    if (deletes_via_row_exists)
        required_access.emplace_back(AccessType::ALTER_DELETE, update_query.getDatabase(), update_query.getTable());
    if (updates_columns)
        required_access.emplace_back(AccessType::ALTER_UPDATE, update_query.getDatabase(), update_query.getTable());

    if (!update_query.cluster.empty())
    {
        /// Substitute the database into table functions that use the current database implicitly, e.g.
        /// `merge('tables_regexp')`, before `executeDDLQueryOnCluster` replaces `currentDatabase()` with the
        /// database of the session. The table identifiers are qualified on each host instead.
        if (resolved_table_id)
        {
            AddDefaultDatabaseVisitor visitor(getContext(), resolved_table_id.getDatabaseName());
            if (update_query.predicate)
                visitor.substituteDatabaseInTableFunctions(*update_query.predicate);
            if (update_query.assignments)
                visitor.substituteDatabaseInTableFunctions(*update_query.assignments);
        }

        DDLQueryOnClusterParams params;
        /// Enforce the read (SELECT) requirements on the initiator too, since the remote DDL worker
        /// may not run as the initiating user.
        required_access.append_range(read_access);
        params.access_to_check = std::move(required_access);
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    if (getContext()->getGlobalContext()->getServerSettings()[ServerSetting::disable_insertion_and_mutation])
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Update queries are prohibited");

    getContext()->checkAccess(required_access);
    /// Same database as `resolved_table_id` above, resolved again because the `ON CLUSTER` branch returns
    /// before this point, and because this one must throw where that one returns empty. Do not collapse.
    auto table_id = getContext()->resolveStorageID(update_query, Context::ResolveOrdinary);
    update_query.setDatabase(table_id.database_name);

    /// First check table storage for validations.
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (table->isStaticStorage())
        throw Exception(ErrorCodes::TABLE_IS_PERMANENTLY_READ_ONLY, "Table is read-only");

    if (!read_access.empty())
        getContext()->checkAccess(read_access);

    if (auto supports = table->supportsLightweightUpdate(); !supports)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Lightweight updates are not supported. {}", supports.error().text);

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (database->shouldReplicateQuery(getContext(), query_ptr))
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name, database.get());
        guard->releaseTableLock();
        return database->tryEnqueueReplicatedDDL(query_ptr, getContext(), {}, std::move(guard));
    }

    /// Expand CTEs before filling the default database, otherwise a CTE alias is qualified as if it
    /// were a table. The context makes CTE expansion respect `enable_global_with_statement`: a CTE
    /// name a subquery does not see is a table name there, and has to be qualified.
    if (update_query.predicate)
    {
        ASTPtr predicate = update_query.predicate->ptr();
        ApplyWithSubqueryVisitor::visit(predicate, getContext());
    }
    if (update_query.assignments)
    {
        ASTPtr assignments = update_query.assignments->ptr();
        ApplyWithSubqueryVisitor::visit(assignments, getContext());
    }

    /// Add default database to table identifiers that we can encounter in the update expression,
    /// and to the dictionary of a `dictGet` and the table of a `joinGet` in it: the expression is
    /// executed later in a background context, and its access check
    /// (`MutationPredicateColumnsAccess`) requires them under the database of the updated table.
    /// A separate visitor per expression: it remembers the names of the recursive common table
    /// expressions it walked, and the two expressions have separate scopes.
    if (update_query.predicate)
    {
        AddDefaultDatabaseVisitor visitor(
            getContext(), table_id.getDatabaseName(),
            /*only_replace_current_database_function_=*/ false, /*only_replace_in_join_=*/ false,
            /*qualify_function_table_names_with_database_name_=*/ true);
        ASTPtr predicate = update_query.predicate->ptr();
        visitor.visit(predicate);
    }
    if (update_query.assignments)
    {
        AddDefaultDatabaseVisitor visitor(
            getContext(), table_id.getDatabaseName(),
            /*only_replace_current_database_function_=*/ false, /*only_replace_in_join_=*/ false,
            /*qualify_function_table_names_with_database_name_=*/ true);
        ASTPtr assignments = update_query.assignments->ptr();
        visitor.visit(assignments);
    }

    MutationCommands commands;
    commands.emplace_back(createMutationCommand(update_query, settings));

    auto table_lock = table->lockForShare(getContext()->getCurrentQueryId(), settings[Setting::lock_acquire_timeout]);

    BlockIO res;
    res.pipeline = table->updateLightweight(commands, getContext());
    res.pipeline.addStorageHolder(table);

    /// The patch part is committed while the pipeline runs, so the share lock must outlive this
    /// function: otherwise a concurrent DROP can clear the data parts index under the sink.
    QueryPlanResourceHolder update_resources;
    update_resources.table_locks.emplace_back(std::move(table_lock));
    res.pipeline.addResources(std::move(update_resources));

    return res;
}

void registerInterpreterUpdateQuery(InterpreterFactory & factory);
void registerInterpreterUpdateQuery(InterpreterFactory & factory)
{
    auto create_fn = [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterUpdateQuery>(args.query, args.context);
    };

    factory.registerInterpreter("InterpreterUpdateQuery", create_fn);
}

}
