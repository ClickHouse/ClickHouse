#include <Interpreters/Access/RowPolicyAlterColumns.h>

#include <Access/AccessControl.h>
#include <Access/RowPolicy.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Planner/Utils.h>
#include <Storages/AlterCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageProxy.h>
#include <Storages/expressionSourceColumns.h>

#include <algorithm>
#include <string_view>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
}

namespace
{

/// if exists on a missing column is a no-op (ignore), clear column keeps the column
bool removesColumn(const AlterCommand & command)
{
    if (command.ignore)
        return false;
    return (command.type == AlterCommand::DROP_COLUMN && !command.clear) || command.type == AlterCommand::RENAME_COLUMN;
}

/// a new type can lack a subcolumn the policy reads or break a function the policy applies to the column
bool modifiesColumn(const AlterCommand & command)
{
    return !command.ignore && command.type == AlterCommand::MODIFY_COLUMN;
}

/// a used name is a column or a subcolumn of one; a command on n changes the column n, or the whole Nested n if there is no column n
bool usesColumn(const Names & used_columns, const ColumnsDescription & columns, const String & column)
{
    const bool whole_nested = !columns.has(column);
    return std::ranges::any_of(used_columns, [&](const String & used)
    {
        const auto found = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, used);
        const auto in_storage = found ? found->getNameInStorage() : used;
        return in_storage == column || (whole_nested && in_storage.starts_with(column + "."));
    });
}

using TableNames = std::unordered_set<StorageID, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;

/// a subquery over any of the tables reads the same columns
Names columnsReadByFilter(
    const String & filter,
    const String & policy_name,
    const StorageSnapshotPtr & storage_snapshot,
    const StorageID & table_id,
    const TableNames & tables,
    const ContextPtr & context,
    std::string_view stage)
{
    try
    {
        ParserExpression parser;
        auto ast = wrapStandaloneSubqueryFilter(parseQuery(parser, filter, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS));
        return expressionSourceColumns(
            ast, storage_snapshot->metadata->getColumns(), context, /*keep_alias_columns=*/ true, storage_snapshot, table_id,
            [&](const StorageID & id) { return tables.contains(id); });
    }
    catch (Exception & e)
    {
        e.addMessage("while checking row policy {} {} ALTER TABLE", policy_name, stage);
        throw;
    }
}

bool isAlias(const StoragePtr & storage)
{
    return storage && resolveStorageProxyLoading(storage)->as<StorageAlias>();
}

/// a read through an `Alias` also applies the policies of its target (see `getRowPolicyFilterForStorage`), and all of them
/// read the columns of the table at the end of the chain, so these are the table, its chain, and every alias that reaches it
TableNames tablesWithPoliciesOnColumnsOf(const StoragePtr & table, const ContextPtr & context)
{
    std::vector<StorageID> pending{table->getStorageID()};
    const auto resolved = resolveStorageProxyLoading(table);
    if (const auto * alias = resolved->as<StorageAlias>())
        for (const auto & [database, name] : alias->getTargetChain())
            pending.emplace_back(database, name);

    TableNames tables;
    auto & catalog = DatabaseCatalog::instance();
    while (!pending.empty())
    {
        auto id = std::move(pending.back());
        pending.pop_back();
        if (!tables.insert(id).second)
            continue;
        /// an alias references nothing but its target
        for (const auto & dependent : catalog.getReferentialDependents(id))
            if (isAlias(catalog.tryGetTable(dependent, context)))
                pending.push_back(dependent);
    }
    return tables;
}

}

void checkRowPoliciesBeforeAlter(
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot,
    const AlterCommands & commands,
    bool share_nested_offsets,
    const ContextPtr & context)
{
    /// row policies are local to a server, so a replica must apply what its initiator already committed
    if (isSecondaryDDLReplay(context)
        || std::none_of(commands.begin(), commands.end(), [](const auto & command) { return removesColumn(command) || modifiesColumn(command); }))
        return;

    const auto storage_snapshot = table->getStorageSnapshotWithoutData(metadata_snapshot, context);
    const auto & old_columns = metadata_snapshot->getColumns();

    StorageSnapshotPtr new_storage_snapshot;
    if (std::any_of(commands.begin(), commands.end(), modifiesColumn))
    {
        auto new_metadata = std::make_shared<StorageInMemoryMetadata>(*metadata_snapshot);
        commands.apply(*new_metadata, context, share_nested_offsets);
        new_storage_snapshot = table->getStorageSnapshotWithoutData(new_metadata, context);
    }

    /// binding the lookup tables of a policy needs no grant of the user who alters the table: a copy of the global context has full access
    auto analysis_context = Context::createCopy(context->getGlobalContext());
    analysis_context->makeQueryContext();
    analysis_context->setSettings(context->getSettingsRef());
    if (const auto & database = context->getCurrentDatabase(); !database.empty())
        analysis_context->setCurrentDatabase(database);

    const auto tables = tablesWithPoliciesOnColumnsOf(table, context);

    const auto & access_control = context->getAccessControl();
    for (const auto & id : access_control.findAll<RowPolicy>())
    {
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (!policy)
            continue;

        const auto policy_name = policy->getFullName().toString();
        /// a read resolves the policy against the name of the table it reads, an `ON db.*` policy against each table of db
        for (const auto & table_id : tables)
        {
            if (table_id.database_name != policy->getDatabase()
                || (!policy->isForDatabase() && table_id.table_name != policy->getTableName()))
                continue;

            for (const auto & filter : policy->filters)
            {
                if (filter.empty())
                    continue;

                const auto used_columns = columnsReadByFilter(filter, policy_name, storage_snapshot, table_id, tables, analysis_context, "before");
                for (const auto & command : commands)
                {
                    if (removesColumn(command) && usesColumn(used_columns, old_columns, command.column_name))
                        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                            "Cannot {} column {}: it is used by row policy {}. Change or drop the policy first",
                            command.type == AlterCommand::DROP_COLUMN ? "drop" : "rename", backQuote(command.column_name), policy_name);
                }

                const bool modifies_used = std::any_of(commands.begin(), commands.end(), [&](const auto & command)
                {
                    return modifiesColumn(command) && usesColumn(used_columns, old_columns, command.column_name);
                });
                if (!modifies_used)
                    continue;

                /// a subquery reads the table before the alter, so only the names tell if its subcolumns are kept
                const auto & new_columns = new_storage_snapshot->metadata->getColumns();
                for (const auto & used : used_columns)
                {
                    const auto old_column = old_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, used);
                    if (old_column && !new_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, used))
                        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                            "Cannot modify column {}: row policy {} reads {}, which the new type does not have. Change or drop the policy first",
                            backQuote(old_column->getNameInStorage()), policy_name, backQuote(used));
                }
                columnsReadByFilter(filter, policy_name, new_storage_snapshot, table_id, tables, analysis_context, "against the columns after");
            }
        }
    }
}

}
