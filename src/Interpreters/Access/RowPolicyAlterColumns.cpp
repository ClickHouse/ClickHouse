#include <Interpreters/Access/RowPolicyAlterColumns.h>

#include <Access/AccessControl.h>
#include <Access/RowPolicy.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/TableNode.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Planner/Utils.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Storages/AlterCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageProxy.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/expressionSourceColumns.h>

#include <algorithm>
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

ASTPtr parseFilter(const String & filter)
{
    ParserExpression parser;
    return wrapStandaloneSubqueryFilter(parseQuery(parser, filter, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS));
}

/// a subquery over any of the tables reads the same columns
Names columnsReadByFilter(
    const String & filter,
    const String & policy_name,
    const StorageSnapshotPtr & storage_snapshot,
    const StorageID & table_id,
    const TableNames & tables,
    const ContextPtr & context,
    Names * subquery_reads)
{
    try
    {
        return expressionSourceColumns(
            parseFilter(filter), storage_snapshot->metadata->getColumns(), context, /*keep_alias_columns=*/ true, storage_snapshot, table_id,
            [&](const StorageID & id) { return tables.contains(id); }, subquery_reads);
    }
    catch (Exception & e)
    {
        e.addMessage("while checking row policy {} before ALTER TABLE", policy_name);
        throw;
    }
}

/// analysis gives a scalar subquery the default value of its type, which a function can refuse, e.g. `intDiv(n, (SELECT k FROM l))`;
/// a subquery of IN is not read
bool readsScalarSubquery(const QueryTreeNodePtr & node)
{
    auto node_type = node->getNodeType();
    if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION)
        return false;

    if (const auto * constant = node->as<ConstantNode>(); constant && constant->hasSourceExpression())
    {
        auto source_type = constant->getSourceExpression()->getNodeType();
        return source_type == QueryTreeNodeType::QUERY || source_type == QueryTreeNodeType::UNION
            || readsScalarSubquery(constant->getSourceExpression());
    }

    return std::ranges::any_of(node->getChildren(), [](const auto & child) { return child && readsScalarSubquery(child); });
}

/// a read also evaluates the filter actions on the empty header of the table, which fails for some types that analysis accepts,
/// e.g. `n = 1` for a String n, or a String filter; returns false for a filter that reads a scalar subquery, which is only analyzed
bool checkFilterAfterAlter(
    const String & filter,
    const String & policy_name,
    const StorageSnapshotPtr & storage_snapshot,
    const StorageID & table_id,
    const ContextPtr & context)
{
    try
    {
        auto query_context = Context::createCopy(context);
        auto expression = buildQueryTree(parseFilter(filter), query_context);
        auto storage = std::make_shared<StorageDummy>(table_id, storage_snapshot->metadata->getColumns(), storage_snapshot);
        auto table_node = std::make_shared<TableNode>(storage, query_context);
        QueryAnalyzer(/*only_analyze=*/ true).resolve(expression, table_node, query_context);
        if (readsScalarSubquery(expression))
            return false;

        auto planner_context = std::make_shared<PlannerContext>(
            query_context, std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{}), SelectQueryOptions{});
        /// the filter can read the table only in a subquery
        planner_context->getOrCreateTableExpressionData(table_node);
        auto filter_info = buildFilterInfo(expression, table_node, planner_context);

        Block header;
        for (const auto * input : filter_info.actions.getInputs())
            header.insert({input->result_type->createColumn(), input->result_type, input->result_name});
        FilterTransform::transformHeader(header, &filter_info.actions, filter_info.column_name, filter_info.do_remove_column);
        return true;
    }
    catch (Exception & e)
    {
        e.addMessage("while checking row policy {} against the columns after ALTER TABLE", policy_name);
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

    /// built only when a policy reads a modified column
    StorageSnapshotPtr new_storage_snapshot;

    /// the analysis reads no data, so the tables of a policy need no grant of the user who alters the table;
    /// a table function or a function such as `file` still needs the grants of the user, as in a read
    auto analysis_context = Context::createCopy(context);
    analysis_context->makeQueryContext();
    analysis_context->setAnalyzeTablesWithoutGrants(true);

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

                Names subquery_reads;
                const auto used_columns = columnsReadByFilter(
                    filter, policy_name, storage_snapshot, table_id, tables, analysis_context, &subquery_reads);
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

                if (!new_storage_snapshot)
                {
                    auto new_metadata = std::make_shared<StorageInMemoryMetadata>(*metadata_snapshot);
                    /// as the storages do before `apply`: an explicit `ADD STATISTICS` replaces an implicit one
                    removeImplicitStatistics(new_metadata->columns);
                    commands.apply(*new_metadata, context, share_nested_offsets);
                    new_storage_snapshot = table->getStorageSnapshotWithoutData(new_metadata, context);
                }

                /// a subquery reads the table before the alter, so only the names and types tell if it still reads the same
                const auto & new_columns = new_storage_snapshot->metadata->getColumns();
                for (const auto & used : used_columns)
                {
                    const auto old_column = old_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, used);
                    if (old_column && !new_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, used))
                        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                            "Cannot modify column {}: row policy {} reads {}, which the new type does not have. Change or drop the policy first",
                            backQuote(old_column->getNameInStorage()), policy_name, backQuote(used));
                }
                auto refuse_new_types = [&](const Names & reads, std::string_view how)
                {
                    for (const auto & used : reads)
                    {
                        const auto old_column = old_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, used);
                        const auto new_column = new_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, used);
                        if (old_column && new_column && !new_column->type->equals(*old_column->type))
                            throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                                "Cannot modify column {}: row policy {} reads {} {}, so the check cannot evaluate the policy with the new type. "
                                "Change or drop the policy first",
                                backQuote(old_column->getNameInStorage()), policy_name, backQuote(used), how);
                    }
                };
                refuse_new_types(subquery_reads, "in a subquery");
                /// with the same types the filter evaluates as before the alter
                if (!checkFilterAfterAlter(filter, policy_name, new_storage_snapshot, table_id, analysis_context))
                    refuse_new_types(used_columns, "and a scalar subquery");
            }
        }
    }
}

}
