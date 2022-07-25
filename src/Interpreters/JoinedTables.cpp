#include <Interpreters/JoinedTables.h>

#include <Core/SettingsEnums.h>

#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InJoinSubqueriesPreprocessor.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/getTableExpressions.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageJoin.h>
#include <Storages/StorageValues.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALIAS_REQUIRED;
    extern const int AMBIGUOUS_COLUMN_NAME;
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename T, typename ... Args>
std::shared_ptr<T> addASTChildrenTo(IAST & node, ASTPtr & children, Args && ... args)
{
    auto new_children = std::make_shared<T>(std::forward<Args>(args)...);
    children = new_children;
    node.children.push_back(children);
    return new_children;
}

template <typename T>
std::shared_ptr<T> addASTChildren(IAST & node)
{
    auto children = std::make_shared<T>();
    node.children.push_back(children);
    return children;
}

void replaceJoinedTable(const ASTSelectQuery & select_query)
{
    const ASTTablesInSelectQueryElement * join = select_query.join();
    if (!join || !join->table_expression)
        return;

    const auto & table_join = join->table_join->as<ASTTableJoin &>();

    /// TODO: Push down for CROSS JOIN is not OK [disabled]
    if (table_join.kind == ASTTableJoin::Kind::Cross)
        return;

    /* Do not push down predicates for ASOF because it can lead to incorrect results
     * (for example, if we will filter a suitable row before joining and will choose another, not the closest row).
     * ANY join behavior can also be different with this optimization,
     * but it's ok because we don't guarantee which row to choose for ANY, unlike ASOF, where we have to pick the closest one.
     */
    if (table_join.strictness == ASTTableJoin::Strictness::Asof)
        return;

    auto & table_expr = join->table_expression->as<ASTTableExpression &>();
    if (table_expr.database_and_table_name)
    {
        const auto & table_id = table_expr.database_and_table_name->as<ASTTableIdentifier &>();
        String table_name = table_id.name();
        String table_short_name = table_id.shortName();
        // FIXME: since the expression "a as b" exposes both "a" and "b" names, which is not equivalent to "(select * from a) as b",
        //        we can't replace aliased tables.
        // FIXME: long table names include database name, which we can't save within alias.
        if (table_id.alias.empty() && table_id.isShort())
        {
            /// Build query of form '(SELECT * FROM table_name) AS table_short_name'
            table_expr = ASTTableExpression();

            auto subquery = addASTChildrenTo<ASTSubquery>(table_expr, table_expr.subquery);
            subquery->setAlias(table_short_name);

            auto sub_select_with_union = addASTChildren<ASTSelectWithUnionQuery>(*subquery);
            auto list_of_selects = addASTChildrenTo<ASTExpressionList>(*sub_select_with_union, sub_select_with_union->list_of_selects);

            auto new_select = addASTChildren<ASTSelectQuery>(*list_of_selects);
            new_select->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
            addASTChildren<ASTAsterisk>(*new_select->select());
            new_select->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());

            auto tables_elem = addASTChildren<ASTTablesInSelectQueryElement>(*new_select->tables());
            auto sub_table_expr = addASTChildrenTo<ASTTableExpression>(*tables_elem, tables_elem->table_expression);
            addASTChildrenTo<ASTTableIdentifier>(*sub_table_expr, sub_table_expr->database_and_table_name, table_name);
        }
    }
}

class RenameQualifiedIdentifiersMatcher
{
public:
    using Data = const std::vector<DatabaseAndTableWithAlias>;

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * t = ast->as<ASTIdentifier>())
            visit(*t, ast, data);
        if (auto * node = ast->as<ASTQualifiedAsterisk>())
            visit(*node, ast, data);
    }

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child)
    {
        if (node->as<ASTTableExpression>() ||
            node->as<ASTQualifiedAsterisk>() ||
            child->as<ASTSubquery>())
            return false; // NOLINT
        return true;
    }

private:
    static void visit(ASTIdentifier & identifier, ASTPtr &, Data & data)
    {
        if (identifier.isShort())
            return;

        bool rewritten = false;
        for (const auto & table : data)
        {
            /// Table has an alias. We do not need to rewrite qualified names with table alias (match == ColumnMatch::TableName).
            auto match = IdentifierSemantic::canReferColumnToTable(identifier, table);
            if (match == IdentifierSemantic::ColumnMatch::AliasedTableName ||
                match == IdentifierSemantic::ColumnMatch::DBAndTable)
            {
                if (rewritten)
                    throw Exception("Failed to rewrite distributed table names. Ambiguous column '" + identifier.name() + "'",
                                    ErrorCodes::AMBIGUOUS_COLUMN_NAME);
                /// Table has an alias. So we set a new name qualified by table alias.
                IdentifierSemantic::setColumnLongName(identifier, table);
                rewritten = true;
            }
        }
    }

    static void visit(const ASTQualifiedAsterisk & node, const ASTPtr &, Data & data)
    {
        auto & identifier = node.children[0]->as<ASTTableIdentifier &>();
        bool rewritten = false;
        for (const auto & table : data)
        {
            if (identifier.name() == table.table)
            {
                if (rewritten)
                    throw Exception("Failed to rewrite distributed table. Ambiguous column '" + identifier.name() + "'",
                                    ErrorCodes::AMBIGUOUS_COLUMN_NAME);
                identifier.setShortName(table.alias);
                rewritten = true;
            }
        }
    }
};
using RenameQualifiedIdentifiersVisitor = InDepthNodeVisitor<RenameQualifiedIdentifiersMatcher, true>;

}

JoinedTables::JoinedTables(ContextPtr context_, const ASTSelectQuery & select_query, bool include_all_columns_)
    : context(context_)
    , table_expressions(getTableExpressions(select_query))
    , include_all_columns(include_all_columns_)
    , left_table_expression(extractTableExpression(select_query, 0))
    , left_db_and_table(getDatabaseAndTable(select_query, 0))
{}

bool JoinedTables::isLeftTableSubquery() const
{
    return left_table_expression && left_table_expression->as<ASTSelectWithUnionQuery>();
}

bool JoinedTables::isLeftTableFunction() const
{
    return left_table_expression && left_table_expression->as<ASTFunction>();
}

std::unique_ptr<InterpreterSelectWithUnionQuery> JoinedTables::makeLeftTableSubquery(const SelectQueryOptions & select_options)
{
    if (!isLeftTableSubquery())
        return {};

    /// Only build dry_run interpreter during analysis. We will reconstruct the subquery interpreter during plan building.
    return std::make_unique<InterpreterSelectWithUnionQuery>(left_table_expression, context, select_options.copy().analyze());
}

StoragePtr JoinedTables::getLeftTableStorage()
{
    if (isLeftTableSubquery())
        return {};

    if (isLeftTableFunction())
        return context->getQueryContext()->executeTableFunction(left_table_expression);

    StorageID table_id = StorageID::createEmpty();
    if (left_db_and_table)
    {
        table_id = context->resolveStorageID(StorageID(left_db_and_table->database, left_db_and_table->table, left_db_and_table->uuid));
    }
    else /// If the table is not specified - use the table `system.one`.
    {
        table_id = StorageID("system", "one");
    }

    if (auto view_source = context->getViewSource())
    {
        const auto & storage_values = static_cast<const StorageValues &>(*view_source);
        auto tmp_table_id = storage_values.getStorageID();
        if (tmp_table_id.database_name == table_id.database_name && tmp_table_id.table_name == table_id.table_name)
        {
            /// Read from view source.
            return context->getViewSource();
        }
    }

    /// Read from table. Even without table expression (implicit SELECT ... FROM system.one).
    return DatabaseCatalog::instance().getTable(table_id, context);
}

bool JoinedTables::resolveTables()
{
    const auto & settings = context->getSettingsRef();
    bool include_alias_cols = include_all_columns || settings.asterisk_include_alias_columns;
    bool include_materialized_cols = include_all_columns || settings.asterisk_include_materialized_columns;
    tables_with_columns = getDatabaseAndTablesWithColumns(table_expressions, context, include_alias_cols, include_materialized_cols);
    if (tables_with_columns.size() != table_expressions.size())
        throw Exception("Unexpected tables count", ErrorCodes::LOGICAL_ERROR);

    if (settings.joined_subquery_requires_alias && tables_with_columns.size() > 1)
    {
        for (size_t i = 0; i < tables_with_columns.size(); ++i)
        {
            const auto & t = tables_with_columns[i];
            if (t.table.table.empty() && t.table.alias.empty())
            {
                throw Exception("No alias for subquery or table function in JOIN (set joined_subquery_requires_alias=0 to disable restriction). While processing '"
                    + table_expressions[i]->formatForErrorMessage() + "'",
                    ErrorCodes::ALIAS_REQUIRED);
            }
        }
    }

    return !tables_with_columns.empty();
}

void JoinedTables::makeFakeTable(StoragePtr storage, const StorageMetadataPtr & metadata_snapshot, const Block & source_header)
{
    if (storage)
    {
        const ColumnsDescription & storage_columns = metadata_snapshot->getColumns();
        tables_with_columns.emplace_back(DatabaseAndTableWithAlias{}, storage_columns.getOrdinary());

        auto & table = tables_with_columns.back();
        table.addHiddenColumns(storage_columns.getMaterialized());
        table.addHiddenColumns(storage_columns.getAliases());
        table.addHiddenColumns(storage->getVirtuals());
    }
    else
        tables_with_columns.emplace_back(DatabaseAndTableWithAlias{}, source_header.getNamesAndTypesList());
}

void JoinedTables::rewriteDistributedInAndJoins(ASTPtr & query)
{
    /// Rewrite IN and/or JOIN for distributed tables according to distributed_product_mode setting.
    InJoinSubqueriesPreprocessor::SubqueryTables renamed_tables;
    InJoinSubqueriesPreprocessor(context, renamed_tables).visit(query);

    String database;
    if (!renamed_tables.empty())
        database = context->getCurrentDatabase();

    for (auto & [subquery, ast_tables] : renamed_tables)
    {
        std::vector<DatabaseAndTableWithAlias> renamed;
        renamed.reserve(ast_tables.size());
        for (auto & ast : ast_tables)
            renamed.emplace_back(DatabaseAndTableWithAlias(ast->as<ASTTableIdentifier &>(), database));

        /// Change qualified column names in distributed subqueries using table aliases.
        RenameQualifiedIdentifiersVisitor::Data data(renamed);
        RenameQualifiedIdentifiersVisitor(data).visit(subquery);
    }
}

std::shared_ptr<TableJoin> JoinedTables::makeTableJoin(const ASTSelectQuery & select_query)
{
    if (tables_with_columns.size() < 2)
        return {};

    auto settings = context->getSettingsRef();
    MultiEnum<JoinAlgorithm> join_algorithm = settings.join_algorithm;
    auto table_join = std::make_shared<TableJoin>(settings, context->getTemporaryVolume());

    const ASTTablesInSelectQueryElement * ast_join = select_query.join();
    const auto & table_to_join = ast_join->table_expression->as<ASTTableExpression &>();

    /// TODO This syntax does not support specifying a database name.
    if (table_to_join.database_and_table_name)
    {
        auto joined_table_id = context->resolveStorageID(table_to_join.database_and_table_name);
        StoragePtr storage = DatabaseCatalog::instance().tryGetTable(joined_table_id, context);
        if (storage)
        {
            if (auto storage_join = std::dynamic_pointer_cast<StorageJoin>(storage); storage_join)
            {
                table_join->setStorageJoin(storage_join);
            }
            else if (auto storage_dict = std::dynamic_pointer_cast<StorageDictionary>(storage); storage_dict)
            {
                table_join->setStorageJoin(storage_dict);
            }
            else if (auto storage_kv = std::dynamic_pointer_cast<IKeyValueStorage>(storage);
                     storage_kv && join_algorithm.isSet(JoinAlgorithm::DIRECT))
            {
                table_join->setStorageJoin(storage_kv);
            }
        }
    }

    if (!table_join->isSpecialStorage() &&
        settings.enable_optimize_predicate_expression)
        replaceJoinedTable(select_query);

    return table_join;
}

void JoinedTables::reset(const ASTSelectQuery & select_query)
{
    table_expressions = getTableExpressions(select_query);
    left_table_expression = extractTableExpression(select_query, 0);
    left_db_and_table = getDatabaseAndTable(select_query, 0);
}

}
