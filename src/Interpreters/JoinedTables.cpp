#include <Interpreters/JoinedTables.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/InJoinSubqueriesPreprocessor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InDepthNodeVisitor.h>

#include <Storages/IStorage.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageValues.h>
#include <Storages/StorageJoin.h>
#include <Storages/StorageDictionary.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALIAS_REQUIRED;
    extern const int AMBIGUOUS_COLUMN_NAME;
}

namespace
{

void replaceJoinedTable(const ASTSelectQuery & select_query)
{
    const ASTTablesInSelectQueryElement * join = select_query.join();
    if (!join || !join->table_expression)
        return;

    /// TODO: Push down for CROSS JOIN is not OK [disabled]
    const auto & table_join = join->table_join->as<ASTTableJoin &>();
    if (table_join.kind == ASTTableJoin::Kind::Cross)
        return;

    auto & table_expr = join->table_expression->as<ASTTableExpression &>();
    if (table_expr.database_and_table_name)
    {
        const auto & table_id = table_expr.database_and_table_name->as<ASTIdentifier &>();
        String expr = "(select * from " + table_id.name + ") as " + table_id.shortName();

        // FIXME: since the expression "a as b" exposes both "a" and "b" names, which is not equivalent to "(select * from a) as b",
        //        we can't replace aliased tables.
        // FIXME: long table names include database name, which we can't save within alias.
        if (table_id.alias.empty() && table_id.isShort())
        {
            ParserTableExpression parser;
            table_expr = parseQuery(parser, expr, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH)->as<ASTTableExpression &>();
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
                match == IdentifierSemantic::ColumnMatch::DbAndTable)
            {
                if (rewritten)
                    throw Exception("Failed to rewrite distributed table names. Ambiguous column '" + identifier.name + "'",
                                    ErrorCodes::AMBIGUOUS_COLUMN_NAME);
                /// Table has an alias. So we set a new name qualified by table alias.
                IdentifierSemantic::setColumnLongName(identifier, table);
                rewritten = true;
            }
        }
    }

    static void visit(const ASTQualifiedAsterisk & node, const ASTPtr &, Data & data)
    {
        ASTIdentifier & identifier = *node.children[0]->as<ASTIdentifier>();
        bool rewritten = false;
        for (const auto & table : data)
        {
            if (identifier.name == table.table)
            {
                if (rewritten)
                    throw Exception("Failed to rewrite distributed table. Ambiguous column '" + identifier.name + "'",
                                    ErrorCodes::AMBIGUOUS_COLUMN_NAME);
                identifier.setShortName(table.alias);
                rewritten = true;
            }
        }
    }
};
using RenameQualifiedIdentifiersVisitor = InDepthNodeVisitor<RenameQualifiedIdentifiersMatcher, true>;

}

JoinedTables::JoinedTables(Context && context_, const ASTSelectQuery & select_query)
    : context(context_)
    , table_expressions(getTableExpressions(select_query))
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
    return std::make_unique<InterpreterSelectWithUnionQuery>(left_table_expression, context, select_options);
}

StoragePtr JoinedTables::getLeftTableStorage()
{
    if (isLeftTableSubquery())
        return {};

    if (isLeftTableFunction())
        return context.getQueryContext().executeTableFunction(left_table_expression);

    if (left_db_and_table)
    {
        table_id = context.resolveStorageID(StorageID(left_db_and_table->database, left_db_and_table->table, left_db_and_table->uuid));
    }
    else /// If the table is not specified - use the table `system.one`.
    {
        table_id = StorageID("system", "one");
    }

    if (auto view_source = context.getViewSource())
    {
        const auto & storage_values = static_cast<const StorageValues &>(*view_source);
        auto tmp_table_id = storage_values.getStorageID();
        if (tmp_table_id.database_name == table_id.database_name && tmp_table_id.table_name == table_id.table_name)
        {
            /// Read from view source.
            return context.getViewSource();
        }
    }

    /// Read from table. Even without table expression (implicit SELECT ... FROM system.one).
    return DatabaseCatalog::instance().getTable(table_id, context);
}

bool JoinedTables::resolveTables()
{
    tables_with_columns = getDatabaseAndTablesWithColumns(table_expressions, context);
    assert(tables_with_columns.size() == table_expressions.size());

    const auto & settings = context.getSettingsRef();
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
        database = context.getCurrentDatabase();

    for (auto & [subquery, ast_tables] : renamed_tables)
    {
        std::vector<DatabaseAndTableWithAlias> renamed;
        renamed.reserve(ast_tables.size());
        for (auto & ast : ast_tables)
            renamed.emplace_back(DatabaseAndTableWithAlias(*ast->as<ASTIdentifier>(), database));

        /// Change qualified column names in distributed subqueries using table aliases.
        RenameQualifiedIdentifiersVisitor::Data data(renamed);
        RenameQualifiedIdentifiersVisitor(data).visit(subquery);
    }
}

std::shared_ptr<TableJoin> JoinedTables::makeTableJoin(const ASTSelectQuery & select_query)
{
    if (tables_with_columns.size() < 2)
        return {};

    auto settings = context.getSettingsRef();
    auto table_join = std::make_shared<TableJoin>(settings, context.getTemporaryVolume());

    const ASTTablesInSelectQueryElement * ast_join = select_query.join();
    const auto & table_to_join = ast_join->table_expression->as<ASTTableExpression &>();

    /// TODO This syntax does not support specifying a database name.
    if (table_to_join.database_and_table_name)
    {
        auto joined_table_id = context.resolveStorageID(table_to_join.database_and_table_name);
        StoragePtr table = DatabaseCatalog::instance().tryGetTable(joined_table_id, context);
        if (table)
        {
            if (dynamic_cast<StorageJoin *>(table.get()) ||
                dynamic_cast<StorageDictionary *>(table.get()))
                table_join->joined_storage = table;
        }
    }

    if (!table_join->joined_storage &&
        settings.enable_optimize_predicate_expression)
        replaceJoinedTable(select_query);

    return table_join;
}

}
