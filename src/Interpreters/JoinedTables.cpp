#include <Interpreters/JoinedTables.h>
#include <Interpreters/Context.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/InJoinSubqueriesPreprocessor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/IStorage.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageValues.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALIAS_REQUIRED;
    extern const int AMBIGUOUS_COLUMN_NAME;
}

namespace
{

template <typename T>
void checkTablesWithColumns(const std::vector<T> & tables_with_columns, const Context & context)
{
    auto & settings = context.getSettingsRef();
    if (settings.joined_subquery_requires_alias && tables_with_columns.size() > 1)
    {
        for (auto & t : tables_with_columns)
            if (t.table.table.empty() && t.table.alias.empty())
                throw Exception("No alias for subquery or table function in JOIN (set joined_subquery_requires_alias=0 to disable restriction).",
                                ErrorCodes::ALIAS_REQUIRED);
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
        for (auto & table : data)
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
        for (auto & table : data)
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
        auto & storage_values = static_cast<const StorageValues &>(*view_source);
        auto tmp_table_id = storage_values.getStorageID();
        if (tmp_table_id.database_name == table_id.database_name && tmp_table_id.table_name == table_id.table_name)
        {
            /// Read from view source.
            return context.getViewSource();
        }
    }

    /// Read from table. Even without table expression (implicit SELECT ... FROM system.one).
    return DatabaseCatalog::instance().getTable(table_id);
}

bool JoinedTables::resolveTables()
{
    tables_with_columns = getDatabaseAndTablesWithColumns(table_expressions, context);
    checkTablesWithColumns(tables_with_columns, context);

    return !tables_with_columns.empty();
}

void JoinedTables::makeFakeTable(StoragePtr storage, const Block & source_header)
{
    if (storage)
    {
        const ColumnsDescription & storage_columns = storage->getColumns();
        tables_with_columns.emplace_back(DatabaseAndTableWithAlias{}, storage_columns.getOrdinary());

        auto & table = tables_with_columns.back();
        table.addHiddenColumns(storage_columns.getMaterialized());
        table.addHiddenColumns(storage_columns.getAliases());
        table.addHiddenColumns(storage_columns.getVirtuals());
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

}
