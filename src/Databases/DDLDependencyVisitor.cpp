#include <Databases/DDLDependencyVisitor.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Poco/String.h>


namespace DB
{

namespace
{
    /// CREATE TABLE or CREATE DICTIONARY or CREATE VIEW or CREATE TEMPORARY TABLE or CREATE DATABASE query.
    void visitCreateQuery(const ASTCreateQuery & create, DDLDependencyVisitor::Data & data)
    {
        if (create.table && !create.temporary)
        {
            /// CREATE TABLE or CREATE DICTIONARY or CREATE VIEW
            data.table_name.table = create.getTable();
            if (!data.table_name.table.empty())
            {
                data.table_name.database = create.getDatabase();
                if (data.table_name.database.empty())
                    data.table_name.database = data.default_database;
            }
        }

        QualifiedTableName to_table{create.to_table_id.database_name, create.to_table_id.table_name};
        if (!to_table.table.empty())
        {
            /// TO target_table (for materialized views)
            if (to_table.database.empty())
                to_table.database = data.default_database;
            data.dependencies.emplace(to_table);
        }

        QualifiedTableName as_table{create.as_database, create.as_table};
        if (!as_table.table.empty())
        {
            /// AS table_name
            if (as_table.database.empty())
                as_table.database = data.default_database;
            data.dependencies.emplace(as_table);
        }
    }

    /// ASTTableExpression represents a reference to a table in SELECT query.
    /// DDLDependencyVisitor should handle ASTTableExpression because some CREATE queries can contain SELECT queries after AS
    /// (for example, CREATE VIEW).
    void visitTableExpression(const ASTTableExpression & expr, DDLDependencyVisitor::Data & data)
    {
        if (!expr.database_and_table_name)
            return;

        const ASTIdentifier * identifier = dynamic_cast<const ASTIdentifier *>(expr.database_and_table_name.get());
        if (!identifier)
            return;

        auto table_identifier = identifier->createTable();
        if (!table_identifier)
            return;

        QualifiedTableName qualified_name{table_identifier->getDatabaseName(), table_identifier->shortName()};
        if (qualified_name.table.empty())
            return;

        if (qualified_name.database.empty())
        {
            /// It can be table/dictionary from default database or XML dictionary, but we cannot distinguish it here.
            qualified_name.database = data.default_database;
        }

        data.dependencies.emplace(qualified_name);
    }

    /// Extracts a table name with optional database written in the form db_name.table_name (as identifier) or 'db_name.table_name' (as string).
    void extractQualifiedTableNameFromArgument(const ASTFunction & function, DDLDependencyVisitor::Data & data, size_t arg_idx)
    {
        /// Just ignore incorrect arguments, proper exception will be thrown later
        if (!function.arguments || function.arguments->children.size() <= arg_idx)
            return;

        QualifiedTableName qualified_name;

        const auto * expr_list = function.arguments->as<ASTExpressionList>();
        if (!expr_list)
            return;

        const auto * arg = expr_list->children[arg_idx].get();
        if (const auto * literal = arg->as<ASTLiteral>())
        {
            if (literal->value.getType() != Field::Types::String)
                return;

            auto maybe_qualified_name = QualifiedTableName::tryParseFromString(literal->value.get<String>());
            /// Just return if name if invalid
            if (!maybe_qualified_name)
                return;

            qualified_name = std::move(*maybe_qualified_name);
        }
        else if (const auto * identifier = dynamic_cast<const ASTIdentifier *>(arg))
        {
            /// ASTIdentifier or ASTTableIdentifier
            auto table_identifier = identifier->createTable();
            /// Just return if table identified is invalid
            if (!table_identifier)
                return;

            qualified_name.database = table_identifier->getDatabaseName();
            qualified_name.table = table_identifier->shortName();
        }
        else
        {
            /// Just return because we don't validate AST in this function.
            return;
        }

        if (qualified_name.database.empty())
        {
            /// It can be table/dictionary from default database or XML dictionary, but we cannot distinguish it here.
            qualified_name.database = data.default_database;
        }
        data.dependencies.emplace(std::move(qualified_name));
    }

    /// Extracts a table name with database written in the form 'db_name', 'table_name' (two strings).
    void extractDatabaseAndTableNameFromArguments(const ASTFunction & function, DDLDependencyVisitor::Data & data, size_t database_arg_idx, size_t table_arg_idx)
    {
        /// Just ignore incorrect arguments, proper exception will be thrown later
        if (!function.arguments || (function.arguments->children.size() <= database_arg_idx)
            || (function.arguments->children.size() <= table_arg_idx))
            return;

        const auto * expr_list = function.arguments->as<ASTExpressionList>();
        if (!expr_list)
            return;

        auto database_literal = expr_list->children[database_arg_idx]->as<ASTLiteral>();
        auto table_name_literal = expr_list->children[table_arg_idx]->as<ASTLiteral>();

        if (!database_literal || !table_name_literal || (database_literal->value.getType() != Field::Types::String)
            || (table_name_literal->value.getType() != Field::Types::String))
            return;

        QualifiedTableName qualified_name{database_literal->value.get<String>(), table_name_literal->value.get<String>()};
        if (qualified_name.table.empty())
            return;

        if (qualified_name.database.empty())
            qualified_name.database = data.default_database;

        data.dependencies.emplace(qualified_name);
    }

    void visitFunction(const ASTFunction & function, DDLDependencyVisitor::Data & data)
    {
        if (function.name == "joinGet" || function.name == "dictHas" || function.name == "dictIsIn" || function.name.starts_with("dictGet"))
        {
            /// dictGet('dict_name', attr_names, id_expr)
            /// dictHas('dict_name', id_expr)
            /// joinGet(join_storage_table_name, `value_column`, join_keys)
            extractQualifiedTableNameFromArgument(function, data, 0);
        }
        else if (function.name == "in" || function.name == "notIn" || function.name == "globalIn" || function.name == "globalNotIn")
        {
            /// in(x, table_name) - function for evaluating (x IN table_name)
            extractQualifiedTableNameFromArgument(function, data, 1);
        }
        else if (function.name == "dictionary")
        {
            /// dictionary(dict_name)
            extractQualifiedTableNameFromArgument(function, data, 0);
        }
    }

    void visitTableEngine(const ASTStorage & storage, DDLDependencyVisitor::Data & data)
    {
        if (!storage.engine)
            return;

        if (storage.engine->name == "Dictionary")
            extractQualifiedTableNameFromArgument(*storage.engine, data, 0);

        if (storage.engine->name == "Buffer")
            extractDatabaseAndTableNameFromArguments(*storage.engine, data, 0, 1);
    }

    void visitDictionaryDef(const ASTDictionary & dictionary, DDLDependencyVisitor::Data & data)
    {
        if (!dictionary.source || dictionary.source->name != "clickhouse" || !dictionary.source->elements)
            return;

        auto config = getDictionaryConfigurationFromAST(data.create_query->as<ASTCreateQuery &>(), data.global_context);
        auto info = getInfoIfClickHouseDictionarySource(config, data.global_context);

        if (!info || !info->is_local)
            return;

        if (info->table_name.database.empty())
            info->table_name.database = data.default_database;
        data.dependencies.emplace(std::move(info->table_name));
    }
}


TableNamesSet getDependenciesSetFromCreateQuery(const ASTPtr & ast, const ContextPtr & global_context)
{
    assert(global_context == global_context->getGlobalContext());
    DDLDependencyVisitor::Data data;
    data.default_database = global_context->getCurrentDatabase();
    data.create_query = ast;
    data.global_context = global_context;
    DDLDependencyVisitor::Visitor visitor{data};
    visitor.visit(ast);
    data.dependencies.erase(data.table_name);
    return data.dependencies;
}

void DDLDependencyVisitor::visit(const ASTPtr & ast, Data & data)
{
    if (auto * create = ast->as<ASTCreateQuery>())
        visitCreateQuery(*create, data);
    else if (const auto * function = ast->as<ASTFunction>())
        visitFunction(*function, data);
    else if (auto * dictionary = ast->as<ASTDictionary>())
        visitDictionaryDef(*dictionary, data);
    else if (const auto * storage = ast->as<ASTStorage>())
        visitTableEngine(*storage, data);
    else if (auto * expr = ast->as<ASTTableExpression>())
        visitTableExpression(*expr, data);
}

bool DDLDependencyVisitor::needChildVisit(const ASTPtr &, const ASTPtr &)
{
    return true;
}

}
