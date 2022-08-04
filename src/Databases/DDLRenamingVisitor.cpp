#include <Databases/DDLRenamingVisitor.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Common/isLocalAddress.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Poco/Net/NetException.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int WRONG_DDL_RENAMING_SETTINGS;
}

namespace
{
    /// CREATE TABLE or CREATE DICTIONARY or CREATE VIEW or CREATE TEMPORARY TABLE or CREATE DATABASE query.
    void visitCreateQuery(ASTCreateQuery & create, const DDLRenamingVisitor::Data & data)
    {
        if (create.temporary)
        {
            /// CREATE TEMPORARY TABLE
            String table_name = create.getTable();
            QualifiedTableName full_table_name{DatabaseCatalog::TEMPORARY_DATABASE, table_name};
            const auto & new_table_name = data.renaming_map.getNewTableName(full_table_name);
            if (new_table_name != full_table_name)
            {
                create.setTable(new_table_name.table);
                if (new_table_name.database != DatabaseCatalog::TEMPORARY_DATABASE)
                {
                    create.temporary = false;
                    create.setDatabase(new_table_name.database);
                }
            }
        }
        else if (create.table)
        {
            /// CREATE TABLE or CREATE DICTIONARY or CREATE VIEW
            QualifiedTableName full_name;
            full_name.table = create.getTable();
            full_name.database = create.getDatabase();

            if (!full_name.database.empty() && !full_name.table.empty())
            {
                auto new_table_name = data.renaming_map.getNewTableName(full_name);
                if (new_table_name != full_name)
                {
                    create.setTable(new_table_name.table);
                    if (new_table_name.database == DatabaseCatalog::TEMPORARY_DATABASE)
                    {
                        create.temporary = true;
                        create.setDatabase("");
                    }
                    else
                    {
                        create.setDatabase(new_table_name.database);
                    }
                }
            }
        }
        else if (create.database)
        {
            /// CREATE DATABASE
            String database_name = create.getDatabase();
            if (!database_name.empty())
            {
                String new_database_name = data.renaming_map.getNewDatabaseName(database_name);
                create.setDatabase(new_database_name);
            }
        }

        QualifiedTableName as_table{create.as_database, create.as_table};
        if (!as_table.table.empty() && !as_table.database.empty())
        {
            auto as_table_new = data.renaming_map.getNewTableName(as_table);
            create.as_database = as_table_new.database;
            create.as_table = as_table_new.table;
        }

        QualifiedTableName to_table{create.to_table_id.database_name, create.to_table_id.table_name};
        if (!to_table.table.empty() && !to_table.database.empty())
        {
            auto to_table_new = data.renaming_map.getNewTableName(to_table);
            if (to_table_new != to_table)
                create.to_table_id = StorageID{to_table_new.database, to_table_new.table};
        }
    }

    /// ASTTableExpression represents a reference to a table in SELECT query.
    /// DDLRenamingVisitor should handle ASTTableExpression because some CREATE queries can contain SELECT queries after AS (e.g. CREATE VIEW).
    /// We'll try to replace database and table name.
    void visitTableExpression(ASTTableExpression & expr, const DDLRenamingVisitor::Data & data)
    {
        if (!expr.database_and_table_name)
            return;

        ASTIdentifier * identifier = dynamic_cast<ASTIdentifier *>(expr.database_and_table_name.get());
        if (!identifier)
            return;

        auto table_identifier = identifier->createTable();
        if (!table_identifier)
            return;

        QualifiedTableName qualified_name{table_identifier->getDatabaseName(), table_identifier->shortName()};
        if (qualified_name.table.empty() || qualified_name.database.empty())
            return;

        auto new_qualified_name = data.renaming_map.getNewTableName(qualified_name);
        if (new_qualified_name == qualified_name)
            return;

        expr.database_and_table_name = std::make_shared<ASTTableIdentifier>(new_qualified_name.database, new_qualified_name.table);
        expr.children.push_back(expr.database_and_table_name);
    }

    /// ASTDictionary keeps a dictionary definition, for example
    /// PRIMARY KEY key_column1, key_column2
    /// SOURCE(CLICKHOUSE(host '127.0.0.1' port 9000 user 'default' password '' db 'default' table 'ids' where 'id=10' query 'SELECT id, value_1, value_2 FROM default.ids'))
    /// LAYOUT ... LIFETIME ... RANGE ...
    ///
    /// We'll try to replace database and table name in SOURCE if the specified `host` is local.
    /// TODO: Probably we could try to replace database and table name in `query` too.
    void visitDictionaryDef(ASTDictionary & dictionary, const DDLRenamingVisitor::Data & data)
    {
        if (!dictionary.source || dictionary.source->name != "clickhouse" || !dictionary.source->elements)
            return;

        auto config = getDictionaryConfigurationFromAST(data.create_query->as<ASTCreateQuery &>(), data.global_context);
        auto info = getInfoIfClickHouseDictionarySource(config, data.global_context);
        if (!info || !info->is_local)
            return;

        auto * source_list = dictionary.source->elements->as<ASTExpressionList>();
        if (!source_list)
            return;

        auto & source_elements = source_list->children;

        Field * database_name_field = nullptr;
        Field * table_name_field = nullptr;

        for (const auto & source_element : source_elements)
        {
            if (!source_element)
                continue;

            auto * pair = source_element->as<ASTPair>();
            if (!pair || !pair->second)
                continue;

            auto * literal = pair->second->as<ASTLiteral>();
            if (!literal)
                continue;

            if (literal->value.getType() == Field::Types::String)
            {
                if (pair->first == "db")
                    database_name_field = &literal->value;
                else if (pair->first == "table")
                    table_name_field = &literal->value;
            }
        }

        if (database_name_field && table_name_field)
        {
            QualifiedTableName qualified_name{database_name_field->get<String>(), table_name_field->get<String>()};
            if (!qualified_name.database.empty() && !qualified_name.table.empty())
            {
                auto new_qualified_name = data.renaming_map.getNewTableName(qualified_name);
                if (new_qualified_name != qualified_name)
                {
                    *database_name_field = new_qualified_name.database;
                    *table_name_field = new_qualified_name.table;
                }
            }
        }
    }

    /// Replaces a qualified table name in a specified function's argument.
    /// It can be either a string or an identifier with a dot in the middle.
    void replaceTableNameInArgument(const ASTFunction & function, const DDLRenamingVisitor::Data & data, size_t arg_idx)
    {
        /// Just ignore incorrect arguments, proper exception will be thrown later
        if (!function.arguments || function.arguments->children.size() <= arg_idx)
            return;

        auto * expr_list = function.arguments->as<ASTExpressionList>();
        if (!expr_list)
            return;

        auto & arg = expr_list->children[arg_idx];
        if (auto * literal = arg->as<ASTLiteral>())
        {
            if (literal->value.getType() != Field::Types::String)
                return;

            auto maybe_qualified_name = QualifiedTableName::tryParseFromString(literal->value.get<String>());
            /// Just return if name if invalid
            if (!maybe_qualified_name || maybe_qualified_name->database.empty() || maybe_qualified_name->table.empty())
                return;

            auto new_qualified_name = data.renaming_map.getNewTableName(*maybe_qualified_name);
            literal->value = new_qualified_name.getFullName();
            return;
        }

        if (const auto * identifier = dynamic_cast<const ASTIdentifier *>(arg.get()))
        {
            /// ASTIdentifier or ASTTableIdentifier
            auto table_identifier = identifier->createTable();
            /// Just return if table identified is invalid
            if (!table_identifier)
                return;

            QualifiedTableName qualified_name{table_identifier->getDatabaseName(), table_identifier->shortName()};
            if (qualified_name.database.empty() || qualified_name.table.empty())
                return;

            auto new_qualified_name = data.renaming_map.getNewTableName(qualified_name);
            arg = std::make_shared<ASTTableIdentifier>(new_qualified_name.database, new_qualified_name.table);
            return;
        }
    }

    /// Replaces a qualified database name in a specified function's argument.
    void replaceDatabaseNameInArguments(const ASTFunction & function, const DDLRenamingVisitor::Data & data, size_t arg_idx)
    {
        /// Just ignore incorrect arguments, proper exception will be thrown later
        if (!function.arguments || function.arguments->children.size() <= arg_idx)
            return;

        auto & arg = function.arguments->as<ASTExpressionList>()->children[arg_idx];
        auto * literal = arg->as<ASTLiteral>();
        if (!literal || (literal->value.getType() != Field::Types::String))
            return;

        auto database_name = literal->value.get<String>();
        if (database_name.empty())
            return;

        auto new_database_name = data.renaming_map.getNewDatabaseName(database_name);
        literal->value = new_database_name;
    }

    void visitTableEngine(ASTStorage & storage, const DDLRenamingVisitor::Data & data)
    {
        if (!storage.engine)
            return;

        if (storage.engine->name == "Dictionary")
        {
            /// Syntax: CREATE TABLE table_name(<fields>) engine = Dictionary('dictionary_name')
            /// We'll try to replace the dictionary name.
            replaceTableNameInArgument(*storage.engine, data, 0);
        }
        else if (storage.engine->name == "Merge")
        {
            /// Syntax: CREATE TABLE ... Engine=Merge(db_name, tables_regexp)
            /// We'll try to replace the database name but we can do nothing to 'tables_regexp'.
            replaceDatabaseNameInArguments(*storage.engine, data, 0);
        }
    }

    void visitFunction(const ASTFunction & function, const DDLRenamingVisitor::Data & data)
    {
        if (function.name == "joinGet" ||
            function.name == "dictHas" ||
            function.name == "dictIsIn" ||
            function.name.starts_with("dictGet"))
        {
            replaceTableNameInArgument(function, data, 0);
        }
        else if (Poco::toLower(function.name) == "in")
        {
            replaceTableNameInArgument(function, data, 1);
        }
        else if (function.name == "merge")
        {
            /// Syntax: merge('db_name', 'tables_regexp')
            /// We'll try to replace the database name but we can do nothing to 'tables_regexp'.
            replaceDatabaseNameInArguments(function, data, 0);
        }
    }
}

void DDLRenamingVisitor::visit(ASTPtr ast, const Data & data)
{
    if (auto * create = ast->as<ASTCreateQuery>())
        visitCreateQuery(*create, data);
    else if (auto * expr = ast->as<ASTTableExpression>())
        visitTableExpression(*expr, data);
    else if (auto * function = ast->as<ASTFunction>())
        visitFunction(*function, data);
    else if (auto * dictionary = ast->as<ASTDictionary>())
        visitDictionaryDef(*dictionary, data);
    else if (auto * storage = ast->as<ASTStorage>())
        visitTableEngine(*storage, data);
}

bool DDLRenamingVisitor::needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }


void DDLRenamingMap::setNewTableName(const QualifiedTableName & old_table_name, const QualifiedTableName & new_table_name)
{
    if (old_table_name.table.empty() || old_table_name.database.empty() || new_table_name.table.empty() || new_table_name.database.empty())
        throw Exception(ErrorCodes::WRONG_DDL_RENAMING_SETTINGS, "Empty names are not allowed");

    auto it = old_to_new_table_names.find(old_table_name);
    if ((it != old_to_new_table_names.end()))
    {
        if (it->second == new_table_name)
            return;
        throw Exception(
            ErrorCodes::WRONG_DDL_RENAMING_SETTINGS,
            "Wrong renaming: it's specified that table {} should be renamed to {} and to {} at the same time",
            old_table_name.getFullName(),
            it->second.getFullName(),
            new_table_name.getFullName());
    }
    old_to_new_table_names[old_table_name] = new_table_name;
}

void DDLRenamingMap::setNewDatabaseName(const String & old_database_name, const String & new_database_name)
{
    if (old_database_name.empty() || new_database_name.empty())
        throw Exception(ErrorCodes::WRONG_DDL_RENAMING_SETTINGS, "Empty names are not allowed");

    auto it = old_to_new_database_names.find(old_database_name);
    if ((it != old_to_new_database_names.end()))
    {
        if (it->second == new_database_name)
            return;
        throw Exception(ErrorCodes::WRONG_DDL_RENAMING_SETTINGS, "Wrong renaming: it's specified that database {} should be renamed to {} and to {} at the same time",
                        backQuoteIfNeed(old_database_name), backQuoteIfNeed(it->second), backQuoteIfNeed(new_database_name));
    }
    old_to_new_database_names[old_database_name] = new_database_name;
}


const String & DDLRenamingMap::getNewDatabaseName(const String & old_database_name) const
{
    auto it = old_to_new_database_names.find(old_database_name);
    if (it != old_to_new_database_names.end())
        return it->second;
    return old_database_name;
}

QualifiedTableName DDLRenamingMap::getNewTableName(const QualifiedTableName & old_table_name) const
{
    auto it = old_to_new_table_names.find(old_table_name);
    if (it != old_to_new_table_names.end())
        return it->second;
    return {getNewDatabaseName(old_table_name.database), old_table_name.table};
}


void renameDatabaseAndTableNameInCreateQuery(ASTPtr ast, const DDLRenamingMap & renaming_map, const ContextPtr & global_context)
{
    DDLRenamingVisitor::Data data{ast, renaming_map, global_context};
    DDLRenamingVisitor::Visitor{data}.visit(ast);
}

}
