#include <Backups/DDLRenamingVisitor.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int WRONG_DDL_RENAMING_SETTINGS;
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// Replaces names of tables and databases used in a CREATE query, which can be either CREATE TABLE or
    /// CREATE DICTIONARY or CREATE VIEW or CREATE TEMPORARY TABLE or CREATE DATABASE query.
    void visitCreateQuery(ASTCreateQuery & create, const DDLRenamingVisitor::Data & data)
    {
        if (create.table)
        {
            /// CREATE TABLE or CREATE DICTIONARY or CREATE VIEW or CREATE TEMPORARY TABLE
            QualifiedTableName table_name;
            table_name.table = create.getTable();
            if (create.temporary)
                table_name.database = DatabaseCatalog::TEMPORARY_DATABASE;
            else if (create.database)
                table_name.database = create.getDatabase();
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name specified in the CREATE TABLE query must not be empty");

            auto new_table_name = data.renaming_settings.getNewTableName(table_name);

            if (new_table_name != table_name)
            {
                create.setTable(new_table_name.table);
                if (new_table_name.database == DatabaseCatalog::TEMPORARY_DATABASE)
                {
                    create.temporary = true;
                    create.setDatabase("");
                }
                else
                {
                    create.temporary = false;
                    create.setDatabase(new_table_name.database);
                }
            }
        }
        else if (create.database)
        {
            /// CREATE DATABASE
            String database_name = create.getDatabase();
            String new_database_name = data.renaming_settings.getNewDatabaseName(database_name);
            create.setDatabase(new_database_name);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name specified in the CREATE DATABASE query must not be empty");

        QualifiedTableName as_table{create.as_database, create.as_table};
        if (!as_table.table.empty() && !as_table.database.empty())
        {
            auto as_table_new = data.renaming_settings.getNewTableName(as_table);
            create.as_database = as_table_new.database;
            create.as_table = as_table_new.table;
        }

        QualifiedTableName to_table{create.to_table_id.database_name, create.to_table_id.table_name};
        if (!to_table.table.empty() && !to_table.database.empty())
        {
            auto to_table_new = data.renaming_settings.getNewTableName(to_table);
            if (to_table_new != to_table)
                create.to_table_id = StorageID{to_table_new.database, to_table_new.table};
        }
    }

    /// Replaces names of a database and a table in a expression like `db`.`table`
    void visitTableExpression(ASTTableExpression & expr, const DDLRenamingVisitor::Data & data)
    {
        if (!expr.database_and_table_name)
            return;

        ASTIdentifier * id = expr.database_and_table_name->as<ASTIdentifier>();
        if (!id)
            return;

        auto table_id = id->createTable();
        if (!table_id)
            return;

        QualifiedTableName table_name{table_id->getDatabaseName(), table_id->shortName()};
        if (table_name.table.empty() || table_name.database.empty())
            return;

        auto new_table_name = data.renaming_settings.getNewTableName(table_name);
        if (new_table_name == table_name)
            return;

        expr.database_and_table_name = std::make_shared<ASTIdentifier>(new_table_name.getParts());
        expr.children.push_back(expr.database_and_table_name);
    }

    /// Replaces a database's name passed via an argument of the function merge() or the table engine Merge.
    void visitFunctionMerge(ASTFunction & function, const DDLRenamingVisitor::Data & data)
    {
        if (!function.arguments)
            return;

        /// The first argument is a database's name and we can rename it.
        /// The second argument is a regular expression and we can do nothing about it.
        auto & args = function.arguments->as<ASTExpressionList &>().children;
        size_t db_name_arg_index = 0;
        if (args.size() <= db_name_arg_index)
            return;

        String db_name = evaluateConstantExpressionForDatabaseName(args[db_name_arg_index], data.context)->as<ASTLiteral &>().value.safeGet<String>();
        if (db_name.empty())
            return;

        String new_db_name = data.renaming_settings.getNewDatabaseName(db_name);
        if (new_db_name == db_name)
            return;
        args[db_name_arg_index] = std::make_shared<ASTLiteral>(new_db_name);
    }

    /// Replaces names of a table and a database passed via arguments of the function remote() or cluster() or the table engine Distributed.
    void visitFunctionRemote(ASTFunction & function, const DDLRenamingVisitor::Data & data)
    {
        if (!function.arguments)
            return;

        /// The first argument is an address or cluster's name, so we skip it.
        /// The second argument can be either 'db.name' or just 'db' followed by the third argument 'table'.
        auto & args = function.arguments->as<ASTExpressionList &>().children;

        const auto * second_arg_as_function = args[1]->as<ASTFunction>();
        if (second_arg_as_function && TableFunctionFactory::instance().isTableFunctionName(second_arg_as_function->name))
            return;

        size_t db_name_index = 1;
        if (args.size() <= db_name_index)
            return;

        String name = evaluateConstantExpressionForDatabaseName(args[db_name_index], data.context)->as<ASTLiteral &>().value.safeGet<String>();

        size_t table_name_index = static_cast<size_t>(-1);

        QualifiedTableName table_name;

        if (function.name == "Distributed")
            table_name.table = name;
        else
            table_name = QualifiedTableName::parseFromString(name);

        if (table_name.database.empty())
        {
            std::swap(table_name.database, table_name.table);
            table_name_index = 2;
            if (args.size() <= table_name_index)
                return;
            table_name.table = evaluateConstantExpressionForDatabaseName(args[table_name_index], data.context)->as<ASTLiteral &>().value.safeGet<String>();
        }

        if (table_name.table.empty() || table_name.database.empty())
            return;

        auto new_table_name = data.renaming_settings.getNewTableName(table_name);
        if (new_table_name == table_name)
            return;

        if (table_name_index != static_cast<size_t>(-1))
        {
            args[db_name_index] = std::make_shared<ASTLiteral>(new_table_name.database);
            args[table_name_index] = std::make_shared<ASTLiteral>(new_table_name.table);
        }
        else
        {
            args[db_name_index] = std::make_shared<ASTLiteral>(new_table_name.database);
            args.insert(args.begin() + db_name_index + 1, std::make_shared<ASTLiteral>(new_table_name.table));
        }
    }

    /// Replaces names of tables and databases used in arguments of a table function or a table engine.
    void visitFunction(ASTFunction & function, const DDLRenamingVisitor::Data & data)
    {
        if ((function.name == "merge") || (function.name == "Merge"))
        {
            visitFunctionMerge(function, data);
        }
        else if ((function.name == "remote") || (function.name == "remoteSecure") || (function.name == "cluster") ||
                 (function.name == "clusterAllReplicas") || (function.name == "Distributed"))
        {
            visitFunctionRemote(function, data);
        }
    }

    /// Replaces names of a table and a database used in source parameters of a dictionary.
    void visitDictionary(ASTDictionary & dictionary, const DDLRenamingVisitor::Data & data)
    {
        if (!dictionary.source || dictionary.source->name != "clickhouse" || !dictionary.source->elements)
            return;

        auto & elements = dictionary.source->elements->as<ASTExpressionList &>().children;
        size_t db_name_index = static_cast<size_t>(-1);
        size_t table_name_index = static_cast<size_t>(-1);
        QualifiedTableName table_name;

        for (size_t i = 0; i != elements.size(); ++i)
        {
            auto & pair = elements[i]->as<ASTPair &>();
            if (pair.first == "db")
            {
                if (db_name_index != static_cast<size_t>(-1))
                    return;
                table_name.database = pair.second->as<ASTLiteral &>().value.safeGet<String>();
                db_name_index = i;
            }
            else if (pair.first == "table")
            {
                if (table_name_index != static_cast<size_t>(-1))
                    return;
                table_name.table = pair.second->as<ASTLiteral &>().value.safeGet<String>();
                table_name_index = i;
            }
        }

        if (table_name.table.empty() || table_name.database.empty())
            return;

        auto new_table_name = data.renaming_settings.getNewTableName(table_name);
        if (new_table_name == table_name)
            return;

        if (new_table_name.database != table_name.database)
        {
            auto & pair = elements[db_name_index]->as<ASTPair &>();
            pair.replace(pair.second, std::make_shared<ASTLiteral>(new_table_name.database));
        }
        if (new_table_name.table != table_name.table)
        {
            auto & pair = elements[table_name_index]->as<ASTPair &>();
            pair.replace(pair.second, std::make_shared<ASTLiteral>(new_table_name.table));
        }
    }
}


void DDLRenamingSettings::setNewTableName(const QualifiedTableName & old_table_name, const QualifiedTableName & new_table_name)
{
    if (old_table_name.table.empty() || old_table_name.database.empty() || new_table_name.table.empty() || new_table_name.database.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty names are not allowed for DDLRenamingSettings::setNewTableName");

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

void DDLRenamingSettings::setNewDatabaseName(const String & old_database_name, const String & new_database_name)
{
    if (old_database_name.empty() || new_database_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty names are not allowed for DDLRenamingSettings::setNewDatabaseName");

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

void DDLRenamingSettings::setFromBackupQuery(const ASTBackupQuery & backup_query)
{
    setFromBackupQuery(backup_query.elements);
}

void DDLRenamingSettings::setFromBackupQuery(const ASTBackupQuery::Elements & backup_query_elements)
{
    old_to_new_table_names.clear();
    old_to_new_database_names.clear();

    using ElementType = ASTBackupQuery::ElementType;

    for (const auto & element : backup_query_elements)
    {
        switch (element.type)
        {
            case ElementType::TABLE:
            {
                const String & table_name = element.table_name;
                const String & new_table_name = element.new_table_name;
                String database_name = element.database_name;
                String new_database_name = element.new_database_name;
                if (element.is_temporary_database)
                {
                    database_name = DatabaseCatalog::TEMPORARY_DATABASE;
                    new_database_name = DatabaseCatalog::TEMPORARY_DATABASE;
                }
                assert(!table_name.empty());
                assert(!database_name.empty());
                assert(!new_table_name.empty());
                assert(!new_database_name.empty());
                setNewTableName({database_name, table_name}, {new_database_name, new_table_name});
                break;
            }

            case ASTBackupQuery::DATABASE:
            {
                const String & database_name = element.database_name;
                const String & new_database_name = element.new_database_name;
                assert(!database_name.empty());
                assert(!new_database_name.empty());
                setNewDatabaseName(database_name, new_database_name);
                break;
            }

            case ASTBackupQuery::ALL_DATABASES: break;
        }
    }
}

QualifiedTableName DDLRenamingSettings::getNewTableName(const QualifiedTableName & old_table_name) const
{
    auto it = old_to_new_table_names.find(old_table_name);
    if (it != old_to_new_table_names.end())
        return it->second;
    return {getNewDatabaseName(old_table_name.database), old_table_name.table};
}


const String & DDLRenamingSettings::getNewDatabaseName(const String & old_database_name) const
{
    auto it = old_to_new_database_names.find(old_database_name);
    if (it != old_to_new_database_names.end())
        return it->second;
    return old_database_name;
}


bool DDLRenamingVisitor::needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

void DDLRenamingVisitor::visit(ASTPtr ast, const Data & data)
{
    if (auto * create = ast->as<ASTCreateQuery>())
        visitCreateQuery(*create, data);
    else if (auto * expr = ast->as<ASTTableExpression>())
        visitTableExpression(*expr, data);
    else if (auto * function = ast->as<ASTFunction>())
        visitFunction(*function, data);
    else if (auto * dictionary = ast->as<ASTDictionary>())
        visitDictionary(*dictionary, data);
}

void renameInCreateQuery(ASTPtr & ast, const DDLRenamingSettings & renaming_settings, const ContextPtr & context)
{
    try
    {
        DDLRenamingVisitor::Data data{renaming_settings, context};
        DDLRenamingVisitor::Visitor{data}.visit(ast);
    }
    catch (...)
    {
        tryLogCurrentException("Backup", "Error while renaming in AST");
    }
}

}
