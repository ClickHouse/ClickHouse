#include <Backups/renameInCreateQuery.h>
#include <Backups/BackupRenamingConfig.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    class RenameInCreateQueryTransformMatcher
    {
    public:
        struct Data
        {
            BackupRenamingConfigPtr renaming_config;
            ContextPtr context;
        };

        static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

        static void visit(ASTPtr & ast, const Data & data)
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

    private:
        /// Replaces names of tables and databases used in a CREATE query, which can be either CREATE TABLE or
        /// CREATE DICTIONARY or CREATE VIEW or CREATE TEMPORARY TABLE or CREATE DATABASE query.
        static void visitCreateQuery(ASTCreateQuery & create, const Data & data)
        {
            if (create.temporary)
            {
                if (create.table.empty())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Table name specified in the CREATE TEMPORARY TABLE query must not be empty");
                create.table = data.renaming_config->getNewTemporaryTableName(create.table);
            }
            else if (create.table.empty())
            {
                if (create.database.empty())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name specified in the CREATE DATABASE query must not be empty");
                create.database = data.renaming_config->getNewDatabaseName(create.database);
            }
            else
            {
                if (create.database.empty())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name specified in the CREATE TABLE query must not be empty");
                std::tie(create.database, create.table) = data.renaming_config->getNewTableName({create.database, create.table});
            }

            create.uuid = UUIDHelpers::Nil;

            if (!create.as_table.empty() && !create.as_database.empty())
                std::tie(create.as_database, create.as_table) = data.renaming_config->getNewTableName({create.as_database, create.as_table});

            if (!create.to_table_id.table_name.empty() && !create.to_table_id.database_name.empty())
            {
                auto to_table = data.renaming_config->getNewTableName({create.to_table_id.database_name, create.to_table_id.table_name});
                create.to_table_id = StorageID{to_table.first, to_table.second};
            }
        }

        /// Replaces names of a database and a table in a expression like `db`.`table`
        static void visitTableExpression(ASTTableExpression & expr, const Data & data)
        {
            if (!expr.database_and_table_name)
                return;

            ASTIdentifier * id = expr.database_and_table_name->as<ASTIdentifier>();
            if (!id)
                return;

            auto table_id = id->createTable();
            if (!table_id)
                return;

            const String & db_name = table_id->getDatabaseName();
            const String & table_name = table_id->shortName();
            if (db_name.empty() || table_name.empty())
                return;

            String new_db_name, new_table_name;
            std::tie(new_db_name, new_table_name) = data.renaming_config->getNewTableName({db_name, table_name});
            if ((new_db_name == db_name) && (new_table_name == table_name))
                return;

            expr.database_and_table_name = std::make_shared<ASTIdentifier>(Strings{new_db_name, new_table_name});
            expr.children.push_back(expr.database_and_table_name);
        }

        /// Replaces names of tables and databases used in arguments of a table function or a table engine.
        static void visitFunction(ASTFunction & function, const Data & data)
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

        /// Replaces a database's name passed via an argument of the function merge() or the table engine Merge.
        static void visitFunctionMerge(ASTFunction & function, const Data & data)
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

            String new_db_name = data.renaming_config->getNewDatabaseName(db_name);
            if (new_db_name == db_name)
                return;
            args[db_name_arg_index] = std::make_shared<ASTLiteral>(new_db_name);
        }

        /// Replaces names of a table and a database passed via arguments of the function remote() or cluster() or the table engine Distributed.
        static void visitFunctionRemote(ASTFunction & function, const Data & data)
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

            QualifiedTableName qualified_name;

            if (function.name == "Distributed")
                qualified_name.table = name;
            else
                qualified_name = QualifiedTableName::parseFromString(name);

            if (qualified_name.database.empty())
            {
                std::swap(qualified_name.database, qualified_name.table);
                table_name_index = 2;
                if (args.size() <= table_name_index)
                    return;
                qualified_name.table = evaluateConstantExpressionForDatabaseName(args[table_name_index], data.context)->as<ASTLiteral &>().value.safeGet<String>();
            }

            const String & db_name = qualified_name.database;
            const String & table_name = qualified_name.table;

            if (db_name.empty() || table_name.empty())
                return;

            String new_db_name, new_table_name;
            std::tie(new_db_name, new_table_name) = data.renaming_config->getNewTableName({db_name, table_name});
            if ((new_db_name == db_name) && (new_table_name == table_name))
                return;

            if (table_name_index != static_cast<size_t>(-1))
            {
                if (new_db_name != db_name)
                    args[db_name_index] = std::make_shared<ASTLiteral>(new_db_name);
                if (new_table_name != table_name)
                    args[table_name_index] = std::make_shared<ASTLiteral>(new_table_name);
            }
            else
            {
                args[db_name_index] = std::make_shared<ASTLiteral>(new_db_name);
                args.insert(args.begin() + db_name_index + 1, std::make_shared<ASTLiteral>(new_table_name));
            }
        }

        /// Replaces names of a table and a database used in source parameters of a dictionary.
        static void visitDictionary(ASTDictionary & dictionary, const Data & data)
        {
            if (!dictionary.source || dictionary.source->name != "clickhouse" || !dictionary.source->elements)
                return;

            auto & elements = dictionary.source->elements->as<ASTExpressionList &>().children;
            String db_name, table_name;
            size_t db_name_index = static_cast<size_t>(-1);
            size_t table_name_index = static_cast<size_t>(-1);

            for (size_t i = 0; i != elements.size(); ++i)
            {
                auto & pair = elements[i]->as<ASTPair &>();
                if (pair.first == "db")
                {
                    if (db_name_index != static_cast<size_t>(-1))
                        return;
                    db_name = pair.second->as<ASTLiteral &>().value.safeGet<String>();
                    db_name_index = i;
                }
                else if (pair.first == "table")
                {
                    if (table_name_index != static_cast<size_t>(-1))
                        return;
                    table_name = pair.second->as<ASTLiteral &>().value.safeGet<String>();
                    table_name_index = i;
                }
            }

            if (db_name.empty() || table_name.empty())
                return;

            String new_db_name, new_table_name;
            std::tie(new_db_name, new_table_name) = data.renaming_config->getNewTableName({db_name, table_name});
            if ((new_db_name == db_name) && (new_table_name == table_name))
                return;

            if (new_db_name != db_name)
            {
                auto & pair = elements[db_name_index]->as<ASTPair &>();
                pair.replace(pair.second, std::make_shared<ASTLiteral>(new_db_name));
            }
            if (new_table_name != table_name)
            {
                auto & pair = elements[table_name_index]->as<ASTPair &>();
                pair.replace(pair.second, std::make_shared<ASTLiteral>(new_table_name));
            }
        }
    };

    using RenameInCreateQueryTransformVisitor = InDepthNodeVisitor<RenameInCreateQueryTransformMatcher, false>;
}


ASTPtr renameInCreateQuery(const ASTPtr & ast, const BackupRenamingConfigPtr & renaming_config, const ContextPtr & context)
{
    auto new_ast = ast->clone();
    try
    {
        RenameInCreateQueryTransformVisitor::Data data{renaming_config, context};
        RenameInCreateQueryTransformVisitor{data}.visit(new_ast);
        return new_ast;
    }
    catch (...)
    {
        tryLogCurrentException("Backup", "Error while renaming in AST");
        return ast;
    }
}

}
