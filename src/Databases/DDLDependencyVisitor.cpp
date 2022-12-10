#include <Databases/DDLDependencyVisitor.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
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
    /// Data for DDLDependencyVisitor.
    /// Used to visits ASTCreateQuery and extracts the names of all tables explicitly referenced in the create query.
    class DDLDependencyVisitorData
    {
    public:
        DDLDependencyVisitorData(const ContextPtr & context_, const QualifiedTableName & table_name_, const ASTPtr & ast_)
            : create_query(ast_), table_name(table_name_), current_database(context_->getCurrentDatabase()), context(context_)
        {
        }

        /// Acquire the result of visiting the create query.
        TableNamesSet getDependencies() &&
        {
            dependencies.erase(table_name);
            return std::move(dependencies);
        }

        bool needChildVisit(const ASTPtr & child) const { return !skip_asts.contains(child.get()); }

        void visit(const ASTPtr & ast)
        {
            if (auto * create = ast->as<ASTCreateQuery>())
            {
                visitCreateQuery(*create);
            }
            else if (auto * dictionary = ast->as<ASTDictionary>())
            {
                visitDictionaryDef(*dictionary);
            }
            else if (auto * expr = ast->as<ASTTableExpression>())
            {
                visitTableExpression(*expr);
            }
            else if (const auto * function = ast->as<ASTFunction>())
            {
                if (function->kind == ASTFunction::Kind::TABLE_ENGINE)
                    visitTableEngine(*function);
                else
                    visitFunction(*function);
            }
        }

    private:
        ASTPtr create_query;
        std::unordered_set<const IAST *> skip_asts;
        QualifiedTableName table_name;
        String current_database;
        ContextPtr context;
        TableNamesSet dependencies;

        /// CREATE TABLE or CREATE DICTIONARY or CREATE VIEW or CREATE TEMPORARY TABLE or CREATE DATABASE query.
        void visitCreateQuery(const ASTCreateQuery & create)
        {
            QualifiedTableName to_table{create.to_table_id.database_name, create.to_table_id.table_name};
            if (!to_table.table.empty())
            {
                /// TO target_table (for materialized views)
                if (to_table.database.empty())
                    to_table.database = current_database;
                dependencies.emplace(to_table);
            }

            QualifiedTableName as_table{create.as_database, create.as_table};
            if (!as_table.table.empty())
            {
                /// AS table_name
                if (as_table.database.empty())
                    as_table.database = current_database;
                dependencies.emplace(as_table);
            }
        }

        /// The definition of a dictionary: SOURCE(CLICKHOUSE(...)) LAYOUT(...) LIFETIME(...)
        void visitDictionaryDef(const ASTDictionary & dictionary)
        {
            if (!dictionary.source || dictionary.source->name != "clickhouse" || !dictionary.source->elements)
                return;

            auto config = getDictionaryConfigurationFromAST(create_query->as<ASTCreateQuery &>(), context);
            auto info = getInfoIfClickHouseDictionarySource(config, context);

            /// We consider only dependencies on local tables.
            if (!info || !info->is_local)
                return;

            if (info->table_name.database.empty())
                info->table_name.database = current_database;
            dependencies.emplace(std::move(info->table_name));
        }

        /// ASTTableExpression represents a reference to a table in SELECT query.
        /// DDLDependencyVisitor should handle ASTTableExpression because some CREATE queries can contain SELECT queries after AS
        /// (for example, CREATE VIEW).
        void visitTableExpression(const ASTTableExpression & expr)
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
                qualified_name.database = current_database;
            }

            dependencies.emplace(qualified_name);
        }

        /// Finds dependencies of a table engine.
        void visitTableEngine(const ASTFunction & table_engine)
        {
            /// Dictionary(db_name.dictionary_name)
            if (table_engine.name == "Dictionary")
                addQualifiedNameFromArgument(table_engine, 0);

            /// Buffer('db_name', 'dest_table_name')
            if (table_engine.name == "Buffer")
                addDatabaseAndTableNameFromArguments(table_engine, 0, 1);
        }

        /// Finds dependencies of a function.
        void visitFunction(const ASTFunction & function)
        {
            if (function.name == "joinGet" || function.name == "dictHas" || function.name == "dictIsIn" || function.name.starts_with("dictGet"))
            {
                /// dictGet('dict_name', attr_names, id_expr)
                /// dictHas('dict_name', id_expr)
                /// joinGet(join_storage_table_name, `value_column`, join_keys)
                addQualifiedNameFromArgument(function, 0);
            }
            else if (function.name == "in" || function.name == "notIn" || function.name == "globalIn" || function.name == "globalNotIn")
            {
                /// in(x, table_name) - function for evaluating (x IN table_name)
                addQualifiedNameFromArgument(function, 1);
            }
            else if (function.name == "dictionary")
            {
                /// dictionary(dict_name)
                addQualifiedNameFromArgument(function, 0);
            }
        }

        /// Gets an argument as a string, evaluates constants if necessary.
        std::optional<String> tryGetStringFromArgument(const ASTFunction & function, size_t arg_idx) const
        {
            if (!function.arguments)
                return {};

            const ASTs & args = function.arguments->children;
            if (arg_idx >= args.size())
                return {};

            const auto & arg = args[arg_idx];

            if (const auto * literal = arg->as<ASTLiteral>())
            {
                if (literal->value.getType() != Field::Types::String))
                    return {};
                return literal->value.safeGet<String>();
            }
            else if (const auto * identifier = dynamic_cast<const ASTIdentifier *>(arg.get()))
            {
                return identifier->name();
            }
            else
            {
                return nullptr;
            }
        }

        /// Gets an argument as a qualified table name.
        /// Accepts forms db_name.table_name (as an identifier) and 'db_name.table_name' (as a string).
        /// The function doesn't replace an empty database name with the current_database (the caller must do that).
        std::optional<QualifiedTableName>
        tryGetQualifiedNameFromArgument(const ASTFunction & function, size_t arg_idx, bool apply_current_database = true) const
        {
            if (!function.arguments)
                return {};

            const ASTs & args = function.arguments->children;
            if (arg_idx >= args.size())
                return {};

            const auto & arg = args[arg_idx];
            QualifiedTableName qualified_name;

            if (const auto * identifier = dynamic_cast<const ASTIdentifier *>(arg.get()))
            {
                /// ASTIdentifier or ASTTableIdentifier
                auto table_identifier = identifier->createTable();
                if (!table_identifier)
                    return {};

                qualified_name.database = table_identifier->getDatabaseName();
                qualified_name.table = table_identifier->shortName();
            }
            else
            {
                auto qualified_name_as_string = tryGetStringFromArgument(function, arg_idx);
                if (!qualified_name_as_string)
                    return {};

                auto maybe_qualified_name = QualifiedTableName::tryParseFromString(*qualified_name_as_string);
                if (!maybe_qualified_name)
                    return {};

                qualified_name = std::move(maybe_qualified_name).value();
            }

            if (qualified_name.database.empty() && apply_current_database)
                qualified_name.database = current_database;

            return qualified_name;
        }

        /// Adds a qualified table name from an argument to the collection of dependencies.
        /// Accepts forms db_name.table_name (as an identifier) and 'db_name.table_name' (as a string).
        void addQualifiedNameFromArgument(const ASTFunction & function, size_t arg_idx)
        {
            if (auto qualified_name = tryGetQualifiedNameFromArgument(function, arg_idx))
                dependencies.emplace(std::move(qualified_name).value());
        }

        /// Returns a database name and a table name extracted from two separate arguments.
        std::optional<QualifiedTableName> tryGetDatabaseAndTableNameFromArguments(
            const ASTFunction & function, size_t database_arg_idx, size_t table_arg_idx, bool apply_current_database = true) const
        {
            auto database = tryGetStringFromArgument(function, database_arg_idx);
            if (!database)
                return {};

            auto table = tryGetStringFromArgument(function, table_arg_idx);
            if (!table)
                return {};

            QualifiedTableName qualified_name;
            qualified_name.database = std::move(database).value();
            qualified_name.table = std::move(table).value();

            if (qualified_name.database.empty() && apply_current_database)
                qualified_name.database = current_database;

            return qualified_name;
        }

        /// Adds a database name and a table name from two separate arguments to the collection of dependencies.
        void addDatabaseAndTableNameFromArguments(const ASTFunction & function, size_t database_arg_idx, size_t table_arg_idx)
        {
            if (auto qualified_name = tryGetDatabaseAndTableNameFromArguments(function, database_arg_idx, table_arg_idx))
                dependencies.emplace(std::move(qualified_name).value());
        }
    };

    /// Visits ASTCreateQuery and extracts the names of all tables explicitly referenced in the create query.
    class DDLDependencyVisitor
    {
    public:
        using Data = DDLDependencyVisitorData;
        using Visitor = ConstInDepthNodeVisitor<DDLDependencyVisitor, /* top_to_bottom= */ true, /* need_child_accept_data= */ true>;

        static bool needChildVisit(const ASTPtr &, const ASTPtr & child, const Data & data) { return data.needChildVisit(child); }
        static void visit(const ASTPtr & ast, Data & data) { data.visit(ast); }
    };
}


TableNamesSet getDependenciesFromCreateQuery(const ContextPtr & context, const QualifiedTableName & table_name, const ASTPtr & ast)
{
    DDLDependencyVisitor::Data data{context, table_name, ast};
    DDLDependencyVisitor::Visitor visitor{data};
    visitor.visit(ast);
    return std::move(data).getDependencies();
}

}
