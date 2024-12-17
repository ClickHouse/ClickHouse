#include <Databases/DDLDependencyVisitor.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Databases/removeWhereConditionPlaceholder.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/misc.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getClusterName.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/KnownObjectNames.h>
#include <Core/Settings.h>
#include <Poco/String.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
}

namespace
{
    /// Data for DDLDependencyVisitor.
    /// Used to visits ASTCreateQuery and extracts the names of all tables explicitly referenced in the create query.
    class DDLDependencyVisitorData
    {
        friend void tryVisitNestedSelect(const String & query, DDLDependencyVisitorData & data);
    public:
        DDLDependencyVisitorData(const ContextPtr & global_context_, const QualifiedTableName & table_name_, const ASTPtr & ast_, const String & current_database_)
            : create_query(ast_), table_name(table_name_), default_database(global_context_->getCurrentDatabase()), current_database(current_database_), global_context(global_context_)
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
        String default_database;
        String current_database;
        ContextPtr global_context;
        TableNamesSet dependencies;

        /// CREATE TABLE or CREATE DICTIONARY or CREATE VIEW or CREATE TEMPORARY TABLE or CREATE DATABASE query.
        void visitCreateQuery(const ASTCreateQuery & create)
        {
            if (create.targets)
            {
                for (const auto & target : create.targets->targets)
                {
                    const auto & table_id = target.table_id;
                    if (!table_id.table_name.empty())
                    {
                        /// TO target_table (for materialized views)
                        QualifiedTableName target_name{table_id.database_name, table_id.table_name};
                        if (target_name.database.empty())
                            target_name.database = current_database;
                        dependencies.emplace(target_name);
                    }
                }
            }

            QualifiedTableName as_table{create.as_database, create.as_table};
            if (!as_table.table.empty())
            {
                /// AS table_name
                if (as_table.database.empty())
                    as_table.database = current_database;
                dependencies.emplace(as_table);
            }

            /// Visit nested select query only for views, for other cases it's not
            /// an actual dependency as it will be executed only once to fill the table.
            if (create.select && !create.isView())
                skip_asts.insert(create.select);
        }

        /// The definition of a dictionary: SOURCE(CLICKHOUSE(...)) LAYOUT(...) LIFETIME(...)
        void visitDictionaryDef(const ASTDictionary & dictionary)
        {
            if (!dictionary.source || dictionary.source->name != "clickhouse" || !dictionary.source->elements)
                return;

            auto config = getDictionaryConfigurationFromAST(create_query->as<ASTCreateQuery &>(), global_context);
            auto info = getInfoIfClickHouseDictionarySource(config, global_context);

            /// We consider only dependencies on local tables.
            if (!info || !info->is_local)
                return;

            if (!info->table_name.table.empty())
            {
                /// If database is not specified in dictionary source, use database of the dictionary itself, not the current/default database.
                if (info->table_name.database.empty())
                    info->table_name.database = table_name.database;
                dependencies.emplace(std::move(info->table_name));
            }
            else
            {
                /// We don't have a table name, we have a select query instead.
                /// All tables from select query in dictionary definition won't
                /// use current database, as this query is executed with global context.
                /// Use default database from global context while visiting select query.
                String current_database_ = current_database;
                current_database = default_database;
                tryVisitNestedSelect(info->query, *this);
                current_database = current_database_;
            }
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

            /// Distributed(cluster_name, db_name, table_name, ...)
            if (table_engine.name == "Distributed")
                visitDistributedTableEngine(table_engine);
        }

        /// Distributed(cluster_name, database_name, table_name, ...)
        void visitDistributedTableEngine(const ASTFunction & table_engine)
        {
            /// We consider only dependencies on local tables.
            bool has_local_replicas = false;

            if (auto cluster_name = tryGetClusterNameFromArgument(table_engine, 0))
            {
                auto cluster = global_context->tryGetCluster(*cluster_name);
                if (cluster && cluster->getLocalShardCount())
                    has_local_replicas = true;
            }

            if (has_local_replicas)
                addDatabaseAndTableNameFromArguments(table_engine, 1, 2);
        }

        /// Finds dependencies of a function.
        void visitFunction(const ASTFunction & function)
        {
            if (functionIsJoinGet(function.name) || functionIsDictGet(function.name))
            {
                /// dictGet('dict_name', attr_names, id_expr)
                /// dictHas('dict_name', id_expr)
                /// joinGet(join_storage_table_name, `value_column`, join_keys)
                addQualifiedNameFromArgument(function, 0);
            }
            else if (functionIsInOrGlobalInOperator(function.name))
            {
                /// x IN table_name.
                /// We set evaluate=false here because we don't want to evaluate a subquery in "x IN subquery".
                addQualifiedNameFromArgument(function, 1, /* evaluate= */ false);
            }
            else if (function.name == "dictionary")
            {
                /// dictionary(dict_name)
                addQualifiedNameFromArgument(function, 0);
            }
            else if (function.name == "remote" || function.name == "remoteSecure")
            {
                visitRemoteFunction(function, /* is_cluster_function= */ false);
            }
            else if (function.name == "cluster" || function.name == "clusterAllReplicas")
            {
                visitRemoteFunction(function, /* is_cluster_function= */ true);
            }
        }

        /// remote('addresses_expr', db_name.table_name, ...)
        /// remote('addresses_expr', 'db_name', 'table_name', ...)
        /// remote('addresses_expr', table_function(), ...)
        /// cluster('cluster_name', db_name.table_name, ...)
        /// cluster('cluster_name', 'db_name', 'table_name', ...)
        /// cluster('cluster_name', table_function(), ...)
        void visitRemoteFunction(const ASTFunction & function, bool is_cluster_function)
        {
            /// We consider dependencies on local tables only.
            bool has_local_replicas = false;

            if (is_cluster_function)
            {
                if (auto cluster_name = tryGetClusterNameFromArgument(function, 0))
                {
                    if (auto cluster = global_context->tryGetCluster(*cluster_name))
                    {
                        if (cluster->getLocalShardCount())
                            has_local_replicas = true;
                    }
                }
            }
            else
            {
                /// remote() and remoteSecure() are not fully supported. To properly support them we would need to check the first
                /// argument to decide whether the host & port pattern specified in the first argument contains the local host or not
                /// which is not trivial. For now we just always assume that the host & port pattern doesn't contain the local host.
            }

            if (!function.arguments)
                return;

            ASTs & args = function.arguments->children;
            if (args.size() < 2)
                return;

            const ASTFunction * table_function = nullptr;
            if (const auto * second_arg_as_function = args[1]->as<ASTFunction>();
                second_arg_as_function && KnownTableFunctionNames::instance().exists(second_arg_as_function->name))
            {
                table_function = second_arg_as_function;
            }

            if (has_local_replicas && !table_function)
            {
                /// We set `apply_current_database=false` here because if this argument is an identifier without dot,
                /// then it's not the name of a table within the current database, it's the name of a database, and
                /// the name of a table will be in the following argument.
                auto maybe_qualified_name = tryGetQualifiedNameFromArgument(function, 1, /* evaluate= */ true, /* apply_current_database= */ false);
                if (!maybe_qualified_name)
                    return;
                auto & qualified_name = *maybe_qualified_name;
                if (qualified_name.database.empty())
                {
                    auto table = tryGetStringFromArgument(function, 2);
                    if (!table)
                        return;
                    qualified_name.database = std::move(qualified_name.table);
                    qualified_name.table = std::move(table).value();
                }
                dependencies.insert(qualified_name);
            }

            if (!has_local_replicas && table_function)
            {
                /// `table function` will be executed remotely, so we won't check it or its arguments for dependencies.
                skip_asts.emplace(table_function);
            }
        }

        /// Gets an argument as a string, evaluates constants if necessary.
        std::optional<String> tryGetStringFromArgument(const ASTFunction & function, size_t arg_idx, bool evaluate = true) const
        {
            if (!function.arguments)
                return {};

            const ASTs & args = function.arguments->children;
            if (arg_idx >= args.size())
                return {};

            const auto & arg = args[arg_idx];

            if (evaluate)
            {
                try
                {
                    /// We're just searching for dependencies here, it's not safe to execute subqueries now.
                    /// Use copy of the global_context and set current database, because expressions can contain currentDatabase() function.
                    ContextMutablePtr global_context_copy = Context::createCopy(global_context);
                    global_context_copy->setCurrentDatabase(current_database);
                    auto evaluated = evaluateConstantExpressionOrIdentifierAsLiteral(arg, global_context_copy);
                    const auto * literal = evaluated->as<ASTLiteral>();
                    if (!literal || (literal->value.getType() != Field::Types::String))
                        return {};
                    return literal->value.safeGet<String>();
                }
                catch (...)
                {
                    return {};
                }
            }
            else
            {
                if (const auto * id = arg->as<ASTIdentifier>())
                    return id->name();
                if (const auto * literal = arg->as<ASTLiteral>())
                {
                    if (literal->value.getType() == Field::Types::String)
                        return literal->value.safeGet<String>();
                }
                return {};
            }
        }

        /// Gets an argument as a qualified table name.
        /// Accepts forms db_name.table_name (as an identifier) and 'db_name.table_name' (as a string).
        /// The function doesn't replace an empty database name with the current_database (the caller must do that).
        std::optional<QualifiedTableName> tryGetQualifiedNameFromArgument(
            const ASTFunction & function, size_t arg_idx, bool evaluate = true, bool apply_current_database = true) const
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
                auto qualified_name_as_string = tryGetStringFromArgument(function, arg_idx, evaluate);
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
        void addQualifiedNameFromArgument(const ASTFunction & function, size_t arg_idx, bool evaluate = true)
        {
            if (auto qualified_name = tryGetQualifiedNameFromArgument(function, arg_idx, evaluate))
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
            if (!table || table->empty())
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

        std::optional<String> tryGetClusterNameFromArgument(const ASTFunction & function, size_t arg_idx) const
        {
            if (!function.arguments)
                return {};

            ASTs & args = function.arguments->children;
            if (arg_idx >= args.size())
                return {};

            auto cluster_name = ::DB::tryGetClusterName(*args[arg_idx]);
            if (cluster_name)
                return cluster_name;

            return tryGetStringFromArgument(function, arg_idx);
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

    void tryVisitNestedSelect(const String & query, DDLDependencyVisitorData & data)
    {
        try
        {
            ParserSelectWithUnionQuery parser;
            String description = fmt::format("Query for ClickHouse dictionary {}", data.table_name);
            String fixed_query = removeWhereConditionPlaceholder(query);
            const Settings & settings = data.global_context->getSettingsRef();
            ASTPtr select = parseQuery(
                parser, fixed_query, description, settings[Setting::max_query_size], settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);

            DDLDependencyVisitor::Visitor visitor{data};
            visitor.visit(select);
        }
        catch (...)
        {
            tryLogCurrentException("DDLDependencyVisitor");
        }
    }
}


TableNamesSet getDependenciesFromCreateQuery(const ContextPtr & global_global_context, const QualifiedTableName & table_name, const ASTPtr & ast, const String & current_database)
{
    DDLDependencyVisitor::Data data{global_global_context, table_name, ast, current_database};
    DDLDependencyVisitor::Visitor visitor{data};
    visitor.visit(ast);
    return std::move(data).getDependencies();
}

TableNamesSet getDependenciesFromDictionaryNestedSelectQuery(const ContextPtr & global_context, const QualifiedTableName & table_name, const ASTPtr & ast, const String & select_query, const String & current_database)
{
    DDLDependencyVisitor::Data data{global_context, table_name, ast, current_database};
    tryVisitNestedSelect(select_query, data);
    return std::move(data).getDependencies();
}

}
