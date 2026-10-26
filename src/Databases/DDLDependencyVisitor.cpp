#include <Poco/Util/AbstractConfiguration.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Analyzer/Identifier.h>
#include <Databases/removeWhereConditionPlaceholder.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/misc.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getClusterName.h>
#include <Storages/StorageMaterializedView.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTViewTargets.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/KnownObjectNames.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Poco/String.h>
#include <base/scope_guard.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool enable_global_with_statement;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
}

namespace
{
    /// The value of `enable_global_with_statement` in the scope of the `SETTINGS` clause `settings_ast`, given its value outside.
    bool applyEnableGlobalWithStatement(const ASTPtr & settings_ast, bool enabled)
    {
        if (!settings_ast)
            return enabled;

        const auto & set_query = settings_ast->as<const ASTSetQuery &>();
        if (const auto * value = set_query.changes.tryGet("enable_global_with_statement"))
            enabled = Settings::castValueUtil("enable_global_with_statement", *value).safeGet<bool>();
        if (std::ranges::contains(set_query.default_settings, "enable_global_with_statement"))
            enabled = Settings{}[Setting::enable_global_with_statement];
        return enabled;
    }

    /// Like in `ApplyWithSubqueryVisitor`, a trailing `SETTINGS` clause of a set operation with several operands
    /// applies to every operand, not only to the last one, where `ParserSelectQuery` leaves it.
    bool applySetOperationEnableGlobalWithStatement(const ASTs & operands, const ASTPtr & settings_ast, bool enabled)
    {
        enabled = applyEnableGlobalWithStatement(settings_ast, enabled);
        if (operands.size() > 1)
        {
            if (const auto * last_select = operands.back()->as<ASTSelectQuery>())
                enabled = applyEnableGlobalWithStatement(last_select->settings(), enabled);
        }
        return enabled;
    }

    /// Data for DDLDependencyVisitor.
    /// Used to visits ASTCreateQuery and extracts the names of all tables explicitly referenced in the create query.
    class DDLDependencyVisitorData
    {
        friend void tryVisitNestedSelect(const String & query, DDLDependencyVisitorData & data);
    public:
        DDLDependencyVisitorData(const ContextPtr & global_context_, const QualifiedTableName & table_name_, const ASTPtr & ast_, const String & current_database_, bool can_throw_, bool validate_current_database_, size_t max_expanded_ast_elements_, const String & engine_arguments_database_)
            : create_query(ast_), table_name(table_name_), default_database(global_context_->getCurrentDatabase()), current_database(current_database_), engine_arguments_database(engine_arguments_database_), global_context(global_context_), can_throw(can_throw_), validate_current_database(validate_current_database_), max_expanded_ast_elements(max_expanded_ast_elements_), enable_global_with_statement(global_context_->getSettingsRef()[Setting::enable_global_with_statement])
        {
        }

        /// Acquire the result of visiting the create query.
        TableNamesSet getDependencies()
        {
            dependencies.erase(table_name);
            return dependencies;
        }
        std::optional<StorageID> getMvToDependency()
        {
            return mv_to_dependency;
        }
        std::optional<StorageID> getMvFromDependency()
        {
            return mv_from_dependency;
        }

        TableNamesSet getPlainViewDependencies()
        {
            if (!is_plain_view)
                return {};

            /// Every table the definition of the view refers to, not only its table expressions: a table named
            /// in `x IN table`, `joinGet` or `dictGet` blocks `DROP` through the referential graph just the same,
            /// so it lists the view among its dependents as well.
            return getDependencies();
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
                if (function->getKind() == ASTFunction::Kind::TABLE_ENGINE)
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
        String engine_arguments_database;
        ContextPtr global_context;
        TableNamesSet dependencies;
        bool can_throw;
        bool validate_current_database;
        bool is_plain_view = false;
        size_t max_expanded_ast_elements;
        std::optional<StorageID> mv_to_dependency;
        std::optional<StorageID> mv_from_dependency;

        struct WithDeclaration
        {
            /// Whether a bare name that refers to the declaration is not a table.
            bool hides_table = false;
            bool is_recursive = false;
        };

        /// The declarations of the common table expressions of the enclosing select queries by name, the nearest last.
        std::unordered_map<String, std::vector<WithDeclaration>> with_declarations;
        /// The value of `enable_global_with_statement` in the current select query.
        bool enable_global_with_statement;
        /// The aliases of the expressions of the `WITH` clauses of the enclosing select queries, visible in the nested select queries.
        std::unordered_set<String> with_expression_aliases;
        /// The aliases of the expressions of the current select query.
        std::unordered_set<String> expression_aliases;
        /// The aliases of the expressions of the current select query which enclose the visited node. Like the analyzer,
        /// a name does not refer to the alias of an expression while that expression is resolved, e.g. `1 IN ids AS ids`.
        std::vector<String> enclosing_aliases;
        /// The table names which refer to a common table expression or to an alias of an expression rather than to a table.
        std::unordered_set<const IAST *> non_table_references;

        /// CREATE TABLE or CREATE DICTIONARY or CREATE VIEW or CREATE TEMPORARY TABLE or CREATE DATABASE query.
        void visitCreateQuery(const ASTCreateQuery & create)
        {
            if (create.targets)
            {
                for (const auto & target : create.targets->targets)
                {
                    if (target.kind == ViewTarget::Kind::To)
                    {
                        const auto & table_id = target.table_id;
                        if (!table_id.table_name.empty())
                        {
                            mv_to_dependency = table_id;
                            if (mv_to_dependency->database_name.empty())
                                mv_to_dependency->database_name = current_database;
                            dependencies.emplace(mv_to_dependency->getQualifiedName());
                        }
                        else
                        {
                            mv_to_dependency = StorageID{table_name.database, table_name.table, create.uuid};
                            mv_to_dependency->table_name = StorageMaterializedView::generateInnerTableName(mv_to_dependency.value());
                            mv_to_dependency->uuid = target.inner_uuid;
                        }
                    }
                    else if (target.kind == ViewTarget::Kind::Inner)
                    {
                        mv_to_dependency = StorageID{table_name.database, target.table_id.getQualifiedName().table, target.inner_uuid};
                        mv_to_dependency->table_name = StorageMaterializedView::generateInnerTableName(mv_to_dependency.value());
                    }
                    else if (target.kind == ViewTarget::Kind::Samples
                        || target.kind == ViewTarget::Kind::RecentSamples
                        || target.kind == ViewTarget::Kind::Tags
                        || target.kind == ViewTarget::Kind::TimeRanges
                        || target.kind == ViewTarget::Kind::MetricFamilies)
                    {
                        /// External target tables of a TimeSeries table are referential dependencies.
                        /// Inner target tables (created and owned by the TimeSeries table) are not, the same way
                        /// the inner "TO" table of a materialized view is not registered as a dependency.
                        const auto & table_id = target.table_id;
                        if (!table_id.table_name.empty())
                        {
                            QualifiedTableName target_name{table_id.database_name, table_id.table_name};
                            if (target_name.database.empty())
                                target_name.database = current_database;
                            dependencies.emplace(std::move(target_name));
                        }
                    }

                    if (mv_to_dependency && mv_to_dependency->database_name.empty())
                        mv_to_dependency->database_name = current_database;
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
            if (create.select)
            {
                if (create.isView())
                {
                    if (create.is_materialized_view)
                    {
                        auto select_copy = create.select->clone();
                        /// The limit comes from the context of the query creating the view. A definition loaded
                        /// from metadata was bounded when it was created, so it is not limited here.
                        ApplyWithSubqueryVisitor::visit(select_copy, max_expanded_ast_elements);

                        /// Use the database where the materialized view is created to resolve nested views.
                        /// The database name can be empty when the AST has been mutated by SharedDatabaseCatalog::serializeCreateQuery
                        /// (which strips the database before serialization). In that case, keep the global context's current database.
                        ContextMutablePtr mv_db_context = Context::createCopy(global_context);
                        if (!table_name.database.empty())
                        {
                            /// During bootstrap/restore scenarios, the database may not exist yet, so we provide a way to skip this validation
                            if (validate_current_database)
                                mv_db_context->setCurrentDatabase(table_name.database);
                            else
                                mv_db_context->setCurrentDatabaseUnchecked(table_name.database);
                        }
                        auto select_query = SelectQueryDescription::getSelectQueryFromASTForMatView(select_copy, create.refresh_strategy != nullptr /*refresheable*/, mv_db_context);
                        if (!select_query.select_table_id.empty())
                        {
                            mv_from_dependency = select_query.select_table_id;

                            /// Keepeing UUID is problematic
                            mv_from_dependency->uuid = UUIDHelpers::Nil;
                        }
                    }
                    else if (create.is_ordinary_view)
                        is_plain_view = true;
                }
                else
                    skip_asts.insert(create.select);
            }

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
            if (expr.table_function)
                visitParameterizedViewCall(*expr.table_function);

            if (!expr.database_and_table_name)
                return;

            const ASTIdentifier * identifier = dynamic_cast<const ASTIdentifier *>(expr.database_and_table_name.get());
            if (!identifier || non_table_references.contains(identifier))
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

        /// Like in the analyzer, a table function which is not registered is a call of a parameterized view,
        /// e.g. `pv(id = 1)` or `db.pv(id = 1)`.
        void visitParameterizedViewCall(const IAST & table_function)
        {
            const auto * function = table_function.as<ASTFunction>();
            if (!function || TableFunctionFactory::instance().isTableFunctionName(function->name))
                return;

            Identifier identifier{function->name};
            if (identifier.getPartsSize() == 1)
                dependencies.emplace(QualifiedTableName{current_database, identifier[0]});
            else if (identifier.getPartsSize() == 2)
                dependencies.emplace(QualifiedTableName{identifier[0], identifier[1]});
        }

    public:
        /// The references of a recursive common table expression to itself stay bare names in the stored definition,
        /// in its `WITH RECURSIVE` list and in the copies of its body `ApplyWithSubqueryVisitor` substitutes for it.
        /// Collects them with the scoping `AddDefaultDatabaseVisitor` leaves them unqualified with. Like the analyzer,
        /// a select query in which `enable_global_with_statement` is disabled sees only its own common table expressions.
        /// Also collects the right arguments of `IN` which are aliases of expressions, which `AddDefaultDatabaseVisitor`
        /// leaves unqualified with the same scoping, except the aliases of the enclosing expressions.
        /// Runs on the whole query before it is visited, so that the `SETTINGS` clauses of the enclosing select queries are known.
        void collectNonTableReferences(const ASTPtr & ast)
        {
            if (const auto * select = ast->as<ASTSelectQuery>())
            {
                auto enclosing_with_declarations = with_declarations;
                auto enclosing_with_expression_aliases = with_expression_aliases;
                auto enclosing_expression_aliases = std::move(expression_aliases);
                auto outer_enclosing_aliases = std::move(enclosing_aliases);
                enclosing_aliases.clear();
                const bool enclosing_enable_global_with_statement = enable_global_with_statement;
                enable_global_with_statement = applyEnableGlobalWithStatement(select->settings(), enable_global_with_statement);
                if (!enable_global_with_statement)
                    with_declarations.clear();

                expression_aliases.clear();
                for (const auto & child : select->children)
                    AddDefaultDatabaseVisitor::collectAliases(child, expression_aliases);

                if (select->with())
                {
                    for (const auto & child : select->with()->children)
                    {
                        if (const auto * with_element = child->as<ASTWithElement>())
                            with_declarations[with_element->name].push_back(
                            {
                                .hides_table = select->recursive_with,
                                .is_recursive = select->recursive_with && AddDefaultDatabaseVisitor::getRecursiveBodyBranches(with_element->subquery),
                            });
                        else
                            AddDefaultDatabaseVisitor::collectWithExpressionAliases(*child, with_expression_aliases);
                    }
                }

                for (const auto & child : select->children)
                {
                    if (child.get() != select->with().get())
                    {
                        collectNonTableReferences(child);
                        continue;
                    }

                    for (const auto & element : child->children)
                    {
                        if (const auto * with_element = element->as<ASTWithElement>())
                            collectNonTableReferencesInBody(with_element->name, with_element->subquery, element);
                        else
                            collectNonTableReferences(element);
                    }
                }

                with_declarations = std::move(enclosing_with_declarations);
                with_expression_aliases = std::move(enclosing_with_expression_aliases);
                expression_aliases = std::move(enclosing_expression_aliases);
                enclosing_aliases = std::move(outer_enclosing_aliases);
                enable_global_with_statement = enclosing_enable_global_with_statement;
                return;
            }

            if (const auto * union_query = ast->as<ASTSelectWithUnionQuery>(); union_query && union_query->list_of_selects)
            {
                collectNonTableReferencesInOperands(union_query->list_of_selects->children, union_query->settings_ast);
                return;
            }

            if (const auto * intersect_except = ast->as<ASTSelectIntersectExceptQuery>())
            {
                collectNonTableReferencesInOperands(intersect_except->getListOfSelects(), nullptr);
                return;
            }

            if (const auto * subquery = ast->as<ASTSubquery>(); subquery && !subquery->cte_name.empty())
            {
                auto it = with_declarations.find(subquery->cte_name);
                if (it != with_declarations.end() && !it->second.empty())
                {
                    for (const auto & child : subquery->children)
                        collectNonTableReferencesInBody(subquery->cte_name, ast, child);
                    return;
                }
            }

            if (const auto * table_expression = ast->as<ASTTableExpression>(); table_expression && table_expression->database_and_table_name)
                addCteReference(*table_expression->database_and_table_name);

            const String alias = ast->tryGetAlias();
            if (!alias.empty())
                enclosing_aliases.push_back(alias);

            if (const auto * function = ast->as<ASTFunction>();
                function && functionIsInOrGlobalInOperator(function->name) && function->arguments && function->arguments->children.size() > 1)
            {
                addCteReference(*function->arguments->children[1]);
                addExpressionAliasReference(*function->arguments->children[1]);
            }

            if (const auto * function = ast->as<ASTFunction>(); function && function->name == "lambda")
            {
                auto enclosing_expression_aliases = expression_aliases;
                AddDefaultDatabaseVisitor::collectLambdaAliases(*function, expression_aliases);
                for (const auto & child : ast->children)
                    collectNonTableReferences(child);
                expression_aliases = std::move(enclosing_expression_aliases);
            }
            else
            {
                for (const auto & child : ast->children)
                    collectNonTableReferences(child);
            }

            if (!alias.empty())
                enclosing_aliases.pop_back();
        }

    private:
        /// Only the recursive members of a recursive common table expression reference it. Inside the body of an ordinary one,
        /// and inside the seed of a recursive one, its name is a table, unless an enclosing select query declares the same name.
        void collectNonTableReferencesInBody(const String & name, const ASTPtr & subquery, const ASTPtr & node)
        {
            auto & declarations = with_declarations[name];
            const WithDeclaration declaration = declarations.back();
            const ASTs * branches = declaration.is_recursive ? AddDefaultDatabaseVisitor::getRecursiveBodyBranches(subquery) : nullptr;

            /// The branches are the operands of the set operation of the body.
            const bool enclosing_enable_global_with_statement = enable_global_with_statement;
            if (branches)
                enable_global_with_statement = applySetOperationEnableGlobalWithStatement(*branches, nullptr, enable_global_with_statement);

            declarations.pop_back();
            collectNonTableReferences(branches ? branches->front() : node);
            with_declarations[name].push_back(declaration);

            if (branches)
            {
                for (size_t i = 1; i < branches->size(); ++i)
                    collectNonTableReferences((*branches)[i]);
            }

            enable_global_with_statement = enclosing_enable_global_with_statement;
        }

        void collectNonTableReferencesInOperands(const ASTs & operands, const ASTPtr & settings_ast)
        {
            const bool enclosing_enable_global_with_statement = enable_global_with_statement;
            enable_global_with_statement = applySetOperationEnableGlobalWithStatement(operands, settings_ast, enable_global_with_statement);

            for (const auto & operand : operands)
                collectNonTableReferences(operand);

            enable_global_with_statement = enclosing_enable_global_with_statement;
        }

        void addCteReference(const IAST & table_name_ast)
        {
            const auto * identifier = dynamic_cast<const ASTIdentifier *>(&table_name_ast);
            if (!identifier || identifier->compound())
                return;

            auto it = with_declarations.find(identifier->name());
            if (it != with_declarations.end() && !it->second.empty() && it->second.back().hides_table)
                non_table_references.insert(identifier);
        }

        void addExpressionAliasReference(const IAST & table_name_ast)
        {
            const auto * identifier = dynamic_cast<const ASTIdentifier *>(&table_name_ast);
            if (!identifier || identifier->compound())
                return;

            const String & name = identifier->name();
            if (std::ranges::contains(enclosing_aliases, name))
                return;

            if (expression_aliases.contains(name) || with_expression_aliases.contains(name))
                non_table_references.insert(identifier);
        }

        /// Finds dependencies of a table engine.
        void visitTableEngine(const ASTFunction & table_engine)
        {
            /// The arguments of the engine are resolved against the database the storage factory evaluates them with.
            String current_database_ = current_database;
            current_database = engine_arguments_database;
            SCOPE_EXIT({ current_database = current_database_; });

            /// Dictionary(db_name.dictionary_name)
            if (table_engine.name == "Dictionary")
                addQualifiedNameFromArgument(table_engine, 0);

            /// Buffer('db_name', 'dest_table_name')
            if (table_engine.name == "Buffer")
                addDatabaseAndTableNameFromArguments(table_engine, 0, 1);

            /// Distributed(cluster_name, db_name, table_name, ...)
            if (table_engine.name == "Distributed")
                visitDistributedTableEngine(table_engine);

            /// Alias(table_name) or Alias(db_name, table_name)
            /// Note: Alias resolves non-qualified target names to its own database (not current_database),
            /// so we use addQualifiedNameFromArgumentUsingTableDatabase for the single-argument case.
            if (table_engine.name == "Alias" && table_engine.arguments)
            {
                if (table_engine.arguments->children.size() == 1)
                    addQualifiedNameFromArgumentUsingTableDatabase(table_engine, 0);
                else
                    addDatabaseAndTableNameFromArguments(table_engine, 0, 1);
            }
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
                /// x IN table_name. A literal, as in x IN ('db.table'), is a set of values, not a table name.
                /// We set evaluate=false here because we don't want to evaluate a subquery in "x IN subquery".
                if (function.arguments && function.arguments->children.size() > 1 && !function.arguments->children[1]->as<ASTLiteral>()
                    && !non_table_references.contains(function.arguments->children[1].get()))
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

            if (const auto * id = arg->as<ASTIdentifier>())
                return id->name();

            if (const auto * literal = arg->as<ASTLiteral>())
            {
                if (literal->value.getType() == Field::Types::String)
                    return literal->value.safeGet<String>();
            }

            if (!evaluate)
                return {};

            try
            {
                /// We're just searching for dependencies here, it's not safe to execute subqueries now.
                /// Use copy of the global_context and set current database, because expressions can contain currentDatabase() function.
                ContextMutablePtr global_context_copy = Context::createCopy(global_context);
                if (!current_database.empty())
                {
                    if (validate_current_database)
                        global_context_copy->setCurrentDatabase(current_database);
                    else
                        global_context_copy->setCurrentDatabaseUnchecked(current_database);
                }
                auto evaluated = evaluateConstantExpressionOrIdentifierAsLiteral(arg, global_context_copy);
                const auto * literal = evaluated->as<ASTLiteral>();
                if (!literal || (literal->value.getType() != Field::Types::String))
                    return {};
                return literal->value.safeGet<String>();
            }
            catch (const Exception &)
            {
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

        /// Like addQualifiedNameFromArgument, but uses the database of the table being created
        /// as the default database (instead of current_database). This matches the behavior of
        /// engines like Alias that resolve non-qualified target names to their own database.
        void addQualifiedNameFromArgumentUsingTableDatabase(const ASTFunction & function, size_t arg_idx, bool evaluate = true)
        {
            if (auto qualified_name = tryGetQualifiedNameFromArgument(function, arg_idx, evaluate, /* apply_current_database= */ false))
            {
                if (qualified_name->database.empty())
                    qualified_name->database = table_name.database;
                dependencies.emplace(std::move(qualified_name).value());
            }
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
            String description = fmt::format("Query for ClickHouse dictionary {}.{}", backQuoteIfNeed(data.table_name.database), backQuoteIfNeed(data.table_name.table));
            String fixed_query = removeWhereConditionPlaceholder(query);
            const Settings & settings = data.global_context->getSettingsRef();
            ASTPtr select = parseQuery(
                parser, fixed_query, description, settings[Setting::max_query_size], settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);

            data.collectNonTableReferences(select);
            DDLDependencyVisitor::Visitor visitor{data};
            visitor.visit(select);
        }
        catch (...)
        {
            if (data.can_throw)
                throw;
            else
                tryLogCurrentException("DDLDependencyVisitor");
        }
    }
}


CreateQueryDependencies getDependenciesFromCreateQuery(const ContextPtr & global_global_context, const QualifiedTableName & table_name, const ASTPtr & ast, const String & current_database, bool can_throw, bool validate_current_database, size_t max_expanded_ast_elements, const std::optional<String> & engine_arguments_database)
{
    DDLDependencyVisitor::Data data{global_global_context, table_name, ast, current_database, can_throw, validate_current_database, max_expanded_ast_elements, engine_arguments_database.value_or(current_database)};
    data.collectNonTableReferences(ast);
    DDLDependencyVisitor::Visitor visitor{data};
    visitor.visit(ast);
    return {data.getDependencies(), data.getMvToDependency(), data.getMvFromDependency(), data.getPlainViewDependencies()};
}

TableNamesSet getDependenciesFromDictionaryNestedSelectQuery(const ContextPtr & global_context, const QualifiedTableName & table_name, const ASTPtr & ast, const String & select_query, const String & current_database, bool can_throw)
{
    DDLDependencyVisitor::Data data{global_context, table_name, ast, current_database, can_throw, /*validate_current_database=*/true, /*max_expanded_ast_elements=*/0, current_database};
    tryVisitNestedSelect(select_query, data);
    return std::move(data).getDependencies();
}

}
