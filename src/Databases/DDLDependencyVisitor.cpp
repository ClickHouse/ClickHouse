#include <Poco/Util/AbstractConfiguration.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Databases/removeWhereConditionPlaceholder.h>
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
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTViewTargets.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/isLocalAddress.h>
#include <Common/parseAddress.h>
#include <Common/parseRemoteDescription.h>
#include <Common/quoteString.h>
#include <Poco/Net/IPAddress.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Poco/String.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 table_function_remote_max_addresses;
}

namespace
{
    /// Data for DDLDependencyVisitor.
    /// Used to visits ASTCreateQuery and extracts the names of all tables explicitly referenced in the create query.
    class DDLDependencyVisitorData
    {
        friend void tryVisitNestedSelect(const String & query, DDLDependencyVisitorData & data);
    public:
        DDLDependencyVisitorData(const ContextPtr & global_context_, const QualifiedTableName & table_name_, const ASTPtr & ast_, const String & current_database_, bool can_throw_, bool validate_current_database_)
            : create_query(ast_), table_name(table_name_), default_database(global_context_->getCurrentDatabase()), current_database(current_database_), global_context(global_context_), can_throw(can_throw_), validate_current_database(validate_current_database_)
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
        ContextPtr global_context;
        TableNamesSet dependencies;
        bool can_throw;
        bool validate_current_database;
        std::optional<StorageID> mv_to_dependency;
        std::optional<StorageID> mv_from_dependency;

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
                    else if (target.kind == ViewTarget::Kind::Inner && !create.is_window_view)
                    {
                        mv_to_dependency = StorageID{table_name.database, target.table_id.getQualifiedName().table, target.inner_uuid};
                        mv_to_dependency->table_name = StorageMaterializedView::generateInnerTableName(mv_to_dependency.value());
                    }
                    else if (target.kind == ViewTarget::Kind::Samples
                        || target.kind == ViewTarget::Kind::Tags
                        || target.kind == ViewTarget::Kind::Metrics)
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
                        ApplyWithSubqueryVisitor::visit(select_copy);

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
        /// or the table-function form Distributed(cluster_name, table_function()[, sharding_key]).
        void visitDistributedTableEngine(const ASTFunction & table_engine)
        {
            /// We consider only dependencies on local tables: this node depends on an object of the cluster only
            /// when it hosts a local replica and therefore runs the query (or the target table function) locally.
            bool has_local_replicas = false;
            if (auto cluster_name = tryGetClusterNameFromArgument(table_engine, 0))
            {
                auto cluster = global_context->tryGetCluster(*cluster_name);
                if (cluster && cluster->getLocalShardCount())
                    has_local_replicas = true;
            }

            /// In the table-function form the second argument is the target and the following arguments are the
            /// sharding key and the storage policy, which the engine ignores (see `has_sharding_key` in
            /// `StorageDistributed`). A `dictGet` / `joinGet` inside the ignored sharding key must not become a
            /// referential dependency: otherwise DROP / RENAME of an object the engine never uses would be
            /// rejected under `check_referential_table_dependencies = 1`, contradicting the documented read-only
            /// semantics of that form. Detect the table-function form the same way `registerStorageDistributed`
            /// disambiguates the second argument, and skip the ignored key subtree so the generic walk does not
            /// collect dependencies from it.
            if (table_engine.arguments && table_engine.arguments->children.size() >= 2)
            {
                const auto * table_function = table_engine.arguments->children[1]->as<ASTFunction>();
                if (table_function && TableFunctionFactory::instance().isTableFunctionName(table_function->name))
                {
                    for (size_t i = 2; i < table_engine.arguments->children.size(); ++i)
                        skip_asts.emplace(table_engine.arguments->children[i].get());

                    /// The target table function is executed on the shards of the named cluster. Only when this
                    /// node hosts a local replica does it run the function locally and depend on the objects it
                    /// reads; otherwise the function runs only on remote shards, so skip its subtree too. Without
                    /// this, the generic walk would descend into the target (e.g. a `dictGet` in its arguments, or
                    /// a nested `view(SELECT ... FROM src)`) and register a bogus referential dependency for a
                    /// remote-only cluster, blocking DROP / RENAME of an object the engine never reads locally.
                    /// This mirrors `visitRemoteFunction`.
                    if (!has_local_replicas)
                        skip_asts.emplace(table_function);

                    return;
                }
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
            else if (function.name == "timeSeriesSamples" || function.name == "timeSeriesData"
                     || function.name == "timeSeriesTags" || function.name == "timeSeriesMetrics")
            {
                /// timeSeriesMetrics([db.]table) / timeSeriesMetrics('db', 'table')
                addDependencyFromLeadingTableNameArguments(function, /* short_form_num_args= */ 1);
            }
            else if (function.name == "timeSeriesSelector")
            {
                /// timeSeriesSelector([db.]table, selector, min_time, max_time)
                addDependencyFromLeadingTableNameArguments(function, /* short_form_num_args= */ 4);
            }
            else if (function.name == "prometheusQuery")
            {
                /// prometheusQuery([db.]table, promql_query, evaluation_time)
                addDependencyFromLeadingTableNameArguments(function, /* short_form_num_args= */ 3);
            }
            else if (function.name == "prometheusQueryRange")
            {
                /// prometheusQueryRange([db.]table, promql_query, start_time, end_time, step)
                addDependencyFromLeadingTableNameArguments(function, /* short_form_num_args= */ 5);
            }
            else if (function.name == "loop")
            {
                /// loop([db.]table) / loop('db', 'table'); for loop(inner_table_function(...)) no name is
                /// extracted here and the generic walk descends into the inner function instead.
                addDependencyFromLeadingTableNameArguments(function, /* short_form_num_args= */ 1);
            }
            else if (function.name == "mergeTreeIndex" || function.name == "mergeTreeProjection"
                     || function.name == "mergeTreeTextIndex" || function.name == "mergeTreeAnalyzeIndexes"
                     || function.name == "mergeTreeCodecBlockCounts")
            {
                /// mergeTreeIndex(database, table, ...) / mergeTreeProjection(database, table, projection) /
                /// mergeTreeTextIndex(database, table, index_name) / mergeTreeAnalyzeIndexes(database, table, ...) /
                /// mergeTreeCodecBlockCounts(database, table):
                /// these inspect a concrete local MergeTree table named by the first two arguments and read it
                /// via `DatabaseCatalog::getTable` at read time, so a DROP / RENAME of that table must be tracked
                /// as a referential dependency. The UUID-resolved form (`mergeTreeAnalyzeIndexesUUID`) references
                /// its source by UUID rather than by name and so needs no name-based dependency here. Only a
                /// spelling with an explicit non-empty database yields a dependency: an empty database argument
                /// resolves against the current database of the *querying* session at read time
                /// (`evaluateConstantExpressionForDatabaseName`), not necessarily that of this CREATE, so it
                /// does not name a stable object (the same rule `addDependencyFromLeadingTableNameArguments`
                /// applies; a persisted `Distributed(..., mergeTree*(...))` target has the create-time database
                /// baked into an empty argument - and any other database expression folded to a literal - by
                /// `bindTableFunctionTargetToCurrentDatabase`, so it keeps yielding a dependency here).
                auto qualified_name = tryGetDatabaseAndTableNameFromArguments(
                    function, 0, 1, /* apply_current_database= */ false, /* evaluate_database= */ false);
                if (qualified_name && !qualified_name->database.empty())
                    dependencies.emplace(std::move(qualified_name).value());
            }
            /// The `merge` table function is deliberately absent here: its argument is a regular expression,
            /// not a table name, and the set of matching tables is resolved anew on every read. Dropping or
            /// renaming a matched table does not break the definition - later reads simply match the remaining
            /// tables - and tables created afterwards join the set without ever appearing in any recorded
            /// dependency. A snapshot of the currently matching tables would therefore be wrong in both
            /// directions, and this analysis is re-run from metadata at server startup, when the catalog is not
            /// fully loaded yet, so a regexp cannot be reliably evaluated at all. This mirrors the `Merge` table
            /// engine, which has never registered dependencies on the tables matched by its regexp.
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
        /// remote(cluster_name_or_named_collection, ...)
        /// cluster('cluster_name', db_name.table_name, ...)
        /// cluster('cluster_name', 'db_name', 'table_name', ...)
        /// cluster('cluster_name', table_function(), ...)
        void visitRemoteFunction(const ASTFunction & function, bool is_cluster_function)
        {
            /// We consider dependencies on local tables only.
            bool has_local_replicas = false;

            const ASTIdentifier * first_arg_identifier = nullptr;
            if (!is_cluster_function && function.arguments && !function.arguments->children.empty())
                first_arg_identifier = function.arguments->children[0]->as<ASTIdentifier>();

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
            else if (first_arg_identifier)
            {
                /// A bare identifier names either a named collection or a configured cluster;
                /// `parseRemoteFunctionArguments` tries them in this order. Both carry their target
                /// specification differently from the positional form, so they are handled here and
                /// not by the address-pattern check below.
                if (auto collection = tryGetNamedCollection(first_arg_identifier->name()))
                {
                    visitRemoteFunctionNamedCollection(function, *collection, function.name == "remoteSecure");
                    return;
                }

                if (auto cluster = global_context->tryGetCluster(first_arg_identifier->name()))
                {
                    if (cluster->getLocalShardCount())
                        has_local_replicas = true;
                }
            }
            else
            {
                /// For remote() / remoteSecure() the addresses are given inline. Whether such an address is
                /// this server cannot be decided in full generality here (see
                /// remoteFunctionAddressesContainLocalHost), but the decidable spellings - an IP literal or
                /// `localhost` on the server's own port, e.g. the loop-back form `remote('127.0.0.1', ...)` -
                /// are recognized, so a persisted target that reads a local table through them records a
                /// referential dependency on it. Any other pattern keeps the long-standing assumption that
                /// it does not contain the local host.
                has_local_replicas = remoteFunctionAddressesContainLocalHost(function, function.name == "remoteSecure");
            }

            if (!function.arguments)
                return;

            ASTs & args = function.arguments->children;
            if (args.size() < 2)
                return;

            const ASTFunction * table_function = nullptr;
            if (const auto * second_arg_as_function = args[1]->as<ASTFunction>();
                second_arg_as_function && TableFunctionFactory::instance().isTableFunctionName(second_arg_as_function->name))
            {
                /// `TableFunctionFactory::isTableFunctionName` (not `KnownTableFunctionNames`) so that factory
                /// aliases are recognized too: e.g. `timeSeriesData` is registered only as an alias of
                /// `timeSeriesSamples` and is absent from `KnownTableFunctionNames`. Missing it here would leave
                /// the remote target subtree unskipped, and the generic walk would then register a bogus local
                /// dependency on the table the function reads only on the remote shards. This matches the
                /// disambiguation used by `visitDistributedTableEngine` and `registerStorageDistributed`.
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

        /// remote(nc[, key = value, ...]) / remoteSecure(nc[, key = value, ...]): the named-collection form.
        /// The addresses, database, and table come from the collection, each replaceable by a `key = value`
        /// override argument (`tryGetNamedCollectionWithOverrides` merges them the same way at read time), so
        /// both locality and the target name are derived from the merged view. Mirroring
        /// `parseRemoteFunctionArguments`: a missing database key means the fixed `default` database, while
        /// an empty value falls back to the current database of the querying session at read time and so
        /// does not name a stable object (a persisted `Distributed` target has the create-time database
        /// injected as an override by `bindTableFunctionTargetToCurrentDatabase`, so it keeps yielding a
        /// dependency here); a table-function value of the `database` / `db` override is the
        /// named-collection spelling of `remote('addr', table_function())`.
        void visitRemoteFunctionNamedCollection(const ASTFunction & function, const NamedCollection & collection, bool secure)
        {
            const ASTs & args = function.arguments->children;

            bool has_database_override = false;
            bool has_table_override = false;
            bool has_addresses_override = false;
            std::optional<String> database_override;
            std::optional<String> table_override;
            const ASTFunction * target_table_function = nullptr;

            for (size_t i = 1; i < args.size(); ++i)
            {
                const auto * override_function = args[i]->as<ASTFunction>();
                if (!override_function || override_function->name != "equals" || !override_function->arguments
                    || override_function->arguments->children.size() != 2)
                    continue;

                const auto * key_identifier = override_function->arguments->children[0]->as<ASTIdentifier>();
                if (!key_identifier)
                    continue;

                const String & key = key_identifier->name();
                const ASTPtr & value = override_function->arguments->children[1];

                if (key == "database" || key == "db")
                {
                    has_database_override = true;
                    if (const auto * value_function = value->as<ASTFunction>();
                        value_function && TableFunctionFactory::instance().isTableFunctionName(value_function->name))
                        target_table_function = value_function;
                    else
                        database_override = tryGetStringFromAST(value);
                }
                else if (key == "table")
                {
                    has_table_override = true;
                    table_override = tryGetStringFromAST(value);
                }
                else if (key == "addresses_expr" || key == "host" || key == "hostname" || key == "port")
                {
                    /// The addresses could be recomputed from the overridden values, but such spellings are
                    /// not seen in practice; keep the long-standing assumption that the target is not local.
                    has_addresses_override = true;
                }
            }

            const bool has_local_replicas = !has_addresses_override && namedCollectionAddressesContainLocalHost(collection, secure);

            if (target_table_function)
            {
                if (!has_local_replicas)
                {
                    /// The table function will be executed remotely, so we won't check it or its arguments
                    /// for dependencies.
                    skip_asts.emplace(target_table_function);
                }
                return;
            }

            if (!has_local_replicas)
                return;

            /// An override that could not be resolved to a constant here leaves the target undecidable.
            if ((has_database_override && !database_override) || (has_table_override && !table_override))
                return;

            String database = database_override ? *database_override : collection.getAnyOrDefault<String>({"db", "database"}, "default");
            String table = table_override ? *table_override : collection.getOrDefault<String>("table", "");
            if (database.empty() || table.empty())
                return;

            dependencies.emplace(QualifiedTableName{std::move(database), std::move(table)});
        }

        /// Gets an argument as a string, evaluates constants if necessary.
        std::optional<String> tryGetStringFromArgument(const ASTFunction & function, size_t arg_idx, bool evaluate = true) const
        {
            if (!function.arguments)
                return {};

            const ASTs & args = function.arguments->children;
            if (arg_idx >= args.size())
                return {};

            return tryGetStringFromAST(args[arg_idx], evaluate);
        }

        /// Gets an AST node as a string, evaluates constants if necessary.
        std::optional<String> tryGetStringFromAST(const ASTPtr & arg, bool evaluate = true) const
        {
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
                global_context_copy->setCurrentDatabase(current_database);
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
        /// With `evaluate_database = false` only a syntactically stable spelling of the database argument is
        /// accepted - a string literal or an identifier, i.e. a name that is stored in the metadata as is.
        /// An arbitrary expression such as `currentDatabase()` is not accepted then: it survives in a stored
        /// query body unchanged (`AddDefaultDatabaseVisitor` folds `currentDatabase()` only in DDL, not in the
        /// `SELECT` of a view) and is therefore re-evaluated against the current database of the *querying*
        /// session at read time, so it does not name a stable object. Evaluating it here would record a
        /// dependency on the create-time database instead, which both blocks `DROP` / `RENAME` of a table the
        /// query does not read and leaves the table it does read unprotected.
        std::optional<QualifiedTableName> tryGetDatabaseAndTableNameFromArguments(
            const ASTFunction & function,
            size_t database_arg_idx,
            size_t table_arg_idx,
            bool apply_current_database = true,
            bool evaluate_database = true) const
        {
            auto database = tryGetStringFromArgument(function, database_arg_idx, evaluate_database);
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

        /// Adds a dependency from a table function whose leading arguments name a table as `[database, ] table`:
        /// in the short form (`short_form_num_args` arguments) the first argument is the table name, and the
        /// long form (one argument more) carries the database and the table in the first two arguments.
        /// Only spellings with an explicit, syntactically stable database yield a dependency: a name spelled out
        /// in the metadata (a string literal or an identifier), not an expression such as `currentDatabase()`
        /// re-evaluated per querying session. An unqualified short-form name - a bare
        /// identifier, or a string, which is the whole table name and is never split at a dot - is resolved
        /// through `Context::resolveStorageID` at execution time, against the current database and the session
        /// temporary tables of the *querying* session, not necessarily those of this CREATE, so it does not name
        /// a stable object this metadata could depend on. Stored queries persist such spellings unchanged
        /// (`AddDefaultDatabaseVisitor` does not rewrite table-function arguments); the only context that binds
        /// them to the create-time database is the persisted `Distributed(..., table_function())` target
        /// (`bindTableFunctionTargetToCurrentDatabase` produces the explicit long form), and an unqualified
        /// spelling surviving there names a session temporary table, which takes no part in dependency tracking.
        void addDependencyFromLeadingTableNameArguments(const ASTFunction & function, size_t short_form_num_args)
        {
            if (!function.arguments)
                return;

            size_t num_args = function.arguments->children.size();
            if (num_args == short_form_num_args)
            {
                if (const auto * identifier = dynamic_cast<const ASTIdentifier *>(function.arguments->children.at(0).get()))
                {
                    auto table_identifier = identifier->createTable();
                    if (table_identifier && !table_identifier->getDatabaseName().empty())
                        dependencies.emplace(QualifiedTableName{table_identifier->getDatabaseName(), table_identifier->shortName()});
                }
            }
            else if (num_args == short_form_num_args + 1)
            {
                auto qualified_name = tryGetDatabaseAndTableNameFromArguments(
                    function, 0, 1, /* apply_current_database= */ false, /* evaluate_database= */ false);
                if (qualified_name && !qualified_name->database.empty())
                    dependencies.emplace(std::move(qualified_name).value());
            }
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

        /// Looks up a named collection. The collections are loaded early at server startup, before any
        /// table metadata, so this is an in-memory lookup even when this analysis re-runs from metadata
        /// while the tables are being loaded.
        NamedCollectionPtr tryGetNamedCollection(const String & collection_name) const
        {
            try
            {
                NamedCollectionFactory::instance().loadIfNot();
                return NamedCollectionFactory::instance().tryGet(collection_name);
            }
            catch (...)
            {
                /// Ok: an identifier that cannot be looked up as a collection is treated as a cluster name,
                /// exactly as parseRemoteFunctionArguments treats an unknown collection.
                return nullptr;
            }
        }

        /// Best-effort check whether the address pattern of a `remote()` / `remoteSecure()` call names this
        /// server. At read time the function builds its cluster from the pattern and marks an address local
        /// exactly when `isLocalAddress` accepts its IP and its port is the server's own TCP port. Resolving
        /// an arbitrary host name the same way would require a DNS lookup, which this analysis cannot afford:
        /// it is re-run from metadata at server startup, where a slow or unavailable resolver would block
        /// loading the tables. So only the spellings that need no resolution are recognized - an IP literal
        /// and `localhost` - and any other host is assumed non-local, as this analysis has always assumed
        /// for the whole pattern.
        bool remoteFunctionAddressesContainLocalHost(const ASTFunction & function, bool secure) const
        {
            auto addresses_expr = tryGetStringFromArgument(function, 0);
            if (!addresses_expr)
                return false;

            return addressesPatternContainsLocalHost(*addresses_expr, secure);
        }

        /// The addresses of the named-collection form of remote() / remoteSecure(), assembled the way
        /// `parseRemoteFunctionArguments` assembles them: `addresses_expr`, or `host` / `hostname` with an
        /// optional `port`.
        bool namedCollectionAddressesContainLocalHost(const NamedCollection & collection, bool secure) const
        {
            try
            {
                String addresses = collection.getOrDefault<String>("addresses_expr", "");
                if (addresses.empty() && collection.hasAny({"host", "hostname"}))
                {
                    addresses = collection.getAny<String>({"host", "hostname"});
                    if (collection.has("port"))
                        addresses += ':' + toString(collection.get<UInt64>("port"));
                }
                return !addresses.empty() && addressesPatternContainsLocalHost(addresses, secure);
            }
            catch (...)
            {
                /// Ok: a collection value of an unexpected type cannot be attributed to this server; the
                /// function itself reports such a collection when it is executed.
                return false;
            }
        }

        bool addressesPatternContainsLocalHost(const String & addresses_expr, bool secure) const
        {
            UInt16 clickhouse_port = secure ? global_context->getTCPPortSecure().value_or(0) : global_context->getTCPPort();
            if (!clickhouse_port)
                return false;

            try
            {
                size_t max_addresses = global_context->getSettingsRef()[Setting::table_function_remote_max_addresses];
                for (const auto & shard : parseRemoteDescription(addresses_expr, 0, addresses_expr.size(), ',', max_addresses))
                {
                    for (const auto & replica : parseRemoteDescription(shard, 0, shard.size(), '|', max_addresses))
                    {
                        auto [host, port] = parseAddress(replica, clickhouse_port);
                        if (port != clickhouse_port)
                            continue;

                        /// parseAddress keeps the square brackets of a bracketed IPv6 host.
                        if (host.size() >= 2 && host.front() == '[' && host.back() == ']')
                            host = host.substr(1, host.size() - 2);

                        if (host == "localhost")
                            return true;

                        Poco::Net::IPAddress ip;
                        if (Poco::Net::IPAddress::tryParse(host, ip) && isLocalAddress(ip))
                            return true;
                    }
                }
            }
            catch (...)
            {
                /// Ok: a pattern this parsing rejects cannot be attributed to this server; the function itself
                /// reports such a pattern when it is executed.
                return false;
            }

            return false;
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


CreateQueryDependencies getDependenciesFromCreateQuery(const ContextPtr & global_global_context, const QualifiedTableName & table_name, const ASTPtr & ast, const String & current_database, bool can_throw, bool validate_current_database)
{
    DDLDependencyVisitor::Data data{global_global_context, table_name, ast, current_database, can_throw, validate_current_database};
    DDLDependencyVisitor::Visitor visitor{data};
    visitor.visit(ast);
    return {data.getDependencies(), data.getMvToDependency(), data.getMvFromDependency()};
}

TableNamesSet getDependenciesFromDictionaryNestedSelectQuery(const ContextPtr & global_context, const QualifiedTableName & table_name, const ASTPtr & ast, const String & select_query, const String & current_database, bool can_throw)
{
    DDLDependencyVisitor::Data data{global_context, table_name, ast, current_database, can_throw, /*validate_current_database=*/true};
    tryVisitNestedSelect(select_query, data);
    return std::move(data).getDependencies();
}

}
