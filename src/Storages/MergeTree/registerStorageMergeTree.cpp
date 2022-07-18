#include <Databases/DatabaseReplicatedHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexMinMax.h>
#include <Storages/MergeTree/MergeTreeIndexSet.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>

#include <Common/Macros.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/typeid_cast.h>
#include <Common/thread_local_rng.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_STORAGE;
    extern const int NO_REPLICA_NAME_GIVEN;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}


/** Get the list of column names.
  * It can be specified in the tuple: (Clicks, Cost),
  * or as one column: Clicks.
  */
static Names extractColumnNames(const ASTPtr & node)
{
    const auto * expr_func = node->as<ASTFunction>();

    if (expr_func && expr_func->name == "tuple")
    {
        const auto & elements = expr_func->children.at(0)->children;
        Names res;
        res.reserve(elements.size());
        for (const auto & elem : elements)
            res.push_back(getIdentifierName(elem));

        return res;
    }
    else
    {
        return {getIdentifierName(node)};
    }
}

static String getMergeTreeVerboseHelp(bool)
{
    using namespace std::string_literals;

    String help = R"(

Syntax for the MergeTree table engine:

CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
    INDEX index_name1 expr1 TYPE type1(...) GRANULARITY value1,
    INDEX index_name2 expr2 TYPE type2(...) GRANULARITY value2
) ENGINE = MergeTree()
ORDER BY expr
[PARTITION BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'], ...]
[SETTINGS name=value, ...]

See details in documentation: https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/. Other engines of the family support different syntax, see details in the corresponding documentation topics.

If you use the Replicated version of engines, see https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication/.
)";

    return help;
}

static ColumnsDescription getColumnsDescriptionFromZookeeper(const String & raw_zookeeper_path, ContextMutablePtr context)
{
    String zookeeper_name = zkutil::extractZooKeeperName(raw_zookeeper_path);
    String zookeeper_path = zkutil::extractZooKeeperPath(raw_zookeeper_path, true);

    if (!context->hasZooKeeper() && !context->hasAuxiliaryZooKeeper(zookeeper_name))
        throw Exception{ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot get replica structure without zookeeper, you must specify the structure manually"};

    zkutil::ZooKeeperPtr zookeeper;
    try
    {
        if (zookeeper_name == StorageReplicatedMergeTree::getDefaultZooKeeperName())
            zookeeper = context->getZooKeeper();
        else
            zookeeper = context->getAuxiliaryZooKeeper(zookeeper_name);
    }
    catch (...)
    {
        throw Exception{ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot get replica structure from zookeeper, because cannot get zookeeper: {}. You must specify structure manually", getCurrentExceptionMessage(false)};
    }

    if (!zookeeper->exists(zookeeper_path + "/replicas"))
        throw Exception{ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot get replica structure, because there no other replicas in zookeeper. You must specify the structure manually"};

    Coordination::Stat columns_stat;
    return ColumnsDescription::parse(zookeeper->get(fs::path(zookeeper_path) / "columns", &columns_stat));
}


static StoragePtr create(const StorageFactory::Arguments & args)
{
    /** [Replicated][|Summing|VersionedCollapsing|Collapsing|Aggregating|Replacing|Graphite]MergeTree (2 * 7 combinations) engines
        * The argument for the engine should be:
        *  - (for Replicated) The path to the table in ZooKeeper
        *  - (for Replicated) Replica name in ZooKeeper
        *  - the name of the column with the date;
        *  - (optional) expression for sampling
        *     (the query with `SAMPLE x` will select rows that have a lower value in this column than `x * UINT32_MAX`);
        *  - an expression for sorting (either a scalar expression or a tuple of several);
        *  - index_granularity;
        *  - (for Collapsing) the name of Int8 column that contains `sign` type with the change of "visit" (taking values 1 and -1).
        * For example: ENGINE = ReplicatedCollapsingMergeTree('/tables/mytable', 'rep02', EventDate, (CounterID, EventDate, intHash32(UniqID), VisitID), 8192, Sign).
        *  - (for Summing, optional) a tuple of columns to be summed. If not specified, all numeric columns that are not included in the primary key are used.
        *  - (for Replacing, optional) the column name of one of the UInt types, which stands for "version"
        * For example: ENGINE = ReplicatedCollapsingMergeTree('/tables/mytable', 'rep02', EventDate, (CounterID, EventDate, intHash32(UniqID), VisitID), 8192, Sign).
        *  - (for Graphite) the parameter name in config file with settings of thinning rules.
        *
        * MergeTree(date, [sample_key], primary_key, index_granularity)
        * CollapsingMergeTree(date, [sample_key], primary_key, index_granularity, sign)
        * SummingMergeTree(date, [sample_key], primary_key, index_granularity, [columns_to_sum])
        * AggregatingMergeTree(date, [sample_key], primary_key, index_granularity)
        * ReplacingMergeTree(date, [sample_key], primary_key, index_granularity, [version_column])
        * GraphiteMergeTree(date, [sample_key], primary_key, index_granularity, 'config_element')
        *
        * Alternatively, you can specify:
        *  - Partitioning expression in the PARTITION BY clause;
        *  - Sorting key in the ORDER BY clause;
        *  - Primary key (if it is different from the sorting key) in the PRIMARY KEY clause;
        *  - Sampling expression in the SAMPLE BY clause;
        *  - Additional MergeTreeSettings in the SETTINGS clause;
        */

    bool is_extended_storage_def = args.storage_def->partition_by || args.storage_def->primary_key || args.storage_def->order_by
        || args.storage_def->sample_by || (args.query.columns_list->indices && !args.query.columns_list->indices->children.empty())
        || (args.query.columns_list->projections && !args.query.columns_list->projections->children.empty()) || args.storage_def->settings;

    String name_part = args.engine_name.substr(0, args.engine_name.size() - strlen("MergeTree"));

    bool replicated = startsWith(name_part, "Replicated");
    if (replicated)
        name_part = name_part.substr(strlen("Replicated"));

    MergeTreeData::MergingParams merging_params;
    merging_params.mode = MergeTreeData::MergingParams::Ordinary;

    if (name_part == "Collapsing")
        merging_params.mode = MergeTreeData::MergingParams::Collapsing;
    else if (name_part == "Summing")
        merging_params.mode = MergeTreeData::MergingParams::Summing;
    else if (name_part == "Aggregating")
        merging_params.mode = MergeTreeData::MergingParams::Aggregating;
    else if (name_part == "Replacing")
        merging_params.mode = MergeTreeData::MergingParams::Replacing;
    else if (name_part == "Graphite")
        merging_params.mode = MergeTreeData::MergingParams::Graphite;
    else if (name_part == "VersionedCollapsing")
        merging_params.mode = MergeTreeData::MergingParams::VersionedCollapsing;
    else if (!name_part.empty())
        throw Exception(
            "Unknown storage " + args.engine_name + getMergeTreeVerboseHelp(is_extended_storage_def), ErrorCodes::UNKNOWN_STORAGE);

    /// NOTE Quite complicated.

    size_t min_num_params = 0;
    size_t max_num_params = 0;
    String needed_params;

    auto add_mandatory_param = [&](const char * desc)
    {
        ++min_num_params;
        ++max_num_params;
        needed_params += needed_params.empty() ? "\n" : ",\n";
        needed_params += desc;
    };
    auto add_optional_param = [&](const char * desc)
    {
        ++max_num_params;
        needed_params += needed_params.empty() ? "\n" : ",\n[";
        needed_params += desc;
        needed_params += "]";
    };

    if (replicated)
    {
        if (is_extended_storage_def)
        {
            add_optional_param("path in ZooKeeper");
            add_optional_param("replica name");
        }
        else
        {
            add_mandatory_param("path in ZooKeeper");
            add_mandatory_param("replica name");
        }
    }

    if (!is_extended_storage_def)
    {
        add_mandatory_param("name of column with date");
        add_optional_param("sampling element of primary key");
        add_mandatory_param("primary key expression");
        add_mandatory_param("index granularity");
    }

    switch (merging_params.mode)
    {
        default:
            break;
        case MergeTreeData::MergingParams::Summing:
            add_optional_param("list of columns to sum");
            break;
        case MergeTreeData::MergingParams::Replacing:
            add_optional_param("version");
            break;
        case MergeTreeData::MergingParams::Collapsing:
            add_mandatory_param("sign column");
            break;
        case MergeTreeData::MergingParams::Graphite:
            add_mandatory_param("'config_element_for_graphite_schema'");
            break;
        case MergeTreeData::MergingParams::VersionedCollapsing: {
            add_mandatory_param("sign column");
            add_mandatory_param("version");
            break;
        }
    }

    ASTs & engine_args = args.engine_args;
    size_t arg_num = 0;
    size_t arg_cnt = engine_args.size();

    if (arg_cnt < min_num_params || arg_cnt > max_num_params)
    {
        String msg;
        if (is_extended_storage_def)
            msg += fmt::format("With extended storage definition syntax storage {} requires ", args.engine_name);
        else
            msg += fmt::format("ORDER BY or PRIMARY KEY clause is missing. "
                               "Consider using extended storage definition syntax with ORDER BY or PRIMARY KEY clause. "
                               "With deprecated old syntax (highly not recommended) storage {} requires ", args.engine_name);

        if (max_num_params == 0)
            msg += "no parameters";
        if (min_num_params == max_num_params)
            msg += fmt::format("{} parameters: {}", min_num_params, needed_params);
        else
            msg += fmt::format("{} to {} parameters: {}", min_num_params, max_num_params, needed_params);


        msg += getMergeTreeVerboseHelp(is_extended_storage_def);

        throw Exception(msg, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    if (is_extended_storage_def)
    {
        /// Allow expressions in engine arguments.
        /// In new syntax argument can be literal or identifier or array/tuple of identifiers.
        size_t arg_idx = 0;
        try
        {
            for (; arg_idx < engine_args.size(); ++arg_idx)
            {
                auto & arg = engine_args[arg_idx];
                auto * arg_func = arg->as<ASTFunction>();
                if (!arg_func)
                    continue;

                /// If we got ASTFunction, let's evaluate it and replace with ASTLiteral.
                /// Do not try evaluate array or tuple, because it's array or tuple of column identifiers.
                if (arg_func->name == "array" || arg_func->name == "tuple")
                    continue;
                Field value = evaluateConstantExpression(arg, args.getLocalContext()).first;
                arg = std::make_shared<ASTLiteral>(value);
            }
        }
        catch (Exception & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot evaluate engine argument {}: {} {}",
                            arg_idx, e.message(), getMergeTreeVerboseHelp(is_extended_storage_def));
        }
    }

    /// For Replicated.
    String zookeeper_path;
    String replica_name;
    StorageReplicatedMergeTree::RenamingRestrictions renaming_restrictions = StorageReplicatedMergeTree::RenamingRestrictions::ALLOW_ANY;

    if (replicated)
    {
        bool has_arguments = arg_num + 2 <= arg_cnt;
        bool has_valid_arguments = has_arguments && engine_args[arg_num]->as<ASTLiteral>() && engine_args[arg_num + 1]->as<ASTLiteral>();

        ASTLiteral * ast_zk_path;
        ASTLiteral * ast_replica_name;

        if (has_valid_arguments)
        {
            /// Get path and name from engine arguments
            ast_zk_path = engine_args[arg_num]->as<ASTLiteral>();
            if (ast_zk_path && ast_zk_path->value.getType() == Field::Types::String)
                zookeeper_path = safeGet<String>(ast_zk_path->value);
            else
                throw Exception(
                    "Path in ZooKeeper must be a string literal" + getMergeTreeVerboseHelp(is_extended_storage_def),
                    ErrorCodes::BAD_ARGUMENTS);
            ++arg_num;

            ast_replica_name = engine_args[arg_num]->as<ASTLiteral>();
            if (ast_replica_name && ast_replica_name->value.getType() == Field::Types::String)
                replica_name = safeGet<String>(ast_replica_name->value);
            else
                throw Exception(
                    "Replica name must be a string literal" + getMergeTreeVerboseHelp(is_extended_storage_def), ErrorCodes::BAD_ARGUMENTS);

            if (replica_name.empty())
                throw Exception(
                    "No replica name in config" + getMergeTreeVerboseHelp(is_extended_storage_def), ErrorCodes::NO_REPLICA_NAME_GIVEN);
            ++arg_num;
        }
        else if (is_extended_storage_def
            && (arg_cnt == 0
                || !engine_args[arg_num]->as<ASTLiteral>()
                || (arg_cnt == 1 && merging_params.mode == MergeTreeData::MergingParams::Graphite)))
        {
            /// Try use default values if arguments are not specified.
            /// Note: {uuid} macro works for ON CLUSTER queries when database engine is Atomic.
            zookeeper_path = args.getContext()->getConfigRef().getString("default_replica_path", "/clickhouse/tables/{uuid}/{shard}");
            /// TODO maybe use hostname if {replica} is not defined?
            replica_name = args.getContext()->getConfigRef().getString("default_replica_name", "{replica}");

            /// Modify query, so default values will be written to metadata
            assert(arg_num == 0);
            ASTs old_args;
            std::swap(engine_args, old_args);
            auto path_arg = std::make_shared<ASTLiteral>(zookeeper_path);
            auto name_arg = std::make_shared<ASTLiteral>(replica_name);
            ast_zk_path = path_arg.get();
            ast_replica_name = name_arg.get();
            engine_args.emplace_back(std::move(path_arg));
            engine_args.emplace_back(std::move(name_arg));
            std::move(std::begin(old_args), std::end(old_args), std::back_inserter(engine_args));
            arg_num = 2;
            arg_cnt += 2;
        }
        else
            throw Exception("Expected two string literal arguments: zookeeper_path and replica_name", ErrorCodes::BAD_ARGUMENTS);

        /// Allow implicit {uuid} macros only for zookeeper_path in ON CLUSTER queries
        bool is_on_cluster = args.getLocalContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
        bool is_replicated_database = args.getLocalContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY &&
                                      DatabaseCatalog::instance().getDatabase(args.table_id.database_name)->getEngineName() == "Replicated";
        bool allow_uuid_macro = is_on_cluster || is_replicated_database || args.query.attach;

        /// Unfold {database} and {table} macro on table creation, so table can be renamed.
        if (!args.attach)
        {
            if (is_replicated_database && !is_extended_storage_def)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Old syntax is not allowed for ReplicatedMergeTree tables in Replicated databases");

            Macros::MacroExpansionInfo info;
            /// NOTE: it's not recursive
            info.expand_special_macros_only = true;
            info.table_id = args.table_id;
            /// Avoid unfolding {uuid} macro on this step.
            /// We did unfold it in previous versions to make moving table from Atomic to Ordinary database work correctly,
            /// but now it's not allowed (and it was the only reason to unfold {uuid} macro).
            info.table_id.uuid = UUIDHelpers::Nil;
            zookeeper_path = args.getContext()->getMacros()->expand(zookeeper_path, info);

            info.level = 0;
            replica_name = args.getContext()->getMacros()->expand(replica_name, info);
        }

        ast_zk_path->value = zookeeper_path;
        ast_replica_name->value = replica_name;

        /// Expand other macros (such as {shard} and {replica}). We do not expand them on previous step
        /// to make possible copying metadata files between replicas.
        Macros::MacroExpansionInfo info;
        info.table_id = args.table_id;
        if (is_replicated_database)
        {
            auto database = DatabaseCatalog::instance().getDatabase(args.table_id.database_name);
            info.shard = getReplicatedDatabaseShardName(database);
            info.replica = getReplicatedDatabaseReplicaName(database);
        }
        if (!allow_uuid_macro)
            info.table_id.uuid = UUIDHelpers::Nil;
        zookeeper_path = args.getContext()->getMacros()->expand(zookeeper_path, info);

        info.level = 0;
        info.table_id.uuid = UUIDHelpers::Nil;
        replica_name = args.getContext()->getMacros()->expand(replica_name, info);

        /// We do not allow renaming table with these macros in metadata, because zookeeper_path will be broken after RENAME TABLE.
        /// NOTE: it may happen if table was created by older version of ClickHouse (< 20.10) and macros was not unfolded on table creation
        /// or if one of these macros is recursively expanded from some other macro.
        /// Also do not allow to move table from Atomic to Ordinary database if there's {uuid} macro
        if (info.expanded_database || info.expanded_table)
            renaming_restrictions = StorageReplicatedMergeTree::RenamingRestrictions::DO_NOT_ALLOW;
        else if (info.expanded_uuid)
            renaming_restrictions = StorageReplicatedMergeTree::RenamingRestrictions::ALLOW_PRESERVING_UUID;
    }

    /// This merging param maybe used as part of sorting key
    std::optional<String> merging_param_key_arg;

    if (merging_params.mode == MergeTreeData::MergingParams::Collapsing)
    {
        if (!tryGetIdentifierNameInto(engine_args[arg_cnt - 1], merging_params.sign_column))
            throw Exception(
                "Sign column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);
        --arg_cnt;
    }
    else if (merging_params.mode == MergeTreeData::MergingParams::Replacing)
    {
        /// If the last element is not index_granularity or replica_name (a literal), then this is the name of the version column.
        if (arg_cnt && !engine_args[arg_cnt - 1]->as<ASTLiteral>())
        {
            if (!tryGetIdentifierNameInto(engine_args[arg_cnt - 1], merging_params.version_column))
                throw Exception(
                    "Version column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                    ErrorCodes::BAD_ARGUMENTS);
            --arg_cnt;
        }
    }
    else if (merging_params.mode == MergeTreeData::MergingParams::Summing)
    {
        /// If the last element is not index_granularity or replica_name (a literal), then this is a list of summable columns.
        if (arg_cnt && !engine_args[arg_cnt - 1]->as<ASTLiteral>())
        {
            merging_params.columns_to_sum = extractColumnNames(engine_args[arg_cnt - 1]);
            --arg_cnt;
        }
    }
    else if (merging_params.mode == MergeTreeData::MergingParams::Graphite)
    {
        String graphite_config_name;
        String error_msg
            = "Last parameter of GraphiteMergeTree must be name (in single quotes) of element in configuration file with Graphite options";
        error_msg += getMergeTreeVerboseHelp(is_extended_storage_def);

        if (const auto * ast = engine_args[arg_cnt - 1]->as<ASTLiteral>())
        {
            if (ast->value.getType() != Field::Types::String)
                throw Exception(error_msg, ErrorCodes::BAD_ARGUMENTS);

            graphite_config_name = ast->value.get<String>();
        }
        else
            throw Exception(error_msg, ErrorCodes::BAD_ARGUMENTS);

        --arg_cnt;
        setGraphitePatternsFromConfig(args.getContext(), graphite_config_name, merging_params.graphite_params);
    }
    else if (merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing)
    {
        if (!tryGetIdentifierNameInto(engine_args[arg_cnt - 1], merging_params.version_column))
            throw Exception(
                "Version column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);

        --arg_cnt;

        if (!tryGetIdentifierNameInto(engine_args[arg_cnt - 1], merging_params.sign_column))
            throw Exception(
                "Sign column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);

        --arg_cnt;
        /// Version collapsing is the only engine which add additional column to
        /// sorting key.
        merging_param_key_arg = merging_params.version_column;
    }

    String date_column_name;

    StorageInMemoryMetadata metadata;

    ColumnsDescription columns;
    if (args.columns.empty() && replicated)
        columns = getColumnsDescriptionFromZookeeper(zookeeper_path, args.getContext());
    else
        columns = args.columns;

    metadata.setColumns(columns);
    metadata.setComment(args.comment);

    std::unique_ptr<MergeTreeSettings> storage_settings;
    if (replicated)
        storage_settings = std::make_unique<MergeTreeSettings>(args.getContext()->getReplicatedMergeTreeSettings());
    else
        storage_settings = std::make_unique<MergeTreeSettings>(args.getContext()->getMergeTreeSettings());

    if (is_extended_storage_def)
    {
        ASTPtr partition_by_key;
        if (args.storage_def->partition_by)
            partition_by_key = args.storage_def->partition_by->ptr();

        /// Partition key may be undefined, but despite this we store it's empty
        /// value in partition_key structure. MergeTree checks this case and use
        /// single default partition with name "all".
        metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_key, metadata.columns, args.getContext());

        /// PRIMARY KEY without ORDER BY is allowed and considered as ORDER BY.
        if (!args.storage_def->order_by && args.storage_def->primary_key)
            args.storage_def->set(args.storage_def->order_by, args.storage_def->primary_key->clone());

        if (!args.storage_def->order_by)
            throw Exception(
                "You must provide an ORDER BY or PRIMARY KEY expression in the table definition. "
                "If you don't want this table to be sorted, use ORDER BY/PRIMARY KEY tuple()",
                ErrorCodes::BAD_ARGUMENTS);

        /// Get sorting key from engine arguments.
        ///
        /// NOTE: store merging_param_key_arg as additional key column. We do it
        /// before storage creation. After that storage will just copy this
        /// column if sorting key will be changed.
        metadata.sorting_key = KeyDescription::getSortingKeyFromAST(
            args.storage_def->order_by->ptr(), metadata.columns, args.getContext(), merging_param_key_arg);

        /// If primary key explicitly defined, than get it from AST
        if (args.storage_def->primary_key)
        {
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
        }
        else /// Otherwise we don't have explicit primary key and copy it from order by
        {
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->order_by->ptr(), metadata.columns, args.getContext());
            /// and set it's definition_ast to nullptr (so isPrimaryKeyDefined()
            /// will return false but hasPrimaryKey() will return true.
            metadata.primary_key.definition_ast = nullptr;
        }

        auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
        auto primary_key_asts = metadata.primary_key.expression_list_ast->children;
        metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
            args.columns, metadata.partition_key.expression_list_ast, minmax_columns, primary_key_asts, args.getContext()));

        if (args.storage_def->sample_by)
            metadata.sampling_key = KeyDescription::getKeyFromAST(args.storage_def->sample_by->ptr(), metadata.columns, args.getContext());

        if (args.storage_def->ttl_table)
        {
            metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
                args.storage_def->ttl_table->ptr(), metadata.columns, args.getContext(), metadata.primary_key);
        }

        if (args.query.columns_list && args.query.columns_list->indices)
            for (auto & index : args.query.columns_list->indices->children)
                metadata.secondary_indices.push_back(IndexDescription::getIndexFromAST(index, columns, args.getContext()));

        if (args.query.columns_list && args.query.columns_list->projections)
            for (auto & projection_ast : args.query.columns_list->projections->children)
            {
                auto projection = ProjectionDescription::getProjectionFromAST(projection_ast, columns, args.getContext());
                metadata.projections.add(std::move(projection));
            }

        auto constraints = metadata.constraints.getConstraints();
        if (args.query.columns_list && args.query.columns_list->constraints)
            for (auto & constraint : args.query.columns_list->constraints->children)
                constraints.push_back(constraint);
        metadata.constraints = ConstraintsDescription(constraints);

        auto column_ttl_asts = columns.getColumnTTLs();
        for (const auto & [name, ast] : column_ttl_asts)
        {
            auto new_ttl_entry = TTLDescription::getTTLFromAST(ast, columns, args.getContext(), metadata.primary_key);
            metadata.column_ttls_by_name[name] = new_ttl_entry;
        }

        storage_settings->loadFromQuery(*args.storage_def);

        // updates the default storage_settings with settings specified via SETTINGS arg in a query
        if (args.storage_def->settings)
            metadata.settings_changes = args.storage_def->settings->ptr();
    }
    else
    {
        /// Syntax: *MergeTree(..., date, [sample_key], primary_key, index_granularity, ...)
        /// Get date:
        if (!tryGetIdentifierNameInto(engine_args[arg_num], date_column_name))
            throw Exception(
                "Date column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);

        auto partition_by_ast = makeASTFunction("toYYYYMM", std::make_shared<ASTIdentifier>(date_column_name));

        metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_ast, metadata.columns, args.getContext());

        ++arg_num;

        /// If there is an expression for sampling
        if (arg_cnt - arg_num == 3)
        {
            metadata.sampling_key = KeyDescription::getKeyFromAST(engine_args[arg_num], metadata.columns, args.getContext());
            ++arg_num;
        }

        /// Get sorting key from engine arguments.
        ///
        /// NOTE: store merging_param_key_arg as additional key column. We do it
        /// before storage creation. After that storage will just copy this
        /// column if sorting key will be changed.
        metadata.sorting_key
            = KeyDescription::getSortingKeyFromAST(engine_args[arg_num], metadata.columns, args.getContext(), merging_param_key_arg);

        /// In old syntax primary_key always equals to sorting key.
        metadata.primary_key = KeyDescription::getKeyFromAST(engine_args[arg_num], metadata.columns, args.getContext());
        /// But it's not explicitly defined, so we evaluate definition to
        /// nullptr
        metadata.primary_key.definition_ast = nullptr;

        ++arg_num;

        auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
        auto primary_key_asts = metadata.primary_key.expression_list_ast->children;
        metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
            args.columns, metadata.partition_key.expression_list_ast, minmax_columns, primary_key_asts, args.getContext()));

        const auto * ast = engine_args[arg_num]->as<ASTLiteral>();
        if (ast && ast->value.getType() == Field::Types::UInt64)
            storage_settings->index_granularity = safeGet<UInt64>(ast->value);
        else
            throw Exception(
                "Index granularity must be a positive integer" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);
        ++arg_num;

        if (args.storage_def->ttl_table && !args.attach)
            throw Exception("Table TTL is not allowed for MergeTree in old syntax", ErrorCodes::BAD_ARGUMENTS);
    }

    DataTypes data_types = metadata.partition_key.data_types;
    if (!args.attach && !storage_settings->allow_floating_point_partition_key)
    {
        for (size_t i = 0; i < data_types.size(); ++i)
            if (isFloat(data_types[i]))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Floating point partition key is not supported: {}", metadata.partition_key.column_names[i]);
    }

    if (arg_num != arg_cnt)
        throw Exception("Wrong number of engine arguments.", ErrorCodes::BAD_ARGUMENTS);

    if (replicated)
        return std::make_shared<StorageReplicatedMergeTree>(
            zookeeper_path,
            replica_name,
            args.attach,
            args.table_id,
            args.relative_data_path,
            metadata,
            args.getContext(),
            date_column_name,
            merging_params,
            std::move(storage_settings),
            args.has_force_restore_data_flag,
            renaming_restrictions);
    else
        return std::make_shared<StorageMergeTree>(
            args.table_id,
            args.relative_data_path,
            metadata,
            args.attach,
            args.getContext(),
            date_column_name,
            merging_params,
            std::move(storage_settings),
            args.has_force_restore_data_flag);
}


void registerStorageMergeTree(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_skipping_indices = true,
        .supports_projections = true,
        .supports_sort_order = true,
        .supports_ttl = true,
        .supports_parallel_insert = true,
    };

    factory.registerStorage("MergeTree", create, features);
    factory.registerStorage("CollapsingMergeTree", create, features);
    factory.registerStorage("ReplacingMergeTree", create, features);
    factory.registerStorage("AggregatingMergeTree", create, features);
    factory.registerStorage("SummingMergeTree", create, features);
    factory.registerStorage("GraphiteMergeTree", create, features);
    factory.registerStorage("VersionedCollapsingMergeTree", create, features);

    features.supports_replication = true;
    features.supports_deduplication = true;
    features.supports_schema_inference = true;

    factory.registerStorage("ReplicatedMergeTree", create, features);
    factory.registerStorage("ReplicatedCollapsingMergeTree", create, features);
    factory.registerStorage("ReplicatedReplacingMergeTree", create, features);
    factory.registerStorage("ReplicatedAggregatingMergeTree", create, features);
    factory.registerStorage("ReplicatedSummingMergeTree", create, features);
    factory.registerStorage("ReplicatedGraphiteMergeTree", create, features);
    factory.registerStorage("ReplicatedVersionedCollapsingMergeTree", create, features);
}

}
