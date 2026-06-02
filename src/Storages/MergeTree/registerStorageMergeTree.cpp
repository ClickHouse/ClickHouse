#include <Databases/DatabaseReplicatedHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexMinMax.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/extractZooKeeperPathFromReplicatedTableDef.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/TableZnodeInfo.h>

#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Common/Jemalloc.h>
#include <Common/JemallocMergeTreeArena.h>
#include <Common/Macros.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/StatisticsDescription.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Disks/DiskType.h>
#include <Disks/IDisk.h>
#include <Disks/StoragePolicy.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/DDLTask.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_deprecated_syntax_for_merge_tree;
    extern const SettingsBool allow_experimental_unique_key;
    extern const SettingsBool allow_suspicious_primary_key;
    extern const SettingsBool allow_suspicious_ttl_expressions;
    extern const SettingsBool create_table_empty_primary_key_by_default;
    extern const SettingsUInt64 database_replicated_allow_replicated_engine_arguments;
    extern const SettingsUInt64 keeper_max_retries;
    extern const SettingsUInt64 keeper_retry_initial_backoff_ms;
    extern const SettingsUInt64 keeper_retry_max_backoff_ms;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_floating_point_partition_key;
    extern const MergeTreeSettingsDeduplicateMergeProjectionMode deduplicate_merge_projection_mode;
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsBool add_minmax_index_for_numeric_columns;
    extern const MergeTreeSettingsBool add_minmax_index_for_string_columns;
    extern const MergeTreeSettingsBool add_minmax_index_for_temporal_columns;
    extern const MergeTreeSettingsBool add_minmax_index_for_block_number_column;
    extern const MergeTreeSettingsBool add_minmax_index_for_block_offset_column;
    extern const MergeTreeSettingsBool enable_block_number_column;
    extern const MergeTreeSettingsBool enable_block_offset_column;
    extern const MergeTreeSettingsString auto_statistics_types;
    extern const MergeTreeSettingsBool escape_index_filenames;
    extern const MergeTreeSettingsString disk;
    extern const MergeTreeSettingsString storage_policy;
}

namespace ServerSetting
{
    extern const ServerSettingsString default_replica_name;
    extern const ServerSettingsString default_replica_path;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_STORAGE;
    extern const int NO_REPLICA_NAME_GIVEN;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int SUPPORT_IS_DISABLED;
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

    return {getIdentifierName(node)};
}

constexpr auto verbose_help_message = R"(

Syntax for the MergeTree table engine:

CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
    INDEX index_name1 expr1 TYPE type1(...) [GRANULARITY value1],
    INDEX index_name2 expr2 TYPE type2(...) [GRANULARITY value2]
) ENGINE = MergeTree()
ORDER BY expr
[PARTITION BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'], ...]
[SETTINGS name=value, ...]
[COMMENT 'comment']

See details in documentation: https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree/. Other engines of the family support different syntax, see details in the corresponding documentation topics.

If you use the Replicated version of engines, see https://clickhouse.com/docs/engines/table-engines/mergetree-family/replication/.
)";

static ColumnsDescription getColumnsDescriptionFromZookeeper(const TableZnodeInfo & zookeeper_info, ContextMutablePtr context)
{
    zkutil::ZooKeeperPtr zookeeper;
    try
    {
        zookeeper = context->getDefaultOrAuxiliaryZooKeeper(zookeeper_info.zookeeper_name);
    }
    catch (...)
    {
        throw Exception{ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot get replica structure from zookeeper, because cannot get zookeeper: {}. You must specify structure manually", getCurrentExceptionMessage(false)};
    }

    if (!zookeeper->exists(zookeeper_info.path + "/replicas"))
        throw Exception{ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot get replica structure, because there no other replicas in zookeeper. You must specify the structure manually"};

    Coordination::Stat columns_stat;
    return ColumnsDescription::parse(zookeeper->get(fs::path(zookeeper_info.path) / "columns", &columns_stat));
}

/// Returns whether a new syntax is used to define a table engine, i.e. MergeTree() PRIMARY KEY ... PARTITION BY ... SETTINGS ...
/// instead of MergeTree(MergeTree(date, [sample_key], primary_key).
static bool isExtendedStorageDef(const ASTCreateQuery & query)
{
    if (query.storage && query.storage->isExtendedStorageDefinition())
        return true;

    if (query.columns_list &&
        ((query.columns_list->indices && !query.columns_list->indices->children.empty()) ||
         (query.columns_list->projections && !query.columns_list->projections->children.empty())))
    {
        return true;
    }

    return false;
}

/// Evaluates expressions in engine arguments.
/// In new syntax an argument can be literal or identifier or array/tuple of identifiers.
static void evaluateEngineArgs(ASTs & engine_args, const ContextPtr & context)
{
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
            Field value = evaluateConstantExpression(arg, context).first;
            arg = make_intrusive<ASTLiteral>(value);
        }
    }
    catch (Exception & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot evaluate engine argument {}: {} {}",
                        arg_idx, e.message(), verbose_help_message);
    }
}

/// Returns whether this is a Replicated table engine?
static bool isReplicated(const String & engine_name)
{
    return engine_name.starts_with("Replicated") && engine_name.ends_with("MergeTree");
}

/// Returns the part of the name of a table engine between "Replicated" (if any) and "MergeTree".
static std::string_view getNamePart(const String & engine_name)
{
    std::string_view name_part = engine_name;
    if (name_part.starts_with("Replicated"))
        name_part.remove_prefix(strlen("Replicated"));

    if (name_part.ends_with("MergeTree"))
        name_part.remove_suffix(strlen("MergeTree"));

    return name_part;
}

/// Extracts zookeeper path and replica name from the table engine's arguments.
/// The function can modify those arguments (that's why they're passed separately in `engine_args`) and also determines RenamingRestrictions.
/// The function assumes the table engine is Replicated.
static TableZnodeInfo extractZooKeeperPathAndReplicaNameFromEngineArgs(
    const ASTCreateQuery & query,
    const StorageID & table_id,
    const String & engine_name,
    ASTs & engine_args,
    LoadingStrictnessLevel mode,
    const ContextPtr & local_context)
{
    chassert(isReplicated(engine_name));

    bool is_extended_storage_def = engine_args.empty() || isExtendedStorageDef(query);

    if (is_extended_storage_def)
    {
        /// Allow expressions in engine arguments.
        /// In new syntax argument can be literal or identifier or array/tuple of identifiers.
        evaluateEngineArgs(engine_args, local_context);
    }

    auto expand_macro = [&] (ASTLiteral * ast_zk_path, ASTLiteral * ast_replica_name, String zookeeper_path, String replica_name) -> TableZnodeInfo
    {
        TableZnodeInfo res = TableZnodeInfo::resolve(zookeeper_path, replica_name, table_id, query, mode, local_context);
        ast_zk_path->value = res.full_path_for_metadata;
        ast_replica_name->value = res.replica_name_for_metadata;
        return res;
    };

    size_t arg_num = 0;
    size_t arg_cnt = engine_args.size();

    bool has_arguments = (arg_num + 2 <= arg_cnt);
    bool has_valid_arguments = has_arguments && engine_args[arg_num]->as<ASTLiteral>() && engine_args[arg_num + 1]->as<ASTLiteral>();
    const auto & server_settings = local_context->getServerSettings();

    if (has_valid_arguments)
    {
        bool is_replicated_database = local_context->isDDLOrOnClusterInternal() &&
            DatabaseCatalog::instance().getDatabase(table_id.database_name)->getEngineName() == "Replicated";

        /// Get path and name from engine arguments
        auto * ast_zk_path = engine_args[arg_num]->as<ASTLiteral>();
        if (!ast_zk_path || ast_zk_path->value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path in ZooKeeper must be a string literal{}", verbose_help_message);

        auto * ast_replica_name = engine_args[arg_num + 1]->as<ASTLiteral>();
        if (!ast_replica_name || ast_replica_name->value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica name must be a string literal{}", verbose_help_message);

        if (!query.attach && is_replicated_database
            && local_context->getSettingsRef()[Setting::database_replicated_allow_replicated_engine_arguments] == 0)
        {
            /// Allow specifying engine arguments even with database_replicated_allow_replicated_engine_arguments=0 (not allowed) but only
            /// throw an error if the specified replica path or replica name is not the same as the default values.
            /// For example, for queries like `CREATE t1 TABLE AS t2 ..` the table structure of t1 is used to create table t2.
            /// In such cases, the engine arguments might already be present in the create query but exceptions should not be thrown
            /// when using database_replicated_allow_replicated_engine_arguments=0.
            if (ast_zk_path->value.safeGet<String>() != server_settings[ServerSetting::default_replica_path].toString()
                || ast_replica_name->value.safeGet<String>() != server_settings[ServerSetting::default_replica_name].toString())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "It's not allowed to specify explicit zookeeper_path and replica_name "
                    "for ReplicatedMergeTree arguments in Replicated database. If you really want to "
                    "specify them explicitly, enable setting "
                    "database_replicated_allow_replicated_engine_arguments.");
        }
        if (!query.attach && is_replicated_database && local_context->getSettingsRef()[Setting::database_replicated_allow_replicated_engine_arguments] == 1)
        {
            LOG_WARNING(
                &Poco::Logger::get("registerStorageMergeTree"),
                "It's not recommended to explicitly specify "
                "zookeeper_path and replica_name in ReplicatedMergeTree arguments");
        }

        if (!query.attach && is_replicated_database && local_context->getSettingsRef()[Setting::database_replicated_allow_replicated_engine_arguments] == 2)
        {
            LOG_WARNING(&Poco::Logger::get("registerStorageMergeTree"), "Replacing user-provided ZooKeeper path and replica name ({}, {}) "
                                                                     "with default arguments", ast_zk_path->value.safeGet<String>(), ast_replica_name->value.safeGet<String>());
            ast_zk_path->value = server_settings[ServerSetting::default_replica_path];
            ast_replica_name->value = server_settings[ServerSetting::default_replica_name];
        }

        return expand_macro(ast_zk_path, ast_replica_name, ast_zk_path->value.safeGet<String>(), ast_replica_name->value.safeGet<String>());
    }
    if (is_extended_storage_def
        && (arg_cnt == 0
            || !engine_args[arg_num]->as<ASTLiteral>()
            || (arg_cnt == 1 && (getNamePart(engine_name) == "Graphite"))))
    {
        /// Try use default values if arguments are not specified.
        /// Note: {uuid} macro works for ON CLUSTER queries when database engine is Atomic.
        /// TODO maybe use hostname if {replica} is not defined?

        /// Modify query, so default values will be written to metadata
        chassert(arg_num == 0);
        ASTs old_args;
        std::swap(engine_args, old_args);
        auto path_arg = make_intrusive<ASTLiteral>("");
        auto name_arg = make_intrusive<ASTLiteral>("");
        auto * ast_zk_path = path_arg.get();
        auto * ast_replica_name = name_arg.get();

        auto res = expand_macro(ast_zk_path, ast_replica_name, server_settings[ServerSetting::default_replica_path], server_settings[ServerSetting::default_replica_name]);

        engine_args.emplace_back(std::move(path_arg));
        engine_args.emplace_back(std::move(name_arg));
        std::move(std::begin(old_args), std::end(old_args), std::back_inserter(engine_args));

        return res;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected two string literal arguments: zookeeper_path and replica_name");
}

/// Extracts a zookeeper path from a specified CREATE TABLE query.
std::optional<String> extractZooKeeperPathFromReplicatedTableDef(const ASTCreateQuery & query, const ContextPtr & local_context)
{
    if (!query.storage || !query.storage->engine)
        return {};

    const String & engine_name = query.storage->engine->name;
    if (!isReplicated(engine_name))
        return {};

    StorageID table_id{query.getDatabase(), query.getTable(), query.uuid};

    ASTs engine_args;
    if (query.storage->engine->arguments)
        engine_args = query.storage->engine->arguments->children;
    for (auto & engine_arg : engine_args)
        engine_arg = engine_arg->clone();

    try
    {
        auto res = extractZooKeeperPathAndReplicaNameFromEngineArgs(
            query, table_id, engine_name, engine_args, LoadingStrictnessLevel::CREATE, local_context);
        return res.full_path;
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::BAD_ARGUMENTS)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Couldn't evaluate engine arguments");
            return {};
        }
        throw;
    }
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
        * ReplacingMergeTree(date, [sample_key], primary_key, index_granularity, [version_column [, is_deleted_column]])
        * GraphiteMergeTree(date, [sample_key], primary_key, index_granularity, 'config_element')
        *
        * Alternatively, you can specify:
        *  - Partitioning expression in the PARTITION BY clause;
        *  - Sorting key in the ORDER BY clause;
        *  - Primary key (if it is different from the sorting key) in the PRIMARY KEY clause;
        *  - Sampling expression in the SAMPLE BY clause;
        *  - Additional MergeTreeSettings in the SETTINGS clause;
        */

    auto component_guard = Coordination::setCurrentComponent("registerStorageMergeTree::create");

    /// Route every long-lived allocation built up while constructing this MergeTree-family
    /// storage into the dedicated MergeTree arena. This covers the `MergeTreeSettings` deep
    /// copy a few dozen lines below (`make_unique<MergeTreeSettings>(*initial_storage_settings)`),
    /// the `StorageInMemoryMetadata` populated here (columns, partition / sorting / sampling
    /// keys, secondary indices, projections, TTLs, comment), the `MergingParams` constructed
    /// for {Summing,Replacing,...}MergeTree, and the AST clones for keys/expressions stored
    /// on the resulting metadata. All of these are subsequently moved into the storage object
    /// (see the `make_shared<Storage{Replicated,}MergeTree>(... metadata, ... storage_settings)`
    /// at the bottom) and live for the table's lifetime. The deeper `MergeTreeData::setProperties`
    /// has its own `ScopedJemallocThreadArena`; nesting is a no-op (RAII saves/restores the
    /// previous index), so this does not break that wrapping.
    ScopedJemallocThreadArena mergetree_arena_scope(JemallocMergeTreeArena::getArenaIndex());

    bool is_extended_storage_def = args.engine_args.empty() || isExtendedStorageDef(args.query);

    const Settings & local_settings = args.getLocalContext()->getSettingsRef();

    bool replicated = isReplicated(args.engine_name);
    std::string_view name_part = getNamePart(args.engine_name);

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
    else if (name_part == "Coalescing")
        merging_params.mode = MergeTreeData::MergingParams::Coalescing;
    else if (name_part == "Graphite")
        merging_params.mode = MergeTreeData::MergingParams::Graphite;
    else if (name_part == "VersionedCollapsing")
        merging_params.mode = MergeTreeData::MergingParams::VersionedCollapsing;
    else if (!name_part.empty())
        throw Exception(ErrorCodes::UNKNOWN_STORAGE, "Unknown storage {}",
            args.engine_name + verbose_help_message);

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
        needed_params += needed_params.empty() ? "\n[" : ",\n[";
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
            add_optional_param("is_deleted column");
            add_optional_param("version");
            break;
        case MergeTreeData::MergingParams::Coalescing:
            add_optional_param("list of columns to sum");
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
    auto context = args.getContext();
    size_t arg_num = 0;
    size_t arg_cnt = engine_args.size();

    if (arg_cnt < min_num_params || arg_cnt > max_num_params)
    {
        String msg;
        if (max_num_params == 0)
            msg += "no parameters";
        else if (min_num_params == max_num_params)
            msg += fmt::format("{} parameters: {}", min_num_params, needed_params);
        else
            msg += fmt::format("{} to {} parameters: {}", min_num_params, max_num_params, needed_params);

        if (is_extended_storage_def)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "With extended storage definition syntax storage {} requires {}{}",
                            args.engine_name, msg, verbose_help_message);
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "ORDER BY or PRIMARY KEY clause is missing. "
            "Consider using extended storage definition syntax with ORDER BY or PRIMARY KEY clause. "
            "With deprecated old syntax (highly not recommended) storage {} requires {}{}",
            args.engine_name,
            msg,
            verbose_help_message);
    }

    if (is_extended_storage_def)
    {
        /// Allow expressions in engine arguments.
        /// In new syntax argument can be literal or identifier or array/tuple of identifiers.
        evaluateEngineArgs(engine_args, args.getLocalContext());
    }
    else if (args.mode <= LoadingStrictnessLevel::CREATE && !local_settings[Setting::allow_deprecated_syntax_for_merge_tree])
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "This syntax for *MergeTree engine is deprecated. "
                                                   "Use extended storage definition syntax with ORDER BY/PRIMARY KEY clause. "
                                                   "See also `allow_deprecated_syntax_for_merge_tree` setting.");
    }

    /// Extract zookeeper path and replica name from engine arguments.
    TableZnodeInfo zookeeper_info;

    if (replicated)
    {
        zookeeper_info = extractZooKeeperPathAndReplicaNameFromEngineArgs(
            args.query, args.table_id, args.engine_name, args.engine_args, args.mode, args.getLocalContext());

        if (zookeeper_info.replica_name.empty())
            throw Exception(ErrorCodes::NO_REPLICA_NAME_GIVEN, "No replica name in config{}", verbose_help_message);
        // '\t' and '\n' will interrupt parsing 'source replica' in ReplicatedMergeTreeLogEntryData::readText
        if (zookeeper_info.replica_name.contains('\t') || zookeeper_info.replica_name.contains('\n'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica name must not contain '\\t' or '\\n'");

        arg_cnt = engine_args.size(); /// Update `arg_cnt` here because extractZooKeeperPathAndReplicaNameFromEngineArgs() could add arguments.
        arg_num = 2;                  /// zookeeper_path and replica_name together are always two arguments.
    }

    /// This merging param maybe used as part of sorting key
    std::optional<String> merging_param_key_arg;

    if (merging_params.mode == MergeTreeData::MergingParams::Collapsing)
    {
        if (!tryGetIdentifierNameInto(engine_args[arg_cnt - 1], merging_params.sign_column))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sign column name must be an unquoted string{}", verbose_help_message);
        --arg_cnt;
    }
    else if (merging_params.mode == MergeTreeData::MergingParams::Replacing)
    {
        // if there is args and number of optional parameter is higher than 1
        // is_deleted is not allowed with the 'allow_deprecated_syntax_for_merge_tree' settings
        if (arg_cnt - arg_num == 2 && !engine_args[arg_cnt - 1]->as<ASTLiteral>() && is_extended_storage_def)
        {
            if (!tryGetIdentifierNameInto(engine_args[arg_cnt - 1], merging_params.is_deleted_column))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "is_deleted column name must be an identifier {}", verbose_help_message);
            --arg_cnt;
        }

        /// If the last element is not index_granularity or replica_name (a literal), then this is the name of the version column.
        if (arg_cnt && !engine_args[arg_cnt - 1]->as<ASTLiteral>())
        {
            if (!tryGetIdentifierNameInto(engine_args[arg_cnt - 1], merging_params.version_column))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Version column name must be an identifier {}", verbose_help_message);
            --arg_cnt;
        }
    }
    else if (merging_params.mode == MergeTreeData::MergingParams::Summing || merging_params.mode == MergeTreeData::MergingParams::Coalescing)
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
        constexpr auto format_str = "Last parameter of GraphiteMergeTree must be the name (in single quotes) of the element "
                                    "in configuration file with the Graphite options{}";
        String error_msg = verbose_help_message;

        if (const auto * ast = engine_args[arg_cnt - 1]->as<ASTLiteral>())
        {
            if (ast->value.getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, format_str, error_msg);

            graphite_config_name = ast->value.safeGet<String>();
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, format_str, error_msg);

        --arg_cnt;
        setGraphitePatternsFromConfig(context, graphite_config_name, merging_params.graphite_params);
    }
    else if (merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing)
    {
        if (!tryGetIdentifierNameInto(engine_args[arg_cnt - 1], merging_params.version_column))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Version column name must be an unquoted string{}", verbose_help_message);

        --arg_cnt;

        if (!tryGetIdentifierNameInto(engine_args[arg_cnt - 1], merging_params.sign_column))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sign column name must be an unquoted string{}", verbose_help_message);

        --arg_cnt;
        /// Version collapsing is the only engine which add additional column to
        /// sorting key.
        merging_param_key_arg = merging_params.version_column;
    }

    String date_column_name;

    StorageInMemoryMetadata metadata;

    ColumnsDescription columns;
    if (args.columns.empty() && replicated)
        columns = getColumnsDescriptionFromZookeeper(zookeeper_info, context);
    else
        columns = args.columns;

    metadata.setColumns(columns);
    metadata.setComment(args.comment);

    const auto & initial_storage_settings = replicated ? context->getReplicatedMergeTreeSettings() : context->getMergeTreeSettings();
    std::unique_ptr<MergeTreeSettings> storage_settings = std::make_unique<MergeTreeSettings>(initial_storage_settings);

    if (is_extended_storage_def)
    {
        ASTPtr partition_by_key;
        if (args.storage_def->partition_by)
            partition_by_key = args.storage_def->partition_by->ptr();

        /// Partition key may be undefined, but despite this we store it's empty
        /// value in partition_key structure. MergeTree checks this case and use
        /// single default partition with name "all".
        metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_key, metadata.columns, MergeTreeData::createVirtuals(nullptr), context);
        metadata.virtuals = MergeTreeData::createVirtuals(&metadata.partition_key);

        /// PRIMARY KEY without ORDER BY is allowed and considered as ORDER BY.
        if (!args.storage_def->order_by && args.storage_def->primary_key)
            args.storage_def->set(args.storage_def->order_by, args.storage_def->primary_key->clone());

        if (!args.storage_def->order_by)
        {
            if (local_settings[Setting::create_table_empty_primary_key_by_default])
            {
                args.storage_def->set(args.storage_def->order_by, makeASTOperator("tuple"));
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "You must provide an ORDER BY or PRIMARY KEY expression in the table definition. "
                                "If you don't want this table to be sorted, use ORDER BY/PRIMARY KEY (). "
                                "Otherwise, you can use the setting 'create_table_empty_primary_key_by_default' to "
                                "automatically add an empty primary key to the table definition");
            }
        }

        /// Get sorting key from engine arguments.
        ///
        /// NOTE: store merging_param_key_arg as additional key column. We do it
        /// before storage creation. After that storage will just copy this
        /// column if sorting key will be changed.
        NamesAndTypesList additional_columns;
        if (merging_param_key_arg)
            additional_columns.emplace_back(*merging_param_key_arg, metadata.columns.getPhysical(*merging_param_key_arg).type);

        metadata.sorting_key = KeyDescription::getKeyFromAST(args.storage_def->order_by->ptr(), metadata.columns, metadata.virtuals, context, additional_columns);

        if (!local_settings[Setting::allow_suspicious_primary_key] && args.mode <= LoadingStrictnessLevel::CREATE)
            MergeTreeData::verifySortingKey(metadata.sorting_key);

        /// If primary key explicitly defined, than get it from AST
        if (args.storage_def->primary_key)
        {
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, metadata.virtuals, context);
        }
        else /// Otherwise we don't have explicit primary key and copy it from order by.
        {
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->order_by->ptr(), metadata.columns, metadata.virtuals, context);
            /// and set it's definition_ast to nullptr (so isPrimaryKeyDefined()
            /// will return false but hasPrimaryKey() will return true.
            metadata.primary_key.definition_ast = nullptr;
        }

        auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
        auto partition_key = metadata.partition_key.expression_list_ast->clone();
        FunctionNameNormalizer::visit(partition_key.get());
        metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
            columns, partition_key, minmax_columns, metadata.primary_key, &metadata.partition_key, context));

        if (args.storage_def->sample_by)
            metadata.sampling_key = KeyDescription::getKeyFromAST(args.storage_def->sample_by->ptr(), metadata.columns, metadata.virtuals, context);

        if (args.storage_def->unique_key)
        {
            /// Gate on CREATE only; ATTACH must load existing metadata regardless of session setting.
            if (args.mode <= LoadingStrictnessLevel::CREATE
                && !local_settings[Setting::allow_experimental_unique_key])
            {
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                    "UNIQUE KEY is an experimental feature. "
                    "Set the session setting `allow_experimental_unique_key = 1` to enable it.");
            }

            /// Reject expression-style elements at parse time: runtime consumers
            /// look up keys via `block.getByName(<column name>)`, so an
            /// expression-style UK passes DDL but crashes the first INSERT.
            {
                const ASTPtr & uk_ast = args.storage_def->unique_key->ptr();
                auto is_plain_identifier = [](const ASTPtr & node) -> bool
                {
                    return node && node->as<ASTIdentifier>() != nullptr;
                };

                const auto * as_function = uk_ast->as<ASTFunction>();
                if (as_function && as_function->name == "tuple")
                {
                    if (as_function->arguments)
                    {
                        for (const auto & child : as_function->arguments->children)
                        {
                            if (!is_plain_identifier(child))
                            {
                                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "UNIQUE KEY must be a list of column identifiers. "
                                    "Expression-style elements such as `{}` are not yet supported; "
                                    "only bare column names are allowed.",
                                    child ? child->formatForErrorMessage() : String("<null>"));
                            }
                        }
                    }
                }
                else if (!is_plain_identifier(uk_ast))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "UNIQUE KEY must be a list of column identifiers. "
                        "Expression-style keys such as `{}` are not yet supported; "
                        "only bare column names are allowed.",
                        uk_ast->formatForErrorMessage());
                }
            }

            metadata.unique_key = KeyDescription::getKeyFromAST(
                args.storage_def->unique_key->ptr(), metadata.columns, metadata.virtuals, context);

            if (metadata.unique_key.column_names.empty())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "UNIQUE KEY must contain at least one column");
            }

            NameSet seen_uk_columns;
            for (const auto & uk_column : metadata.unique_key.column_names)
            {
                if (!seen_uk_columns.insert(uk_column).second)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "UNIQUE KEY contains duplicate column `{}`",
                        uk_column);
            }
        }

        bool allow_suspicious_ttl
            = LoadingStrictnessLevel::SECONDARY_CREATE <= args.mode || local_settings[Setting::allow_suspicious_ttl_expressions];

        if (args.storage_def->ttl_table)
        {
            metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
                args.storage_def->ttl_table->ptr(), metadata.columns, context, metadata.primary_key, allow_suspicious_ttl);
        }

        /// We use the local (query) context here so that user-level settings profiles can control
        /// access to dynamic disk features such as `from_env`, `include`, and `from_zk`
        /// (settings `dynamic_disk_allow_from_env`, `dynamic_disk_allow_include`, `dynamic_disk_allow_from_zk`).
        /// When loading from existing metadata (`FORCE_ATTACH` / `FORCE_RESTORE`, e.g. server
        /// restart or `UNDROP TABLE`), security checks inside `getDiskConfigurationFromAST` are
        /// intentionally skipped because the disk configuration was already validated when the
        /// table was originally created.
        /// User-initiated `ATTACH TABLE` queries use `LoadingStrictnessLevel::ATTACH` and must
        /// still be subject to these checks.
        storage_settings->loadFromQuery(*args.storage_def, args.getLocalContext(), isLoadingFromExistingMetadata(args.mode));

        /// Updates the default storage_settings with settings specified via SETTINGS arg in a query
        if (args.storage_def->settings)
        {
            if (args.mode <= LoadingStrictnessLevel::CREATE)
                args.getLocalContext()->checkMergeTreeSettingsConstraints(initial_storage_settings, storage_settings->changes());
            metadata.settings_changes = args.storage_def->settings->ptr();
        }

        /// UNIQUE KEY tables must reside on local-only storage policies.
        /// CREATE only; ATTACH must still load existing tables.
        if (metadata.hasUniqueKey() && args.mode <= LoadingStrictnessLevel::CREATE)
        {
            StoragePolicyPtr resolved_storage_policy = (*storage_settings)[MergeTreeSetting::disk].changed
                ? context->getStoragePolicyFromDisk((*storage_settings)[MergeTreeSetting::disk])
                : context->getStoragePolicy((*storage_settings)[MergeTreeSetting::storage_policy]);

            for (const auto & disk : resolved_storage_policy->getDisks())
            {
                const auto & desc = disk->getDataSourceDescription();
                if (desc.type != DataSourceType::Local)
                {
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                        "UNIQUE KEY on non-local disks is not yet supported "
                        "(disk `{}` has source type `{}`). "
                        "UNIQUE KEY tables must currently reside on a local-only storage policy.",
                        disk->getName(), desc.toString());
                }
            }
        }

        metadata.add_minmax_index_for_numeric_columns = (*storage_settings)[MergeTreeSetting::add_minmax_index_for_numeric_columns];
        metadata.add_minmax_index_for_string_columns = (*storage_settings)[MergeTreeSetting::add_minmax_index_for_string_columns];
        metadata.add_minmax_index_for_temporal_columns = (*storage_settings)[MergeTreeSetting::add_minmax_index_for_temporal_columns];
        metadata.add_minmax_index_for_block_number_column = (*storage_settings)[MergeTreeSetting::add_minmax_index_for_block_number_column] && (*storage_settings)[MergeTreeSetting::enable_block_number_column];
        metadata.add_minmax_index_for_block_offset_column = (*storage_settings)[MergeTreeSetting::add_minmax_index_for_block_offset_column] && (*storage_settings)[MergeTreeSetting::enable_block_offset_column];
        metadata.escape_index_filenames = (*storage_settings)[MergeTreeSetting::escape_index_filenames];
        if (args.query.columns_list && args.query.columns_list->indices)
        {
            for (const auto & index : args.query.columns_list->indices->children)
            {
                metadata.secondary_indices.push_back(IndexDescription::getIndexFromAST(index, columns, /* is_implicitly_created */ false, metadata.escape_index_filenames, context));
                auto index_name = index->as<ASTIndexDeclaration>()->name;

                auto using_auto_minmax_index =
                       metadata.add_minmax_index_for_numeric_columns
                    || metadata.add_minmax_index_for_string_columns
                    || metadata.add_minmax_index_for_temporal_columns
                    || metadata.add_minmax_index_for_block_number_column
                    || metadata.add_minmax_index_for_block_offset_column;
                if (using_auto_minmax_index && index_name.starts_with(IMPLICITLY_ADDED_MINMAX_INDEX_PREFIX))
                {
                    if (args.mode <= LoadingStrictnessLevel::CREATE)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create table because index {} uses a reserved index name", index_name);

                    /// Backward compatibility: older versions (before 25.12) stored implicit indices
                    /// on disk as regular indices. Re-mark them so `explicitToString` excludes them.
                    auto & added = metadata.secondary_indices.back();
                    String col_name = index_name.substr(strlen(IMPLICITLY_ADDED_MINMAX_INDEX_PREFIX));
                    if (columns.has(col_name))
                    {
                        const auto & col_type = columns.get(col_name).type;
                        if ((metadata.add_minmax_index_for_numeric_columns && isNumber(col_type))
                            || (metadata.add_minmax_index_for_string_columns && isString(col_type))
                            || (metadata.add_minmax_index_for_temporal_columns && isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64(col_type)))
                            added.is_implicitly_created = true;
                    }
                }
            }
        }

        /// Try to add "implicit" min-max indexes on all columns
        for (const auto & column : metadata.columns)
        {
            metadata.addImplicitIndicesForColumn(column, context);
        }
        metadata.addImplicitIndicesForVirtualColumns(context);

        String statistics_types_str = (*storage_settings)[MergeTreeSetting::auto_statistics_types];

        if (!statistics_types_str.empty() && args.table_id.database_name != DatabaseCatalog::SYSTEM_DATABASE)
        {
            addImplicitStatistics(metadata.columns, statistics_types_str);
        }

        if (args.query.columns_list && args.query.columns_list->projections)
        {
            for (auto & projection_ast : args.query.columns_list->projections->children)
            {
                try
                {
                    auto projection = ProjectionDescription::getProjectionFromAST(projection_ast, columns, &metadata.partition_key, context, args.mode);
                    metadata.projections.add(std::move(projection));
                }
                catch (...)
                {
                    if (args.mode < LoadingStrictnessLevel::FORCE_ATTACH)
                        throw;
                    tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format(
                        "Cannot parse projection {} during server startup, skipping it. "
                        "It may be caused by a dependency on a dropped dictionary or a missing object. "
                        "Consider recreating the projection or dropping and recreating the table.",
                        projection_ast->formatForErrorMessage()));
                }
            }
        }

        auto column_ttl_asts = columns.getColumnTTLs();
        for (const auto & [name, ast] : column_ttl_asts)
        {
            auto new_ttl_entry = TTLDescription::getTTLFromAST(ast, columns, context, metadata.primary_key, allow_suspicious_ttl);
            metadata.column_ttls_by_name[name] = new_ttl_entry;
        }

        auto constraints = metadata.constraints.getConstraints();
        if (args.query.columns_list && args.query.columns_list->constraints)
            for (auto & constraint : args.query.columns_list->constraints->children)
                constraints.push_back(constraint);
        metadata.constraints = ConstraintsDescription(constraints);
    }
    else
    {
        /// Syntax: *MergeTree(..., date, [sample_key], primary_key, index_granularity, ...)
        /// Get date:
        if (!tryGetIdentifierNameInto(engine_args[arg_num], date_column_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Date column name must be an unquoted string{}", verbose_help_message);

        auto partition_by_ast = makeASTFunction("toYYYYMM", make_intrusive<ASTIdentifier>(date_column_name));
        metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_ast, metadata.columns, MergeTreeData::createVirtuals(nullptr), context);
        metadata.virtuals = MergeTreeData::createVirtuals(&metadata.partition_key);

        ++arg_num;

        /// If there is an expression for sampling
        if (arg_cnt - arg_num == 3)
        {
            metadata.sampling_key = KeyDescription::getKeyFromAST(engine_args[arg_num], metadata.columns, metadata.virtuals, context);
            ++arg_num;
        }

        /// Get sorting key from engine arguments.
        ///
        /// NOTE: store merging_param_key_arg as additional key column. We do it
        /// before storage creation. After that storage will just copy this
        /// column if sorting key will be changed.
        NamesAndTypesList additional_columns;
        if (merging_param_key_arg)
            additional_columns.emplace_back(*merging_param_key_arg, metadata.columns.getPhysical(*merging_param_key_arg).type);

        metadata.sorting_key = KeyDescription::getKeyFromAST(engine_args[arg_num], metadata.columns, metadata.virtuals, context, additional_columns);

        if (!local_settings[Setting::allow_suspicious_primary_key] && args.mode <= LoadingStrictnessLevel::CREATE)
            MergeTreeData::verifySortingKey(metadata.sorting_key);

        /// In old syntax primary_key always equals to sorting key.
        metadata.primary_key = KeyDescription::getKeyFromAST(engine_args[arg_num], metadata.columns, metadata.virtuals, context);
        /// But it's not explicitly defined, so we evaluate definition to
        /// nullptr
        metadata.primary_key.definition_ast = nullptr;

        ++arg_num;

        auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
        auto partition_key = metadata.partition_key.expression_list_ast->clone();
        FunctionNameNormalizer::visit(partition_key.get());
        metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
            columns, partition_key, minmax_columns, metadata.primary_key, &metadata.partition_key, context));

        const auto * ast = engine_args[arg_num]->as<ASTLiteral>();
        if (ast && ast->value.getType() == Field::Types::UInt64)
        {
            (*storage_settings)[MergeTreeSetting::index_granularity] = ast->value.safeGet<UInt64>();
            if (args.mode <= LoadingStrictnessLevel::CREATE)
            {
                SettingsChanges changes;
                changes.emplace_back("index_granularity", Field((*storage_settings)[MergeTreeSetting::index_granularity]));
                args.getLocalContext()->checkMergeTreeSettingsConstraints(initial_storage_settings, changes);
            }
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Index granularity must be a positive integer{}", verbose_help_message);
        ++arg_num;

        if (args.storage_def->ttl_table && args.mode <= LoadingStrictnessLevel::CREATE)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table TTL is not allowed for MergeTree in old syntax");
    }

    DataTypes data_types = metadata.partition_key.data_types;
    if (args.mode <= LoadingStrictnessLevel::CREATE && !(*storage_settings)[MergeTreeSetting::allow_floating_point_partition_key])
    {
        for (size_t i = 0; i < data_types.size(); ++i)
            if (isFloat(data_types[i]))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Floating point partition key is not supported: {}", metadata.partition_key.column_names[i]);
    }

    if (metadata.hasProjections() && args.mode == LoadingStrictnessLevel::CREATE)
    {
        /// Now let's handle the merge tree family. Note we only handle in the mode of CREATE due to backward compatibility.
        /// Otherwise, it would fail to start in the case of existing projections with special mergetree.
        if (merging_params.mode != MergeTreeData::MergingParams::Mode::Ordinary
            && (*storage_settings)[MergeTreeSetting::deduplicate_merge_projection_mode] == DeduplicateMergeProjectionMode::THROW)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Projections are not supported for {}MergeTree with deduplicate_merge_projection_mode = throw. "
                "Please set setting 'deduplicate_merge_projection_mode' to 'drop' or 'rebuild'",
                merging_params.getModeName());
    }

    if (arg_num != arg_cnt)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong number of engine arguments.");

    if (replicated)
    {
        bool need_check_table_structure = true;
        if (auto txn = args.getLocalContext()->getZooKeeperMetadataTransaction())
            need_check_table_structure = txn->isInitialQuery();

        ZooKeeperRetriesInfo create_query_zk_retries_info{
            local_settings[Setting::keeper_max_retries],
            local_settings[Setting::keeper_retry_initial_backoff_ms],
            local_settings[Setting::keeper_retry_max_backoff_ms],
            args.getLocalContext()->getProcessListElementSafe()};

        return std::make_shared<StorageReplicatedMergeTree>(
            zookeeper_info,
            args.mode,
            args.table_id,
            args.relative_data_path,
            metadata,
            context,
            date_column_name,
            merging_params,
            std::move(storage_settings),
            need_check_table_structure,
            create_query_zk_retries_info);
    }

    return std::make_shared<StorageMergeTree>(
        args.table_id,
        args.relative_data_path,
        metadata,
        args.mode,
        context,
        date_column_name,
        merging_params,
        std::move(storage_settings));
}


void registerStorageMergeTree(StorageFactory & factory);
void registerStorageMergeTree(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_skipping_indices = true,
        .supports_projections = true,
        .supports_sort_order = true,
        .supports_ttl = true,
        .supports_parallel_insert = true,
        .supports_unique_key = true,
        .has_builtin_setting_fn = MergeTreeSettings::hasBuiltin,
    };

    factory.registerStorage("MergeTree", create, features, Documentation{
        .description = String(R"DOCS_MD(
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# MergeTree table engine

The `MergeTree` engine and other engines of the `MergeTree` family (e.g. `ReplacingMergeTree`, `AggregatingMergeTree`) are the most commonly used and most robust table engines in ClickHouse.

`MergeTree`-family table engines are designed for high data ingest rates and huge data volumes.
Insert operations create table parts which are merged by a background process with other table parts.

Main features of `MergeTree`-family table engines.

- The table's primary key determines the sort order within each table part (clustered index). The primary key also does not reference individual rows but blocks of 8192 rows called granules. This makes primary keys of huge data sets small enough to remain loaded in main memory, while still providing fast access to on-disk data.

- Tables can be partitioned using an arbitrary partition expression. Partition pruning ensures partitions are omitted from reading when the query allows it.

- Data can be replicated across multiple cluster nodes for high availability, failover, and zero downtime upgrades. See [Data replication](/engines/table-engines/mergetree-family/replication.md).

- `MergeTree` table engines support various statistics kinds and sampling methods to help query optimization.

:::note
Despite a similar name, the [Merge](/engines/table-engines/special/merge) engine is different from `*MergeTree` engines.
:::

## Creating tables {#table_engine-mergetree-creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr1] [COMMENT ...] [CODEC(codec1)] [STATISTICS(stat1)] [TTL expr1] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    name2 [type2] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr2] [COMMENT ...] [CODEC(codec2)] [STATISTICS(stat2)] [TTL expr2] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    ...
    INDEX index_name1 expr1 TYPE type1(...) [GRANULARITY value1],
    INDEX index_name2 expr2 TYPE type2(...) [GRANULARITY value2],
    ...
    PROJECTION projection_name_1 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY]),
    PROJECTION projection_name_2 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY])
) ENGINE = MergeTree()
ORDER BY expr
[PARTITION BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr
    [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx' [, ...] ]
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ] ]
[SETTINGS name = value, ...]
```

For a detailed description of the parameters, see the [CREATE TABLE](/sql-reference/statements/create/table.md) statement

### Query clauses {#mergetree-query-clauses}

#### ENGINE {#engine}

`ENGINE` — Name and parameters of the engine. `ENGINE = MergeTree()`. The `MergeTree` engine has no parameters.

#### ORDER BY {#order_by}

`ORDER BY` — The sorting key.

A tuple of column names or arbitrary expressions. Example: `ORDER BY (CounterID + 1, EventDate)`.

If no primary key is defined (i.e. `PRIMARY KEY` was not specified), ClickHouse uses the the sorting key as primary key.

If no sorting is required, you can use syntax `ORDER BY tuple()`.
Alternatively, if setting `create_table_empty_primary_key_by_default` is enabled, `ORDER BY ()` is implicitly added to `CREATE TABLE` statements. See [Selecting a Primary Key](#selecting-a-primary-key).

#### PARTITION BY {#partition-by}

`PARTITION BY` — The [partitioning key](/engines/table-engines/mergetree-family/custom-partitioning-key.md). Optional. In most cases, you don't need a partition key, and if you do need to partition, generally you do not need a partition key more granular than by month. Partitioning does not speed up queries (in contrast to the ORDER BY expression). You should never use too granular partitioning. Don't partition your data by client identifiers or names (instead, make client identifier or name the first column in the ORDER BY expression).

For partitioning by month, use the `toYYYYMM(date_column)` expression, where `date_column` is a column with a date of the type [Date](/sql-reference/data-types/date.md). The partition names here have the `"YYYYMM"` format.

#### PRIMARY KEY {#primary-key}

`PRIMARY KEY` — The primary key if it [differs from the sorting key](#choosing-a-primary-key-that-differs-from-the-sorting-key). Optional.

Specifying a sorting key (using `ORDER BY` clause) implicitly specifies a primary key.
It is usually not necessary to specify the primary key in addition to the sorting key.

#### SAMPLE BY {#sample-by}

`SAMPLE BY` — A sampling expression. Optional.

If specified, it must be contained in the primary key.
The sampling expression must result in an unsigned integer.

Example: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

####  TTL {#ttl}

`TTL` — A list of rules that specify the storage duration of rows and the logic of automatic parts movement [between disks and volumes](#table_engine-mergetree-multiple-volumes). Optional.

Expression must result in a `Date` or `DateTime`, e.g. `TTL date + INTERVAL 1 DAY`.

Type of the rule `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'|GROUP BY` specifies an action to be done with the part if the expression is satisfied (reaches current time): removal of expired rows, moving a part (if expression is satisfied for all rows in a part) to specified disk (`TO DISK 'xxx'`) or to volume (`TO VOLUME 'xxx'`), or aggregating values in expired rows. Default type of the rule is removal (`DELETE`). List of multiple rules can be specified, but there should be no more than one `DELETE` rule.

For more details, see [TTL for columns and tables](#table_engine-mergetree-ttl)

#### SETTINGS {#settings}

See [MergeTree Settings](../../../operations/settings/merge-tree-settings.md).

**Example of Sections Setting**

```sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

In the example, we set partitioning by month.

We also set an expression for sampling as a hash by the user ID. This allows you to pseudorandomize the data in the table for each `CounterID` and `EventDate`. If you define a [SAMPLE](/sql-reference/statements/select/sample) clause when selecting the data, ClickHouse will return an evenly pseudorandom data sample for a subset of users.

The `index_granularity` setting can be omitted because 8192 is the default value.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::note
Do not use this method in new projects. If possible, switch old projects to the method described above.
:::

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

**MergeTree() Parameters**

- `date-column` — The name of a column of the [Date](/sql-reference/data-types/date.md) type. ClickHouse automatically creates partitions by month based on this column. The partition names are in the `"YYYYMM"` format.
- `sampling_expression` — An expression for sampling.
- `(primary, key)` — Primary key. Type: [Tuple()](/sql-reference/data-types/tuple.md)
- `index_granularity` — The granularity of an index. The number of data rows between the "marks" of an index. The value 8192 is appropriate for most tasks.

**Example**

```sql
MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
```

The `MergeTree` engine is configured in the same way as in the example above for the main engine configuration method.
</details>

## Data storage {#mergetree-data-storage}

A table consists of data parts sorted by primary key.

When data is inserted in a table, separate data parts are created and each of them is lexicographically sorted by primary key. For example, if the primary key is `(CounterID, Date)`, the data in the part is sorted by `CounterID`, and within each `CounterID`, it is ordered by `Date`.

Data belonging to different partitions are separated into different parts. In the background, ClickHouse merges data parts for more efficient storage. Parts belonging to different partitions are not merged. The merge mechanism does not guarantee that all rows with the same primary key will be in the same data part.

Data parts can be stored in `Wide` or `Compact` format. In `Wide` format each column is stored in a separate file in a filesystem, in `Compact` format all columns are stored in one file. `Compact` format can be used to increase performance of small and frequent inserts.

Data storing format is controlled by the `min_bytes_for_wide_part` and `min_rows_for_wide_part` settings of the table engine. If the number of bytes or rows in a data part is less then the corresponding setting's value, the part is stored in `Compact` format. Otherwise it is stored in `Wide` format. If none of these settings is set, data parts are stored in `Wide` format.

Each data part is logically divided into granules. A granule is the smallest indivisible data set that ClickHouse reads when selecting data. ClickHouse does not split rows or values, so each granule always contains an integer number of rows. The first row of a granule is marked with the value of the primary key for the row. For each data part, ClickHouse creates an index file that stores the marks. For each column, whether it's in the primary key or not, ClickHouse also stores the same marks. These marks let you find data directly in column files.

The granule size is restricted by the `index_granularity` and `index_granularity_bytes` settings of the table engine. The number of rows in a granule lays in the `[1, index_granularity]` range, depending on the size of the rows. The size of a granule can exceed `index_granularity_bytes` if the size of a single row is greater than the value of the setting. In this case, the size of the granule equals the size of the row.

## Primary Keys and Indexes in Queries {#primary-keys-and-indexes-in-queries}

Take the `(CounterID, Date)` primary key as an example. In this case, the sorting and index can be illustrated as follows:

```text
Whole data:     [---------------------------------------------]
CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
Marks:           |      |      |      |      |      |      |      |      |      |      |
                a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
Marks numbers:   0      1      2      3      4      5      6      7      8      9      10
```

If the data query specifies:

- `CounterID in ('a', 'h')`, the server reads the data in the ranges of marks `[0, 3)` and `[6, 8)`.
- `CounterID IN ('a', 'h') AND Date = 3`, the server reads the data in the ranges of marks `[1, 3)` and `[7, 8)`.
- `Date = 3`, the server reads the data in the range of marks `[1, 10]`.

The examples above show that it is always more effective to use an index than a full scan.

A sparse index allows extra data to be read. When reading a single range of the primary key, up to `index_granularity * 2` extra rows in each data block can be read.

Sparse indexes allow you to work with a very large number of table rows, because in most cases, such indexes fit in the computer's RAM.

ClickHouse does not require a unique primary key. You can insert multiple rows with the same primary key.

You can use `Nullable`-typed expressions in the `PRIMARY KEY` and `ORDER BY` clauses but it is strongly discouraged. To allow this feature, turn on the [allow_nullable_key](/operations/settings/merge-tree-settings/#allow_nullable_key) setting. The [NULLS_LAST](/sql-reference/statements/select/order-by.md/#sorting-of-special-values) principle applies for `NULL` values in the `ORDER BY` clause.

### Selecting a primary key {#selecting-a-primary-key}

The number of columns in the primary key is not explicitly limited. Depending on the data structure, you can include more or fewer columns in the primary key. This may:

- Improve the performance of an index.

    If the primary key is `(a, b)`, then adding another column `c` will improve the performance if the following conditions are met:

  - There are queries with a condition on column `c`.
  - Long data ranges (several times longer than the `index_granularity`) with identical values for `(a, b)` are common. In other words, when adding another column allows you to skip quite long data ranges.

- Improve data compression.

    ClickHouse sorts data by primary key, so the higher the consistency, the better the compression.

- Provide additional logic when merging data parts in the [CollapsingMergeTree](/engines/table-engines/mergetree-family/collapsingmergetree) and [SummingMergeTree](/engines/table-engines/mergetree-family/summingmergetree.md) engines.

    In this case it makes sense to specify the *sorting key* that is different from the primary key.

A long primary key will negatively affect the insert performance and memory consumption, but extra columns in the primary key do not affect ClickHouse performance during `SELECT` queries.

You can create a table without a primary key using the `ORDER BY tuple()` syntax. In this case, ClickHouse stores data in the order of inserting. If you want to save data order when inserting data by `INSERT ... SELECT` queries, set [max_insert_threads = 1](/operations/settings/settings#max_insert_threads).

To select data in the initial order, use [single-threaded](/operations/settings/settings.md/#max_threads) `SELECT` queries.

### Choosing a primary key that differs from the sorting key {#choosing-a-primary-key-that-differs-from-the-sorting-key}

It is possible to specify a primary key (an expression with values that are written in the index file for each mark) that is different from the sorting key (an expression for sorting the rows in data parts). In this case the primary key expression tuple must be a prefix of the sorting key expression tuple.

This feature is helpful when using the [SummingMergeTree](/engines/table-engines/mergetree-family/summingmergetree.md) and
[AggregatingMergeTree](/engines/table-engines/mergetree-family/aggregatingmergetree.md) table engines. In a common case when using these engines, the table has two types of columns: *dimensions* and *measures*. Typical queries aggregate values of measure columns with arbitrary `GROUP BY` and filtering by dimensions. Because SummingMergeTree and AggregatingMergeTree aggregate rows with the same value of the sorting key, it is natural to add all dimensions to it. As a result, the key expression consists of a long list of columns and this list must be frequently updated with newly added dimensions.

In this case it makes sense to leave only a few columns in the primary key that will provide efficient range scans and add the remaining dimension columns to the sorting key tuple.

[ALTER](/sql-reference/statements/alter/index.md) of the sorting key is a lightweight operation because when a new column is simultaneously added to the table and to the sorting key, existing data parts do not need to be changed. Since the old sorting key is a prefix of the new sorting key and there is no data in the newly added column, the data is sorted by both the old and new sorting keys at the moment of table modification.

### Use of indexes and partitions in queries {#use-of-indexes-and-partitions-in-queries}

For `SELECT` queries, ClickHouse analyzes whether an index can be used. An index can be used if the `WHERE/PREWHERE` clause has an expression (as one of the conjunction elements, or entirely) that represents an equality or inequality comparison operation, or if it has `IN` or `LIKE` with a fixed prefix on columns or expressions that are in the primary key or partitioning key, or on certain partially repetitive functions of these columns, or logical relationships of these expressions.

Thus, it is possible to quickly run queries on one or many ranges of the primary key. In this example, queries will be fast when run for a specific tracking tag, for a specific tag and date range, for a specific tag and date, for multiple tags with a date range, and so on.

Let's look at the engine configured as follows:
```sql
ENGINE MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity=8192
```

In this case, in queries:

```sql
SELECT count() FROM table
WHERE EventDate = toDate(now())
AND CounterID = 34

SELECT count() FROM table
WHERE EventDate = toDate(now())
AND (CounterID = 34 OR CounterID = 42)

SELECT count() FROM table
WHERE ((EventDate >= toDate('2014-01-01')
AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01'))
AND CounterID IN (101500, 731962, 160656)
AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

ClickHouse will use the primary key index to trim improper data and the monthly partitioning key to trim partitions that are in improper date ranges.

The queries above show that the index is used even for complex expressions. Reading from the table is organized so that using the index can't be slower than a full scan.

In the example below, the index can't be used.

```sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

To check whether ClickHouse can use the index when running a query, use the settings [force_index_by_date](/operations/settings/settings.md/#force_index_by_date) and [force_primary_key](/operations/settings/settings#force_primary_key).

The key for partitioning by month allows reading only those data blocks which contain dates from the proper range. In this case, the data block may contain data for many dates (up to an entire month). Within a block, data is sorted by primary key, which might not contain the date as the first column. Because of this, using a query with only a date condition that does not specify the primary key prefix will cause more data to be read than for a single date.

### Use of index for deterministic expressions in primary keys {#use-of-index-for-deterministic-expressions-in-primary-keys}

The primary key can contain expressions, not only column names. These expressions are not limited to simple function chains: they can be arbitrary expression trees (for example, nested functions and composite expressions), as long as they are deterministic.

An expression is **deterministic** if it always returns the same result for the same input values (for example: `length()`, `toDate()`, `lower()`, `left()`, `cityHash64()`, `toUUID()`; unlike `now()` or `rand()`). If the primary key contains deterministic expressions, ClickHouse can apply them to constant values from the query and use the result to build conditions on the primary key index. This enables data skipping for predicates like `=`, `IN`, and `has`.

A common use case is to keep the primary key compact (e.g. store a hash instead of a long `String`), while still allowing predicates on the original column to use the index.

Example of a deterministic (but non-injective) primary key:
```sql
ENGINE = MergeTree()
ORDER BY length(user_id)
```

Example predicates that can use the index:
```sql
SELECT * FROM table WHERE user_id = 'alice';
SELECT * FROM table WHERE user_id IN ('alice', 'bob');
SELECT * FROM table WHERE has(['alice', 'bob'], user_id);
```

In these cases, ClickHouse computes `length('alice')` (and other constants) once and uses the length values to narrow the ranges in the primary key index. Since length of a string is **not injective**, different `user_id` strings can share the same length, so the index may read extra granules (false positives). The result remains correct because the original predicate (`user_id = ...`, `IN`, etc.) is still applied after reading.

If the deterministic expression is also **injective** (different inputs cannot produce the same output for the argument types used), additionally ClickHouse can effectively use the index for the negated forms: `!=`, `NOT IN`, and `NOT has(...)`. For example, `reverse(p)` and `hex(p)` are injective for `String`.

Example of an injective primary key:
```sql
ENGINE = MergeTree()
ORDER BY hex(p)
```

More complex injective expressions are also supported, for example:
```sql
ENGINE = MergeTree()
ORDER BY reverse(tuple(reverse(p), hex(p)))
```

Example predicates that can use the index:
```sql
SELECT * FROM table WHERE p != 'abc';
SELECT * FROM table WHERE p NOT IN ('abc', '12345');
SELECT * FROM table WHERE NOT has(['abc', '12345'], p);
```

### Use of index for partially-monotonic primary keys {#use-of-index-for-partially-monotonic-primary-keys}

Consider, for example, the days of the month. They form a [monotonic sequence](https://en.wikipedia.org/wiki/Monotonic_function) for one month, but not monotonic for more extended periods. This is a partially-monotonic sequence. If a user creates the table with partially-monotonic primary key, ClickHouse creates a sparse index as usual. When a user selects data from this kind of table, ClickHouse analyzes the query conditions. If the user wants to get data between two marks of the index and both these marks fall within one month, ClickHouse can use the index in this particular case because it can calculate the distance between the parameters of a query and index marks.

ClickHouse cannot use an index if the values of the primary key in the query parameter range do not represent a monotonic sequence. In this case, ClickHouse uses the full scan method.

ClickHouse uses this logic not only for days of the month sequences, but for any primary key that represents a partially-monotonic sequence.

### Data skipping indexes {#table_engine-mergetree-data_skipping-indexes}

The index declaration is in the columns section of the `CREATE` query.

```sql
INDEX index_name expr TYPE type(...) [GRANULARITY granularity_value]
```

For tables from the `*MergeTree` family, data skipping indices can be specified.

These indices aggregate some information about the specified expression on blocks, which consist of `granularity_value` granules (the size of the granule is specified using the `index_granularity` setting in the table engine). Then these aggregates are used in `SELECT` queries for reducing the amount of data to read from the disk by skipping big blocks of data where the `where` query cannot be satisfied.

The `GRANULARITY` clause can be omitted, the default value of `granularity_value` is 1.

**Example**

```sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX idx1 u64 TYPE bloom_filter GRANULARITY 3,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 3,
    INDEX idx3 u64 * length(s) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

Indices from the example can be used by ClickHouse to reduce the amount of data to read from disk in the following queries:

```sql
SELECT count() FROM table WHERE u64 == 10;
SELECT count() FROM table WHERE u64 * i32 >= 1234
SELECT count() FROM table WHERE u64 * length(s) == 1234
```

Data skipping indexes can also be created on composite columns:

```sql
-- on columns of type Map:
INDEX map_key_index mapKeys(map_column) TYPE bloom_filter
INDEX map_value_index mapValues(map_column) TYPE bloom_filter

-- on columns of type JSON:
INDEX json_paths_index JSONAllPaths(json_column) TYPE bloom_filter

-- on columns of type Tuple:
INDEX tuple_1_index tuple_column.1 TYPE bloom_filter
INDEX tuple_2_index tuple_column.2 TYPE bloom_filter

-- on columns of type Nested:
INDEX nested_1_index col.nested_col1 TYPE bloom_filter
INDEX nested_2_index col.nested_col2 TYPE bloom_filter
```

### Skip Index Types {#skip-index-types}

The `MergeTree` table engine supports the following types of skip indexes.
For more information on how skip indexes can be used for performance optimization
see ["Understanding ClickHouse data skipping indexes"](/optimize/skipping-indexes).

- [`MinMax`](#minmax) index
- [`Set`](#set) index
- [`bloom_filter`](#bloom-filter) index
- [`ngrambf_v1`](#n-gram-bloom-filter) index *(Deprecated)*
- [`tokenbf_v1`](#token-bloom-filter) index *(Deprecated)*
- [`text`](#text) index
- [`vector_similarity`](#vector-similarity) index

#### MinMax skip index {#minmax}

For each index granule, the minimum and maximum values of an expression are stored.
(If the expression is of type `tuple`, it stores the minimum and maximum for each tuple element.)

```text title="Syntax"
minmax
```

#### Set {#set}

For each index granule at most `max_rows` many unique values of the specified expression are stored.
`max_rows = 0` means "store all unique values".

```text title="Syntax"
set(max_rows)
```

#### Bloom filter {#bloom-filter}

For each index granule stores a [bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) for the specified columns.

```text title="Syntax"
bloom_filter([false_positive_rate])
```

The `false_positive_rate` parameter can take on a value between 0 and 1 (by default: `0.025`) and specifies the probability of generating a positive (which increases the amount of data to be read).

The following data types are supported:
- `(U)Int*`
- `Float*`
- `Enum`
- `Date`
- `DateTime`
- `String`
- `FixedString`
- `Array`
- `LowCardinality`
- `Nullable`
- `UUID`
- `Map`

:::note Map data type: specifying index creation with keys or values
For the `Map` data type, the client can specify if the index should be created for keys or for values using the [`mapKeys`](/sql-reference/functions/tuple-map-functions.md/#mapKeys) or [`mapValues`](/sql-reference/functions/tuple-map-functions.md/#mapValues) functions.
:::

:::note JSON data type: indexing JSON paths
For the [`JSON`](/sql-reference/data-types/newjson) data type, a bloom filter index can be created on the set of paths using the [`JSONAllPaths`](/sql-reference/functions/json-functions#JSONAllPaths) function. This allows skipping granules where a queried JSON path is absent. See [Data skipping indexes for JSON](/sql-reference/data-types/newjson#data-skipping-indexes-for-json) for details.
:::

#### N-gram bloom filter *(Deprecated)* {#n-gram-bloom-filter}

:::note
With general availability (GA) of the `text` index starting from ClickHouse version 26.2, the `ngrambf_v1` index is no longer recommended for full text search.

See page ["Full-text search with text indexes"](./textindexes.md) for details.
:::

For each index granule stores a [bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) for the [n-grams](https://en.wikipedia.org/wiki/N-gram) of the specified columns.

```text title="Syntax"
ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

| Parameter                       | Description |
|---------------------------------|-------------|
| `n`                             | ngram size  |
| `size_of_bloom_filter_in_bytes` | Bloom filter size in bytes. You can use a large value here, for example, `256` or `512`, because it can be compressed well).|
|`number_of_hash_functions`       |The number of hash functions used in the bloom filter.|
|`random_seed` |Seed for the bloom filter hash functions.|

This index only works with the following data types:
- [`String`](/sql-reference/data-types/string.md)
- [`FixedString`](/sql-reference/data-types/fixedstring.md)
- [`Map`](/sql-reference/data-types/map.md)

To estimate the parameters of `ngrambf_v1`, you can use the following [User Defined Functions (UDFs)](/sql-reference/statements/create/function.md).

```sql title="UDFs for ngrambf_v1"
CREATE FUNCTION bfEstimateFunctions [ON CLUSTER cluster]
AS
(total_number_of_all_grams, size_of_bloom_filter_in_bits) -> round((size_of_bloom_filter_in_bits / total_number_of_all_grams) * log(2));

CREATE FUNCTION bfEstimateBmSize [ON CLUSTER cluster]
AS
(total_number_of_all_grams, probability_of_false_positives) -> ceil((total_number_of_all_grams * log(probability_of_false_positives)) / log(1 / pow(2, log(2))));

CREATE FUNCTION bfEstimateFalsePositive [ON CLUSTER cluster]
AS
(total_number_of_all_grams, number_of_hash_functions, size_of_bloom_filter_in_bytes) -> pow(1 - exp(-number_of_hash_functions/ (size_of_bloom_filter_in_bytes / total_number_of_all_grams)), number_of_hash_functions);

CREATE FUNCTION bfEstimateGramNumber [ON CLUSTER cluster]
AS
(number_of_hash_functions, probability_of_false_positives, size_of_bloom_filter_in_bytes) -> ceil(size_of_bloom_filter_in_bytes / (-number_of_hash_functions / log(1 - exp(log(probability_of_false_positives) / number_of_hash_functions))))
```

To use these functions, you need to specify at least two parameters:
- `total_number_of_all_grams`
- `probability_of_false_positives`

For example, there are `4300` ngrams in the granule and you expect false positives to be less than `0.0001`.
The other parameters can then be estimated by executing the following queries:

```sql
--- estimate number of bits in the filter
SELECT bfEstimateBmSize(4300, 0.0001) / 8 AS size_of_bloom_filter_in_bytes;

┌─size_of_bloom_filter_in_bytes─┐
│                         10304 │
└───────────────────────────────┘

--- estimate number of hash functions
SELECT bfEstimateFunctions(4300, bfEstimateBmSize(4300, 0.0001)) as number_of_hash_functions

┌─number_of_hash_functions─┐
│                       13 │
└──────────────────────────┘
```

Of course, you can also use those functions to estimate parameters for other conditions.
The functions above refer to the bloom filter calculator [here](https://hur.st/bloomfilter).

#### Token bloom filter {#token-bloom-filter}

:::note
With general availability (GA) of the `text` index starting from ClickHouse version 26.2, the `tokenbf_v1` index is no longer recommended for full text search.

See page ["Full-text search with text indexes"](./textindexes.md) for details.
:::

```text title="Syntax"
tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

#### Sparse grams bloom filter {#sparse-grams-bloom-filter}

The sparse grams bloom filter is similar to `ngrambf_v1` but uses [sparse grams tokens](/sql-reference/functions/string-functions.md/#sparseGrams) instead of ngrams.

```text title="Syntax"
sparse_grams(min_ngram_length, max_ngram_length, min_cutoff_length, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

### Text index {#text}

Builds an inverted index over tokenized string data, enabling efficient and deterministic full-text search. See [here](textindexes.md) for details.

#### Vector similarity {#vector-similarity}

Supports approximate nearest neighbor search, see [here](annindexes.md) for details.

### Functions support {#functions-support}

Conditions in the `WHERE` clause contains calls of the functions that operate with columns. If the column is a part of an index, ClickHouse tries to use this index when performing the functions. ClickHouse supports different subsets of functions for using indexes.

Indexes of type `set` can be utilized by all functions. The other index types are supported as follows:

| Function (operator) / Index                                                                                                    | primary key | minmax | ngrambf_v1 | tokenbf_v1 | bloom_filter | sparse_grams | text |
|--------------------------------------------------------------------------------------------------------------------------------|-------------|--------|------------|------------|--------------|--------------|------|
| [equals (=, ==)](/sql-reference/functions/comparison-functions.md/#equals)                                                     | ✔           | ✔      | ✔          | ✔          | ✔            | ✔            | ✔    |
| [notEquals(!=, &lt;&gt;)](/sql-reference/functions/comparison-functions.md/#notEquals)                                         | ✔           | ✔      | ✔          | ✔          | ✔            | ✔            | ✗    |
| [like](/sql-reference/functions/string-search-functions.md/#like)                                                              | ✔           | ✔      | ✔          | ✔          | ✗            | ✔            | ✔    |
| [notLike](/sql-reference/functions/string-search-functions.md/#notLike)                                                        | ✔           | ✔      | ✔          | ✔          | ✗            | ✔            | ✗    |
| [match](/sql-reference/functions/string-search-functions.md/#match)                                                            | ✗           | ✗      | ✔          | ✔          | ✗            | ✔            | ✔    |
| [startsWith](/sql-reference/functions/string-functions.md/#startsWith)                                                         | ✔           | ✔      | ✔          | ✔          | ✗            | ✔            | ✔    |
| [endsWith](/sql-reference/functions/string-functions.md/#endsWith)                                                             | ✗           | ✗      | ✔          | ✔          | ✗            | ✔            | ✔    |
| [multiSearchAny](/sql-reference/functions/string-search-functions.md/#multiSearchAny)                                          | ✗           | ✗      | ✔          | ✗          | ✗            | ✗            | ✗    |
| [in](/sql-reference/functions/in-functions)                                                                                    | ✔           | ✔      | ✔          | ✔          | ✔            | ✔            | ✔    |
| [notIn](/sql-reference/functions/in-functions)                                                                                 | ✔           | ✔      | ✔          | ✔          | ✔            | ✔            | ✗    |
| [less (`<`)](/sql-reference/functions/comparison-functions.md/#less)                                                           | ✔           | ✔      | ✗          | ✗          | ✗            | ✗            | ✗    |
| [greater (`>`)](/sql-reference/functions/comparison-functions.md/#greater)                                                     | ✔           | ✔      | ✗          | ✗          | ✗            | ✗            | ✗    |
| [lessOrEquals (`<=`)](/sql-reference/functions/comparison-functions.md/#lessOrEquals)                                          | ✔           | ✔      | ✗          | ✗          | ✗            | ✗            | ✗    |
| [greaterOrEquals (`>=`)](/sql-reference/functions/comparison-functions.md/#greaterOrEquals)                                    | ✔           | ✔      | ✗          | ✗          | ✗            | ✗            | ✗    |
| [empty](/sql-reference/functions/array-functions/#empty)                                                                       | ✔           | ✔      | ✗          | ✗          | ✗            | ✗            | ✗    |
| [notEmpty](/sql-reference/functions/array-functions/#notEmpty)                                                                 | ✗           | ✔      | ✗          | ✗          | ✗            | ✔            | ✗    |
| [has](/sql-reference/functions/array-functions#has)                                                                            | ✔           | ✔      | ✔          | ✔          | ✔            | ✔            | ✔    |
| [hasAny](/sql-reference/functions/array-functions#hasAny)                                                                      | ✗           | ✗      | ✔          | ✔          | ✔            | ✔            | ✗    |
| [hasAll](/sql-reference/functions/array-functions#hasAll)                                                                      | ✗           | ✗      | ✔          | ✔          | ✔            | ✔            | ✗    |
| [hasToken](/sql-reference/functions/string-search-functions.md/#hasToken)                                                      | ✗           | ✗      | ✗          | ✔          | ✗            | ✗            | ✔    |
| [hasTokenOrNull](/sql-reference/functions/string-search-functions.md/#hasTokenOrNull)                                          | ✗           | ✗      | ✗          | ✔          | ✗            | ✗            | ✔    |
| [hasTokenCaseInsensitive (`*`)](/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitive)                  | ✗           | ✗      | ✗          | ✔          | ✗            | ✗            | ✗    |
| [hasTokenCaseInsensitiveOrNull (`*`)](/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitiveOrNull)      | ✗           | ✗      | ✗          | ✔          | ✗            | ✗            | ✗    |
| [hasAnyTokens](/sql-reference/functions/string-search-functions.md/#hasAnyTokens)                                              | ✗           | ✗      | ✗          | ✗          | ✗            | ✗            | ✔    |
| [hasAllTokens](/sql-reference/functions/string-search-functions.md/#hasAllTokens)                                              | ✗           | ✗      | ✗          | ✗          | ✗            | ✗            | ✔    |
| [pointInPolygon](/sql-reference/functions/geo/coordinates.md#pointinpolygon)                                                   | ✔           | ✔      | ✗          | ✗          | ✗            | ✗            |  ✗    |
| [mapContains (mapContainsKey)](/sql-reference/functions/tuple-map-functions#mapContainsKey)                                    | ✗           | ✗      | ✗          | ✗          | ✗            | ✗            | ✔    |
| [mapContainsKeyLike](/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)                                          | ✗           | ✗      | ✗          | ✗          | ✗            | ✗            | ✔    |
| [mapContainsValue](/sql-reference/functions/tuple-map-functions#mapContainsValue)                                              | ✗           | ✗      | ✗          | ✗          | ✗            | ✗            | ✔    |
| [mapContainsValueLike](/sql-reference/functions/tuple-map-functions#mapContainsValueLike)                                      | ✗           | ✗      | ✗          | ✗          | ✗            | ✗            | ✔    |

Functions with a constant argument that is less than ngram size can't be used by `ngrambf_v1` for query optimization.

(*) For `hasTokenCaseInsensitive` and `hasTokenCaseInsensitiveOrNull` to be effective, the `tokenbf_v1` index must be created on lowercased data, for example `INDEX idx (lower(str_col)) TYPE tokenbf_v1(512, 3, 0)`.

:::note
Bloom filters can have false positive matches, so the `ngrambf_v1`, `tokenbf_v1`, `sparse_grams`, and `bloom_filter` indexes can not be used for optimizing queries where the result of a function is expected to be false.

For example:

- Can be optimized:
  - `s LIKE '%test%'`
  - `NOT s NOT LIKE '%test%'`
  - `s = 1`
  - `NOT s != 1`
  - `startsWith(s, 'test')`
- Can not be optimized:
  - `NOT s LIKE '%test%'`
  - `s NOT LIKE '%test%'`
  - `NOT s = 1`
  - `s != 1`
  - `NOT startsWith(s, 'test')`
:::

)DOCS_MD")
        // The MergeTree documentation is concatenated at runtime from several string literals because a single
        // string literal would exceed the 65536-byte length that C++ compilers are required to support.
        + R"DOCS_MD(## Projections {#projections}
Projections are like [materialized views](/sql-reference/statements/create/view) but defined in part-level. It provides consistency guarantees along with automatic usage in queries.

:::note
When you are implementing projections you should also consider the [force_optimize_projection](/operations/settings/settings#force_optimize_projection) setting.
:::

Projections are not supported in the `SELECT` statements with the [FINAL](/sql-reference/statements/select/from#final-modifier) modifier.

### Projection query {#projection-query}
A projection query is what defines a projection. It implicitly selects data from the parent table.
**Syntax**

```sql
SELECT <column list expr> [GROUP BY] <group keys expr> [ORDER BY] <expr>
```

Projections can be modified or dropped with the [ALTER](/sql-reference/statements/alter/projection.md) statement.

### Projection indexes {#projection-index}

Projection indexes extend the projection subsystem by providing a lightweight and explicit way to define projection-level indexes.
Externally, a projection index is still a projection, but with simplified syntax and clearer intent: it defines an expression which is dedicated to filtering, rather than serving materialized data.
Internally, a projection index does not materialize the original table in permuted row order like a regular projection.
Instead, the permutation is stored in the form of a numeric permutation column `_part_offset`, i.e. `SELECT _part_offset ORDER BY <index_expr>`.

#### Syntax {#projection-index-syntax}

```sql
PROJECTION <name> INDEX <index_expr> TYPE <index_type>
```

Example:

```sql
CREATE TABLE example
(
    id UInt64,
    region String,
    user_id UInt32,
    PROJECTION region_proj INDEX region TYPE basic,
    PROJECTION uid_proj INDEX user_id TYPE basic
)
ENGINE = MergeTree
ORDER BY id;
```

#### Index types {#projection-index-types}

Currently supported:

* **basic**: equivalent to a normal MergeTree index on the expression.

The framework allows adding more index types in the future.

### Projection storage {#projection-storage}
Projections are stored inside the part directory. It's similar to an index but contains a subdirectory that stores an anonymous `MergeTree` table's part. The table is induced by the definition query of the projection. If there is a `GROUP BY` clause, the underlying storage engine becomes [AggregatingMergeTree](aggregatingmergetree.md), and all aggregate functions are converted to `AggregateFunction`. If there is an `ORDER BY` clause, the `MergeTree` table uses it as its primary key expression. During the merge process the projection part is merged via its storage's merge routine. The checksum of the parent table's part is combined with the projection's part. Other maintenance jobs are similar to skip indices.

### Query analysis {#projection-query-analysis}
1. Check if the projection can be used to answer the given query, that is, it generates the same answer as querying the base table.
2. Select the best feasible match, which contains the least granules to read.
3. The query pipeline which uses projections will be different from the one that uses the original parts. If the projection is absent in some parts, we can add the pipeline to "project" it on the fly.

## Concurrent data access {#concurrent-data-access}

For concurrent table access, we use multi-versioning. In other words, when a table is simultaneously read and updated, data is read from a set of parts that is current at the time of the query. There are no lengthy locks. Inserts do not get in the way of read operations.

Reading from a table is automatically parallelized.

## TTL for columns and tables {#table_engine-mergetree-ttl}

Determines the lifetime of values.

The `TTL` clause can be set for the whole table and for each individual column. Table-level `TTL` can also specify the logic of automatic moving data between disks and volumes, or recompressing parts where all the data has been expired.

Expressions must evaluate to [Date](/sql-reference/data-types/date.md), [Date32](/sql-reference/data-types/date32.md), [DateTime](/sql-reference/data-types/datetime.md) or [DateTime64](/sql-reference/data-types/datetime64.md) data type.

:::tip[Avoid non-deterministic functions in TTL expressions]
TTL is evaluated during background merges, and not at insert time.
Functions like `rand()`, `now()`, or `now64()` will be re-evaluated on every merge, leading to unpredictable deletion behavior.
ClickHouse blocks expressions with no column dependency at all, but does not currently reject non-deterministic functions mixed with a column reference (e.g. `ts + rand()`). TTL expressions should be based solely on deterministic, column-derived values for predictable results.
:::

**Syntax**

Setting time-to-live for a column:

```sql
TTL time_column
TTL time_column + interval
```

To define `interval`, use [time interval](/sql-reference/operators#operators-for-working-with-dates-and-times) operators, for example:

```sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

### Column TTL {#mergetree-column-ttl}

When the values in the column expire, ClickHouse replaces them with the default values for the column data type. If all the column values in the data part expire, ClickHouse deletes this column from the data part in a filesystem.

The `TTL` clause can't be used for key columns.

**Examples**

#### Creating a table with `TTL`: {#creating-a-table-with-ttl}

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int TTL d + INTERVAL 1 MONTH,
    b Int TTL d + INTERVAL 1 MONTH,
    c String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d;
```

#### Adding TTL to a column of an existing table {#adding-ttl-to-a-column-of-an-existing-table}

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

#### Altering TTL of the column {#altering-ttl-of-the-column}

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

### Table TTL {#mergetree-table-ttl}

Table can have an expression for removal of expired rows, and multiple expressions for automatic move of parts between [disks or volumes](#table_engine-mergetree-multiple-volumes). When rows in the table expire, ClickHouse deletes all corresponding rows. For parts moving or recompressing, all rows of a part must satisfy the `TTL` expression criteria.

```sql
TTL expr
    [DELETE|RECOMPRESS codec_name1|TO DISK 'xxx'|TO VOLUME 'xxx'][, DELETE|RECOMPRESS codec_name2|TO DISK 'aaa'|TO VOLUME 'bbb'] ...
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ]
```

Type of TTL rule may follow each TTL expression. It affects an action which is to be done once the expression is satisfied (reaches current time):

- `DELETE` - delete expired rows (default action);
- `RECOMPRESS codec_name` - recompress data part with the `codec_name`;
- `TO DISK 'aaa'` - move part to the disk `aaa`;
- `TO VOLUME 'bbb'` - move part to the disk `bbb`;
- `GROUP BY` - aggregate expired rows.

`DELETE` action can be used together with `WHERE` clause to delete only some of the expired rows based on a filtering condition:
```sql
TTL time_column + INTERVAL 1 MONTH DELETE WHERE column = 'value'
```

`GROUP BY` expression must be a prefix of the table primary key.

If a column is not part of the `GROUP BY` expression and is not set explicitly in the `SET` clause, in result row it contains an occasional value from the grouped rows (as if aggregate function `any` is applied to it).

**Examples**

#### Creating a table with `TTL`: {#creating-a-table-with-ttl-1}

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE,
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

#### Altering `TTL` of the table: {#altering-ttl-of-the-table}

```sql
ALTER TABLE tab
    MODIFY TTL d + INTERVAL 1 DAY;
```

Creating a table, where the rows are expired after one month. The expired rows where dates are Mondays are deleted:

```sql
CREATE TABLE table_with_where
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE WHERE toDayOfWeek(d) = 1;
```

#### Creating a table, where expired rows are recompressed: {#creating-a-table-where-expired-rows-are-recompressed}

```sql
CREATE TABLE table_for_recompression
(
    d DateTime,
    key UInt64,
    value String
) ENGINE MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), d + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10))
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
```

Creating a table, where expired rows are aggregated. In result rows `x` contains the maximum value across the grouped rows, `y` — the minimum value, and `d` — any occasional value from grouped rows.

```sql
CREATE TABLE table_for_aggregation
(
    d DateTime,
    k1 Int,
    k2 Int,
    x Int,
    y Int
)
ENGINE = MergeTree
ORDER BY (k1, k2)
TTL d + INTERVAL 1 MONTH GROUP BY k1, k2 SET x = max(x), y = min(y);
```

### Removing expired data {#mergetree-removing-expired-data}

Data with an expired `TTL` is removed when ClickHouse merges data parts.

When ClickHouse detects that data is expired, it performs an off-schedule merge. To control the frequency of such merges, you can set `merge_with_ttl_timeout`. If the value is too low, it will perform many off-schedule merges that may consume a lot of resources.

If you perform the `SELECT` query between merges, you may get expired data. To avoid it, use the [OPTIMIZE](/sql-reference/statements/optimize.md) query before `SELECT`.

**See Also**

- [ttl_only_drop_parts](/operations/settings/merge-tree-settings#ttl_only_drop_parts) setting

## Disk types {#disk-types}

In addition to local block devices, ClickHouse supports these storage types:
- [`s3` for S3 and MinIO](#table_engine-mergetree-s3)
- [`gcs` for GCS](/integrations/data-ingestion/gcs/index.md/#creating-a-disk)
- [`blob_storage_disk` for Azure Blob Storage](/operations/storing-data#azure-blob-storage)
- [`hdfs` for HDFS](/engines/table-engines/integrations/hdfs)
- [`web` for read-only from web](/operations/storing-data#web-storage)
- [`cache` for local caching](/operations/storing-data#using-local-cache)
- [`s3_plain` for backups to S3](/operations/backup/disk)
- [`s3_plain_rewritable` for immutable, non-replicated tables in S3](/operations/storing-data.md#s3-plain-rewritable-storage)

## Using multiple block devices for data storage {#table_engine-mergetree-multiple-volumes}

### Introduction {#introduction}

`MergeTree` family table engines can store data on multiple block devices. For example, it can be useful when the data of a certain table are implicitly split into "hot" and "cold". The most recent data is regularly requested but requires only a small amount of space. On the contrary, the fat-tailed historical data is requested rarely. If several disks are available, the "hot" data may be located on fast disks (for example, NVMe SSDs or in memory), while the "cold" data - on relatively slow ones (for example, HDD).

This applies to all disk types, including S3 and other object storage disks. For example, you can spread data across multiple S3 buckets within a single volume, or create tiered policies that move data from local disks to S3. See [Using S3 disks with multiple volumes](#s3-multiple-volumes) for details.

Data part is the minimum movable unit for `MergeTree`-engine tables. The data belonging to one part are stored on one disk. Data parts can be moved between disks in the background (according to user settings) as well as by means of the [ALTER](/sql-reference/statements/alter/partition) queries.

### Terms {#terms}

- Disk — Block device mounted to the filesystem.
- Default disk — Disk that stores the path specified in the [path](/operations/server-configuration-parameters/settings.md/#path) server setting.
- Volume — Ordered set of equal disks (similar to [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)).
- Storage policy — Set of volumes and the rules for moving data between them.

The names given to the described entities can be found in the system tables, [system.storage_policies](/operations/system-tables/storage_policies) and [system.disks](/operations/system-tables/disks). To apply one of the configured storage policies for a table, use the `storage_policy` setting of `MergeTree`-engine family tables.

### Configuration {#table_engine-mergetree-multiple-volumes_configure}

Disks, volumes and storage policies should be declared inside the `<storage_configuration>` tag either in a file in the `config.d` directory.

:::tip
Disks can also be declared in the `SETTINGS` section of a query.  This is useful
for ad-hoc analysis to temporarily attach a disk that is, for example, hosted at a URL.
See [dynamic storage](/operations/storing-data#dynamic-configuration) for more details.
:::

Configuration structure:

```xml
<storage_configuration>
    <disks>
        <disk_name_1> <!-- disk name -->
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>

        ...
    </disks>

    ...
</storage_configuration>
```

Tags:

- `<disk_name_N>` — Disk name. Names must be different for all disks.
- `path` — path under which a server will store data (`data` and `shadow` folders), should be terminated with '/'.
- `keep_free_space_bytes` — the amount of free disk space to be reserved.

The order of the disk definition is not important.

Storage policies configuration markup:

```xml
<storage_configuration>
    ...
    <policies>
        <policy_name_1>
            <volumes>
                <volume_name_1>
                    <disk>disk_name_from_disks_configuration</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                    <load_balancing>round_robin</load_balancing>
                </volume_name_1>
                <volume_name_2>
                    <!-- configuration -->
                </volume_name_2>
                <!-- more volumes -->
            </volumes>
            <move_factor>0.2</move_factor>
        </policy_name_1>
        <policy_name_2>
            <!-- configuration -->
        </policy_name_2>

        <!-- more policies -->
    </policies>
    ...
</storage_configuration>
```

Tags:

- `policy_name_N` — Policy name. Policy names must be unique.
- `volume_name_N` — Volume name. Volume names must be unique.
- `disk` — a disk within a volume.
- `max_data_part_size_bytes` — the maximum size of a part that can be stored on any of the volume's disks. If the a size of a merged part estimated to be bigger than `max_data_part_size_bytes` then this part will be written to a next volume. Basically this feature allows to keep new/small parts on a hot (SSD) volume and move them to a cold (HDD) volume when they reach large size. Do not use this setting if your policy has only one volume.
- `move_factor` — when the amount of available space gets lower than this factor, data automatically starts to move on the next volume if any (by default, 0.1). ClickHouse sorts existing parts by size from largest to smallest (in descending order) and selects parts with the total size that is sufficient to meet the `move_factor` condition. If the total size of all parts is insufficient, all parts will be moved.
- `perform_ttl_move_on_insert` — Disables TTL move on data part INSERT. By default (if enabled) if we insert a data part that already expired by the TTL move rule it immediately goes to a volume/disk declared in move rule. This can significantly slowdown insert in case if destination volume/disk is slow (e.g. S3). If disabled then already expired data part is written into a default volume and then right after moved to TTL volume.
- `load_balancing` - Policy for disk balancing, `round_robin` or `least_used`.
- `least_used_ttl_ms` - Configure timeout (in milliseconds) for the updating available space on all disks (`0` - update always, `-1` - never update, default is `60000`). Note, if the disk can be used by ClickHouse only and is not subject to a online filesystem resize/shrink you can use `-1`, in all other cases it is not recommended, since eventually it will lead to incorrect space distribution.
- `prefer_not_to_merge` — You should not use this setting. Disables merging of data parts on this volume (this is harmful and leads to performance degradation). When this setting is enabled (don't do it), merging data on this volume is not allowed (which is bad). This allows (but you don't need it) controlling (if you want to control something, you're making a mistake) how ClickHouse works with slow disks (but ClickHouse knows better, so please don't use this setting).
- `volume_priority` — Defines the priority (order) in which volumes are filled. Lower value means higher priority. The parameter values should be natural numbers and collectively cover the range from 1 to N (lowest priority given) without skipping any numbers.
  * If _all_ volumes are tagged, they are prioritized in given order.
  * If only _some_ volumes are tagged, those without the tag have the lowest priority, and they are prioritized in the order they are defined in config.
  * If _no_ volumes are tagged, their priority is set correspondingly to their order they are declared in configuration.
  * Two volumes cannot have the same priority value.

Configuration examples:

```xml
<storage_configuration>
    ...
    <policies>
        <hdd_in_order> <!-- policy name -->
            <volumes>
                <single> <!-- volume name -->
                    <disk>disk1</disk>
                    <disk>disk2</disk>
                </single>
            </volumes>
        </hdd_in_order>

        <moving_from_ssd_to_hdd>
            <volumes>
                <hot>
                    <disk>fast_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>disk1</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </moving_from_ssd_to_hdd>

        <small_jbod_with_external_no_merges>
            <volumes>
                <main>
                    <disk>jbod1</disk>
                </main>
                <external>
                    <disk>external</disk>
                </external>
            </volumes>
        </small_jbod_with_external_no_merges>
    </policies>
    ...
</storage_configuration>
```

In given example, the `hdd_in_order` policy implements the [round-robin](https://en.wikipedia.org/wiki/Round-robin_scheduling) approach. Thus this policy defines only one volume (`single`), the data parts are stored on all its disks in circular order. Such policy can be quite useful if there are several similar disks are mounted to the system, but RAID is not configured. Keep in mind that each individual disk drive is not reliable and you might want to compensate it with replication factor of 3 or more.

If there are different kinds of disks available in the system, `moving_from_ssd_to_hdd` policy can be used instead. The volume `hot` consists of an SSD disk (`fast_ssd`), and the maximum size of a part that can be stored on this volume is 1GB. All the parts with the size larger than 1GB will be stored directly on the `cold` volume, which contains an HDD disk `disk1`.
Also, once the disk `fast_ssd` gets filled by more than 80%, data will be transferred to the `disk1` by a background process.

The order of volume enumeration within a storage policy is important in case at least one of the volumes listed has no explicit `volume_priority` parameter.
Once a volume is overfilled, data are moved to the next one. The order of disk enumeration is important as well because data are stored on them in turns.

When creating a table, one can apply one of the configured storage policies to it:

```sql
CREATE TABLE table_with_non_default_policy (
    EventDate Date,
    OrderID UInt64,
    BannerID UInt64,
    SearchPhrase String
) ENGINE = MergeTree
ORDER BY (OrderID, BannerID)
PARTITION BY toYYYYMM(EventDate)
SETTINGS storage_policy = 'moving_from_ssd_to_hdd'
```

The `default` storage policy implies using only one volume, which consists of only one disk given in `<path>`.
You could change storage policy after table creation with [ALTER TABLE ... MODIFY SETTING] query, new policy should include all old disks and volumes with same names.

The number of threads performing background moves of data parts can be changed by [background_move_pool_size](/operations/server-configuration-parameters/settings.md/#background_move_pool_size) setting.

### Details {#details}

In the case of `MergeTree` tables, data is getting to disk in different ways:

- As a result of an insert (`INSERT` query).
- During background merges and [mutations](/sql-reference/statements/alter#mutations).
- When downloading from another replica.
- As a result of partition freezing [ALTER TABLE ... FREEZE PARTITION](/sql-reference/statements/alter/partition#freeze-partition).

In all these cases except for mutations and partition freezing, a part is stored on a volume and a disk according to the given storage policy:

1.  The first volume (in the order of definition) that has enough disk space for storing a part (`unreserved_space > current_part_size`) and allows for storing parts of a given size (`max_data_part_size_bytes > current_part_size`) is chosen.
2.  Within this volume, that disk is chosen that follows the one, which was used for storing the previous chunk of data, and that has free space more than the part size (`unreserved_space - keep_free_space_bytes > current_part_size`).

Under the hood, mutations and partition freezing make use of [hard links](https://en.wikipedia.org/wiki/Hard_link). Hard links between different disks are not supported, therefore in such cases the resulting parts are stored on the same disks as the initial ones.

In the background, parts are moved between volumes on the basis of the amount of free space (`move_factor` parameter) according to the order the volumes are declared in the configuration file.
Data is never transferred from the last one and into the first one. One may use system tables [system.part_log](/operations/system-tables/part_log) (field `type = MOVE_PART`) and [system.parts](/operations/system-tables/parts.md) (fields `path` and `disk`) to monitor background moves. Also, the detailed information can be found in server logs.

User can force moving a part or a partition from one volume to another using the query [ALTER TABLE ... MOVE PART\|PARTITION ... TO VOLUME\|DISK ...](/sql-reference/statements/alter/partition), all the restrictions for background operations are taken into account. The query initiates a move on its own and does not wait for background operations to be completed. User will get an error message if not enough free space is available or if any of the required conditions are not met.

Moving data does not interfere with data replication. Therefore, different storage policies can be specified for the same table on different replicas.

After the completion of background merges and mutations, old parts are removed only after a certain amount of time (`old_parts_lifetime`).
During this time, they are not moved to other volumes or disks. Therefore, until the parts are finally removed, they are still taken into account for evaluation of the occupied disk space.

User can assign new big parts to different disks of a [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures) volume in a balanced way using the [min_bytes_to_rebalance_partition_over_jbod](/operations/settings/merge-tree-settings.md/#min_bytes_to_rebalance_partition_over_jbod) setting.

## Using external storage for data storage {#table_engine-mergetree-s3}

[MergeTree](/engines/table-engines/mergetree-family/mergetree.md) family table engines can store data to `S3`, `AzureBlobStorage`, `HDFS` using a disk with types `s3`, `azure_blob_storage`, `hdfs` accordingly. See [configuring external storage options](/operations/storing-data.md/#configuring-external-storage) for more details.

Example for [S3](https://aws.amazon.com/s3/) as external storage using a disk with type `s3`.

Configuration markup:
```xml
<storage_configuration>
    ...
    <disks>
        <s3>
            <type>s3</type>
            <support_batch_delete>true</support_batch_delete>
            <endpoint>https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/root-path/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
            <region></region>
            <header>Authorization: Bearer SOME-TOKEN</header>
            <server_side_encryption_customer_key_base64>your_base64_encoded_customer_key</server_side_encryption_customer_key_base64>
            <server_side_encryption_kms_key_id>your_kms_key_id</server_side_encryption_kms_key_id>
            <server_side_encryption_kms_encryption_context>your_kms_encryption_context</server_side_encryption_kms_encryption_context>
            <server_side_encryption_kms_bucket_key_enabled>true</server_side_encryption_kms_bucket_key_enabled>
            <proxy>
                <uri>http://proxy1</uri>
                <uri>http://proxy2</uri>
            </proxy>
            <connect_timeout_ms>10000</connect_timeout_ms>
            <request_timeout_ms>5000</request_timeout_ms>
            <retry_attempts>10</retry_attempts>
            <single_read_retries>4</single_read_retries>
            <min_bytes_for_seek>1000</min_bytes_for_seek>
            <metadata_path>/var/lib/clickhouse/disks/s3/</metadata_path>
            <skip_access_check>false</skip_access_check>
        </s3>
        <s3_cache>
            <type>cache</type>
            <disk>s3</disk>
            <path>/var/lib/clickhouse/disks/s3_cache/</path>
            <max_size>10Gi</max_size>
        </s3_cache>
    </disks>
    ...
</storage_configuration>
```

Also see [configuring external storage options](/operations/storing-data.md/#configuring-external-storage).

### Using S3 disks with multiple volumes {#s3-multiple-volumes}

S3 (and other object storage) disks can be used in multi-disk and multi-volume storage policies the same way as local disks. This allows you to spread data across multiple S3 buckets within a single volume (JBOD-style), or set up tiered storage policies with S3 volumes.

For example, to distribute data across two S3 buckets in a round-robin fashion:

```xml
<storage_configuration>
    <disks>
        <s3_bucket1>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-1/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket1>
        <s3_bucket2>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-2/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket2>
    </disks>
    <policies>
        <s3_multi_bucket>
            <volumes>
                <main>
                    <disk>s3_bucket1</disk>
                    <disk>s3_bucket2</disk>
                </main>
            </volumes>
        </s3_multi_bucket>
    </policies>
</storage_configuration>
```

You can also combine local and S3 volumes in a tiered policy, for example moving data from a local SSD to S3 as it ages:

```xml
<storage_configuration>
    <disks>
        <local_ssd>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </local_ssd>
        <s3_cold>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/cold-storage/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_cold>
    </disks>
    <policies>
        <local_to_s3>
            <volumes>
                <hot>
                    <disk>local_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>s3_cold</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </local_to_s3>
    </policies>
</storage_configuration>
```

:::note
When using `use_environment_credentials` for S3 authentication, the environment credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) are shared across all S3 disks. It is not possible to use different environment credentials for different disks. If you need different credentials for each S3 disk, use explicit `access_key_id` and `secret_access_key` settings per disk instead.
:::

It is possible to set up non-replicated MergeTree tables with a one-writer, many-readers scenario on shared storage. This is provided by the automatic refresh of the parts list, which can be set up on readers. Note that this requires shared filesystem metadata across replicas (or `table_disk = true` with a table-local disk). See [refresh_parts_interval and table_disk](/operations/storing-data.md/#refresh-parts-interval-and-table-disk).

:::note cache configuration
ClickHouse versions 22.3 through 22.7 use a different cache configuration, see [using local cache](/operations/storing-data.md/#using-local-cache) if you are using one of those versions.
:::

## Virtual columns {#virtual-columns}

- `_part` — Name of a part.
- `_part_index` — Sequential index of the part in the query result.
- `_part_starting_offset` — Cumulative starting row of the part in the query result.
- `_part_offset` — Number of row in the part.
- `_part_granule_offset` — Number of granule in the part.
- `_partition_id` — Name of a partition.
- `_part_uuid` — Unique part identifier (if enabled MergeTree setting `assign_part_uuids`).
- `_part_data_version` — Data version of part (either min block number or mutation version).
- `_partition_value` — Values (a tuple) of a `partition by` expression.
- `_sample_factor` — Sample factor (from the query).
- `_block_number` — Original number of block for row that was assigned at insert, persisted on merges when setting `enable_block_number_column` is enabled.
- `_block_offset` — Original number of row in block that was assigned at insert, persisted on merges when setting `enable_block_offset_column` is enabled.
- `_disk_name` — Disk name used for the storage.

## Column statistics {#column-statistics}

<CloudNotSupportedBadge/>

The statistics declaration is in the columns section of the `CREATE` query for tables from the `*MergeTree*` Family:

```sql
CREATE TABLE tab
(
    a Int64 STATISTICS(TDigest, Uniq),
    b Float64
)
ENGINE = MergeTree
ORDER BY a
```

We can also manipulate statistics with `ALTER` statements:

```sql
ALTER TABLE tab ADD STATISTICS b TYPE TDigest, Uniq;
ALTER TABLE tab DROP STATISTICS a;
```

These lightweight statistics aggregate information about distribution of values in columns. Statistics are stored in every part and updated when every insert comes.
They can be used for prewhere optimization only if we enable `set use_statistics = 1`.

#### Part Pruning with Statistics {#part-pruning-with-statistics}

When `use_statistics_for_part_pruning` is enabled, statistics can be used for part pruning.
Currently, only `MinMax` statistics support part pruning. When MinMax statistics are defined on a column, ClickHouse tracks the minimum and maximum values for that column in each part.
Part pruning allows to skip reading entire data parts when the query filter condition cannot match any rows in that part.

**Example:**

```sql
-- Create a table with MinMax statistics on the 'value' column
CREATE TABLE test_stats
(
    id UInt64,
    value Int64 STATISTICS(MinMax)
)
ENGINE = MergeTree
ORDER BY id;

SYSTEM STOP MERGES test_stats;

-- Insert data in separate inserts to create multiple parts
INSERT INTO test_stats SELECT number, number FROM numbers(1000); -- Part 1: value range [0, 999]
INSERT INTO test_stats SELECT number, number + 10000 FROM numbers(1000); -- Part 2: value range [10000, 10999]

SET use_statistics_for_part_pruning = 1;

-- This query will skip Part 1 entirely because its max value (999) < 5000
SELECT count() FROM test_stats WHERE value > 5000;

-- Use EXPLAIN to see the pruning effect
EXPLAIN indexes = 1 SELECT count() FROM test_stats WHERE value > 5000;
-- The output will show "Parts: 1/2" indicating one part was pruned
```

### Available types of column statistics {#available-types-of-column-statistics}

- `MinMax`

    The minimum and maximum column value which allows to estimate the selectivity of range filters on numeric columns.

    Syntax: `minmax`

- `TDigest`

    [TDigest](https://github.com/tdunning/t-digest) sketches which allow to compute approximate percentiles (e.g. the 90th percentile) for numeric columns.

    Syntax: `tdigest`

- `Uniq`

    [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) sketches which provide an estimation how many distinct values a column contains.

    Syntax: `uniq`

- `CountMin`

    [CountMin](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) sketches which provide an approximate count of the frequency of each value in a column.

    Syntax `countmin`

### Supported data types {#supported-data-types}

|           | (U)Int*, Float*, Decimal(*), Date*, Boolean, Enum* | String or FixedString |
|-----------|----------------------------------------------------|-----------------------|
| CountMin  | ✔                                                  | ✔                     |
| MinMax    | ✔                                                  | ✗                     |
| TDigest   | ✔                                                  | ✗                     |
| Uniq      | ✔                                                  | ✔                     |

### Supported operations {#supported-operations}

|           | Equality filters (==) | Range filters (`>, >=, <, <=`) |
|-----------|-----------------------|------------------------------|
| CountMin  | ✔                     | ✗                            |
| MinMax    | ✗                     | ✔                            |
| TDigest   | ✗                     | ✔                            |
| Uniq      | ✔                     | ✗                            |

## Column-level settings {#column-level-settings}

Certain MergeTree settings can be overridden at column level:

- `max_compress_block_size` — Maximum size of blocks of uncompressed data before compressing for writing to a table.
- `min_compress_block_size` — Minimum size of blocks of uncompressed data required for compression when writing the next mark.

Example:

```sql
CREATE TABLE tab
(
    id Int64,
    document String SETTINGS (min_compress_block_size = 16777216, max_compress_block_size = 16777216)
)
ENGINE = MergeTree
ORDER BY id
```

Column-level settings can be modified or removed using [ALTER MODIFY COLUMN](/sql-reference/statements/alter/column.md), for example:

- Remove `SETTINGS` from column declaration:

```sql
ALTER TABLE tab MODIFY COLUMN document REMOVE SETTINGS;
```

- Modify a setting:

```sql
ALTER TABLE tab MODIFY COLUMN document MODIFY SETTING min_compress_block_size = 8192;
```

- Reset one or more settings, also removes the setting declaration in the column expression of the table's CREATE query.

```sql
ALTER TABLE tab MODIFY COLUMN document RESET SETTING min_compress_block_size;
```
)DOCS_MD",
        .syntax = "ENGINE = MergeTree() ORDER BY expr [PARTITION BY expr] [PRIMARY KEY expr] [SAMPLE BY expr] [TTL expr] [SETTINGS ...]",
        .related = {"ReplicatedMergeTree"}});

    factory.registerStorage("CollapsingMergeTree", create, features, Documentation{
        .description = R"DOCS_MD(
## Description {#description}

The `CollapsingMergeTree` engine inherits from [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)
and adds logic for collapsing rows during the merge process.
The `CollapsingMergeTree` table engine asynchronously deletes (collapses)
pairs of rows if all the fields in a sorting key (`ORDER BY`) are equivalent except for the special field `Sign`,
which can have values of either `1` or `-1`.
Rows without a pair of opposite valued `Sign` are kept.

For more details, see the [Collapsing](#table_engine-collapsingmergetree-collapsing) section of the document.

:::note
This engine may significantly reduce the volume of storage,
increasing the efficiency of `SELECT` queries as a consequence.
:::

## Parameters {#parameters}

All parameters of this table engine, with the exception of the `Sign` parameter,
have the same meaning as in [`MergeTree`](/engines/table-engines/mergetree-family/mergetree).

- `Sign` — The name given to a column with the type of row where `1` is a "state" row and `-1` is a "cancel" row. Type: [Int8](/sql-reference/data-types/int-uint).

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
)
ENGINE = CollapsingMergeTree(Sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::note
The method below is not recommended for use in new projects.
We advise, if possible, to update old projects to use the new method.
:::

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
)
ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, Sign)
```

`Sign` — The name given to a column with the type of row where `1` is a "state" row and `-1` is a "cancel" row. [Int8](/sql-reference/data-types/int-uint).

</details>

- For a description of query parameters, see [query description](../../../sql-reference/statements/create/table.md).
- When creating a `CollapsingMergeTree` table, the same [query clauses](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) are required, as when creating a `MergeTree` table.

## Collapsing {#table_engine-collapsingmergetree-collapsing}

### Data {#data}

Consider the situation where you need to save continually changing data for some given object.
It may sound logical to have one row per object and update it anytime something changes,
however, update operations are expensive and slow for the DBMS because they require rewriting the data in storage.
If we need to write data quickly, performing large numbers of updates is not an acceptable approach,
but we can always write the changes of an object sequentially.
To do so, we make use of the special column `Sign`.

- If `Sign` = `1` it means that the row is a "state" row: _a row containing fields which represent a current valid state_.
- If `Sign` = `-1` it means that the row is a "cancel" row: _a row used for the cancellation of state of an object with the same attributes_.

For example, we want to calculate how many pages users checked on some website and how long they visited them for.
At some given moment in time, we write the following row with the state of user activity:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

At a later moment in time, we register the change of user activity and write it with the following two rows:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

The first row cancels the previous state of the object (representing a user in this case).
It should copy all the sorting key fields for the "canceled" row except for `Sign`.
The second row above contains the current state.

As we need only the last state of user activity, the original "state" row and the "cancel"
row that we inserted can be deleted as shown below, collapsing the invalid (old) state of an object:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │ -- old "state" row can be deleted
│ 4324182021466249494 │         5 │      146 │   -1 │ -- "cancel" row can be deleted
│ 4324182021466249494 │         6 │      185 │    1 │ -- new "state" row remains
└─────────────────────┴───────────┴──────────┴──────┘
```

`CollapsingMergeTree` carries out precisely this _collapsing_ behavior while merging of the data parts takes place.

:::note
The reason for why two rows are needed for each change
is further discussed in the [Algorithm](#table_engine-collapsingmergetree-collapsing-algorithm) paragraph.
:::

**The peculiarities of such an approach**

1.  The program that writes the data should remember the state of an object to be able to cancel it. The "cancel" row should contain copies of sorting key fields of the "state" and the opposite `Sign`. This increases the initial size of storage but allows us to write the data quickly.
2.  Long growing arrays in columns reduce the efficiency of the engine due to the increased load for writing. The more straightforward the data, the higher the efficiency.
3.  The `SELECT` results depend strongly on the consistency of the object change history. Be accurate when preparing data for inserting. You can get unpredictable results with inconsistent data. For example, negative values for non-negative metrics such as session depth.

### Algorithm {#table_engine-collapsingmergetree-collapsing-algorithm}

When ClickHouse merges data [parts](/concepts/glossary#parts),
each group of consecutive rows with the same sorting key (`ORDER BY`) is reduced to no more than two rows,
the "state" row with `Sign` = `1` and the "cancel" row with `Sign` = `-1`.
In other words, in ClickHouse entries collapse.

For each resulting data part ClickHouse saves:

|  |                                                                                                                                     |
|--|-------------------------------------------------------------------------------------------------------------------------------------|
|1.| The first "cancel" and the last "state" rows, if the number of "state" and "cancel" rows matches and the last row is a "state" row. |
|2.| The last "state" row, if there are more "state" rows than "cancel" rows.                                                            |
|3.| The first "cancel" row, if there are more "cancel" rows than "state" rows.                                                          |
|4.| None of the rows, in all other cases.                                                                                               |

Additionally, when there are at least two more "state" rows than "cancel"
rows, or at least two more "cancel" rows than "state" rows, the merge continues.
ClickHouse, however, treats this situation as a logical error and records it in the server log.
This error can occur if the same data is inserted more than once.
Thus, collapsing should not change the results of calculating statistics.
Changes are gradually collapsed so that in the end only the last state of almost every object is left.

The `Sign` column is required because the merging algorithm does not guarantee
that all the rows with the same sorting key will be in the same resulting data part and even on the same physical server.
ClickHouse processes `SELECT` queries with multiple threads, and it cannot predict the order of rows in the result.

Aggregation is required if there is a need to get completely "collapsed" data from the `CollapsingMergeTree` table.
To finalize collapsing, write a query with the `GROUP BY` clause and aggregate functions that account for the sign.
For example, to calculate quantity, use `sum(Sign)` instead of `count()`.
To calculate the sum of something, use `sum(Sign * x)` together `HAVING sum(Sign) > 0` instead of `sum(x)`
as in the [example](#example-of-use) below.

The aggregates `count`, `sum` and `avg` could be calculated this way.
The aggregate `uniq` could be calculated if an object has at least one non-collapsed state.
The aggregates `min` and `max` could not be calculated
because `CollapsingMergeTree` does not save the history of the collapsed states.

:::note
If you need to extract data without aggregation
(for example, to check whether rows whose newest values match certain conditions are present),
you can use the [`FINAL`](../../../sql-reference/statements/select/from.md#final-modifier) modifier for the `FROM` clause. It will merge the data before returning the result.
For CollapsingMergeTree, only the latest state row for each key is returned.
:::

## Examples {#examples}

### Example of use {#example-of-use}

Given the following example data:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Let's create a table `UAct` using the `CollapsingMergeTree`:

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

Next we will insert some data:

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1),(4324182021466249494, 6, 185, 1)
```

We use two `INSERT` queries to create two different data parts.

:::note
If we insert the data with a single query, ClickHouse creates only one data part and will not perform any merge ever.
:::

We can select the data using:

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Let's take a look at the returned data above and see if collapsing occurred...
With two `INSERT` queries, we created two data parts.
The `SELECT` query was performed in two threads, and we got a random order of rows.
However, collapsing **did not occur** because there was no merge of the data parts yet
and ClickHouse merges data parts in the background at an unknown moment which we cannot predict.

We therefore need an aggregation
which we perform with the [`sum`](/sql-reference/aggregate-functions/reference/sum)
aggregate function and the [`HAVING`](/sql-reference/statements/select/having) clause:

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration
FROM UAct
GROUP BY UserID
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

If we do not need aggregation and want to force collapsing, we can also use the `FINAL` modifier for `FROM` clause.

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```
:::note
This way of selecting the data is less efficient and is not recommended for use with large amounts of scanned data (millions of rows).
:::

### Example of another approach {#example-of-another-approach}

The idea with this approach is that merges take into account only key fields.
In the "cancel" row, we can therefore specify negative values
that equalize the previous version of the row when summing without using the `Sign` column.

For this example, we will make use of the sample data below:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

For this approach, it is necessary to change the data types of `PageViews` and `Duration` to store negative values.
We therefore change the values of these columns from `UInt8` to `Int16` when we create our table `UAct` using the
`collapsingMergeTree`:

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews Int16,
    Duration Int16,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

Let's test the approach by inserting data into our table.

For examples or small tables, it is, however, acceptable:

```sql
INSERT INTO UAct VALUES(4324182021466249494,  5,  146,  1);
INSERT INTO UAct VALUES(4324182021466249494, -5, -146, -1);
INSERT INTO UAct VALUES(4324182021466249494,  6,  185,  1);

SELECT * FROM UAct FINAL;
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

```sql
SELECT
    UserID,
    sum(PageViews) AS PageViews,
    sum(Duration) AS Duration
FROM UAct
GROUP BY UserID
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

```sql
SELECT COUNT() FROM UAct
```

```text
┌─count()─┐
│       3 │
└─────────┘
```

```sql
OPTIMIZE TABLE UAct FINAL;

SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```
)DOCS_MD",
        .syntax = "ENGINE = CollapsingMergeTree(sign) ORDER BY expr",
        .related = {"MergeTree", "VersionedCollapsingMergeTree", "ReplicatedCollapsingMergeTree"}});

    factory.registerStorage("ReplacingMergeTree", create, features, Documentation{
        .description = R"DOCS_MD(
The engine differs from [MergeTree](/engines/table-engines/mergetree-family/versionedcollapsingmergetree) in that it removes duplicate entries with the same [sorting key](../../../engines/table-engines/mergetree-family/mergetree.md) value (`ORDER BY` table section, not `PRIMARY KEY`).

Data deduplication occurs only during a merge. Merging occurs in the background at an unknown time, so you can't plan for it. Some of the data may remain unprocessed. Although you can run an unscheduled merge using the `OPTIMIZE` query, do not count on using it, because the `OPTIMIZE` query will read and write a large amount of data.

Thus, `ReplacingMergeTree` is suitable for clearing out duplicate data in the background in order to save space, but it does not guarantee the absence of duplicates.

:::note
A detailed guide on ReplacingMergeTree, including best practices and how to optimize performance, is available [here](/guides/replacing-merge-tree).
:::

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver [, is_deleted]])
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

For a description of request parameters, see [statement description](../../../sql-reference/statements/create/table.md).

:::note
Uniqueness of rows is determined by the `ORDER BY` table section, not `PRIMARY KEY`.
:::

## ReplacingMergeTree parameters {#replacingmergetree-parameters}

### `ver` {#ver}

`ver` — column with the version number. Type `UInt*`, `Date`, `DateTime` or `DateTime64`. Optional parameter.

When merging, `ReplacingMergeTree` from all the rows with the same sorting key leaves only one:

- The last in the selection, if `ver` not set. A selection is a set of rows in a set of parts participating in the merge. The most recently created part (the last insert) will be the last one in the selection. Thus, after deduplication, the very last row from the most recent insert will remain for each unique sorting key.
- With the maximum version, if `ver` specified. If `ver` is the same for several rows, then it will use "if `ver` is not specified" rule for them, i.e. the most recent inserted row will remain.

Example:

```sql
-- without ver - the last inserted 'wins'
CREATE TABLE myFirstReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree
ORDER BY key;

INSERT INTO myFirstReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO myFirstReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM myFirstReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ second  │ 2020-01-01 00:00:00 │
└─────┴─────────┴─────────────────────┘


-- with ver - the row with the biggest ver 'wins'
CREATE TABLE mySecondReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree(eventTime)
ORDER BY key;

INSERT INTO mySecondReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO mySecondReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM mySecondReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ first   │ 2020-01-01 01:01:01 │
└─────┴─────────┴─────────────────────┘
```

### `is_deleted` {#is_deleted}

`is_deleted` —  Name of a column used during a merge to determine whether the data in this row represents the state or is to be deleted; `1` is a "deleted" row, `0` is a "state" row.

Column data type — `UInt8`.

:::note
`is_deleted` can only be enabled when `ver` is used.

No matter the operation on the data, the version should be increased. If two inserted rows have the same version number, the last inserted row is kept.

By default, ClickHouse will keep the last row for a key even if that row is a delete row. This is so that any future rows with lower versions can
be safely inserted and the delete row will still be applied.

To permanently drop such delete rows, enable the table setting `allow_experimental_replacing_merge_with_cleanup` and either:

1. Set the table settings `enable_replacing_merge_with_cleanup_for_min_age_to_force_merge`, `min_age_to_force_merge_on_partition_only` and `min_age_to_force_merge_seconds`. If all parts in a partition are older than `min_age_to_force_merge_seconds`, ClickHouse will merge them
all into a single part and remove any delete rows.

2. Manually run `OPTIMIZE TABLE table [PARTITION partition | PARTITION ID 'partition_id'] FINAL CLEANUP`.
:::

Example:
```sql
-- with ver and is_deleted
CREATE OR REPLACE TABLE myThirdReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime,
    `is_deleted` UInt8
)
ENGINE = ReplacingMergeTree(eventTime, is_deleted)
ORDER BY key
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 0);
INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 1);

select * from myThirdReplacingMT final;

0 rows in set. Elapsed: 0.003 sec.

-- delete rows with is_deleted
OPTIMIZE TABLE myThirdReplacingMT FINAL CLEANUP;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 00:00:00', 0);

select * from myThirdReplacingMT final;

┌─key─┬─someCol─┬───────────eventTime─┬─is_deleted─┐
│   1 │ first   │ 2020-01-01 00:00:00 │          0 │
└─────┴─────────┴─────────────────────┴────────────┘
```

## Query clauses {#query-clauses}

When creating a `ReplacingMergeTree` table the same [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) are required, as when creating a `MergeTree` table.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::note
Do not use this method in new projects and, if possible, switch old projects to the method described above.
:::

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
```

All of the parameters excepting `ver` have the same meaning as in `MergeTree`.

- `ver` - column with the version. Optional parameter. For a description, see the text above.

</details>

## Query time de-duplication & FINAL {#query-time-de-duplication--final}

At merge time, the ReplacingMergeTree identifies duplicate rows, using the values of the `ORDER BY` columns (used to create the table) as a unique identifier, and retains only the highest version. This, however, offers eventual correctness only - it does not guarantee rows will be deduplicated, and you should not rely on it. Queries can, therefore, produce incorrect answers due to update and delete rows being considered in queries.

To obtain correct answers, users will need to complement background merges with query time deduplication and deletion removal. This can be achieved using the `FINAL` operator. For example, consider the following example:

```sql
CREATE TABLE rmt_example
(
    `number` UInt16
)
ENGINE = ReplacingMergeTree
ORDER BY number

INSERT INTO rmt_example SELECT floor(randUniform(0, 100)) AS number
FROM numbers(1000000000)

0 rows in set. Elapsed: 19.958 sec. Processed 1.00 billion rows, 8.00 GB (50.11 million rows/s., 400.84 MB/s.)
```
Querying without `FINAL` produces an incorrect count (exact result will vary depending on merges):

```sql
SELECT count()
FROM rmt_example

┌─count()─┐
│     200 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

Adding final produces a correct result:

```sql
SELECT count()
FROM rmt_example
FINAL

┌─count()─┐
│     100 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

For further details on `FINAL`, including how to optimize `FINAL` performance, we recommend reading our [detailed guide on ReplacingMergeTree](/guides/replacing-merge-tree).
)DOCS_MD",
        .syntax = "ENGINE = ReplacingMergeTree([ver [, is_deleted]]) ORDER BY expr",
        .related = {"MergeTree", "ReplicatedReplacingMergeTree"}});

    factory.registerStorage("CoalescingMergeTree", create, features, Documentation{
        .description = R"DOCS_MD(
:::note Available from version 25.6
This table engine is available from version 25.6 and higher in both OSS and Cloud.
:::

This engine inherits from [MergeTree](/engines/table-engines/mergetree-family/mergetree). The key difference is in how data parts are merged: for `CoalescingMergeTree` tables, ClickHouse replaces all rows with the same primary key (or more precisely, the same [sorting key](../../../engines/table-engines/mergetree-family/mergetree.md)) with a single row that contains the latest non-NULL values for each column.

This enables column-level upserts, meaning you can update only specific columns rather than entire rows.

`CoalescingMergeTree` is intended for use with Nullable types in non-key columns. If the columns are not Nullable, the behavior is the same as with [ReplacingMergeTree](/engines/table-engines/mergetree-family/replacingmergetree).

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CoalescingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

For a description of request parameters, see [request description](../../../sql-reference/statements/create/table.md).

### Parameters of CoalescingMergeTree {#parameters-of-coalescingmergetree}

#### Columns {#columns}

`columns` - Optional. A tuple with the names of columns where values will be united. The provided columns must not be in the partition or sorting key. If `columns` is not specified, ClickHouse unites the values in all columns that are not in the sorting key.

### Query clauses {#query-clauses}

When creating a `CoalescingMergeTree` table the same [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) are required, as when creating a `MergeTree` table.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::note
Do not use this method in new projects and, if possible, switch the old projects to the method described above.
:::

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] CoalescingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
```

All of the parameters excepting `columns` have the same meaning as in `MergeTree`.

- `columns` — tuple with names of columns values of which will be summed. Optional parameter. For a description, see the text above.

</details>

## Usage example {#usage-example}

Consider the following table:

```sql
CREATE TABLE test_table
(
    key UInt64,
    value_int Nullable(UInt32),
    value_string Nullable(String),
    value_date Nullable(Date)
)
ENGINE = CoalescingMergeTree()
ORDER BY key
```

Insert data to it:

```sql
INSERT INTO test_table VALUES(1, NULL, NULL, '2025-01-01'), (2, 10, 'test', NULL);
INSERT INTO test_table VALUES(1, 42, 'win', '2025-02-01');
INSERT INTO test_table(key, value_date) VALUES(2, '2025-02-01');
```

The result will looks like this:

```sql
SELECT * FROM test_table ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   1 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-01-01 │
│   2 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-02-01 │
│   2 │        10 │ test         │       ᴺᵁᴸᴸ │
└─────┴───────────┴──────────────┴────────────┘
```

Recommended query for correct and final result:

```sql
SELECT * FROM test_table FINAL ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   2 │        10 │ test         │ 2025-02-01 │
└─────┴───────────┴──────────────┴────────────┘
```

Using the `FINAL` modifier forces ClickHouse to apply merge logic at query time, ensuring you get the correct, coalesced "latest" value for each column. This is the safest and most accurate method when querying from a CoalescingMergeTree table.

:::note

An approach with `GROUP BY` may return incorrect results if the underlying parts have not been fully merged.

```sql
SELECT key, last_value(value_int), last_value(value_string), last_value(value_date)  FROM test_table GROUP BY key; -- Not recommended.
```

:::
)DOCS_MD",
        .syntax = "ENGINE = CoalescingMergeTree([columns]) ORDER BY expr",
        .related = {"MergeTree", "SummingMergeTree", "AggregatingMergeTree", "ReplicatedCoalescingMergeTree"}});

    factory.registerStorage("AggregatingMergeTree", create, features, Documentation{
        .description = R"DOCS_MD(
The engine inherits from [MergeTree](/engines/table-engines/mergetree-family/versionedcollapsingmergetree), altering the logic for data parts merging. ClickHouse replaces all rows with the same primary key (or more accurately, with the same [sorting key](../../../engines/table-engines/mergetree-family/mergetree.md)) with a single row (within a single data part) that stores a combination of states of aggregate functions.

You can use `AggregatingMergeTree` tables for incremental data aggregation, including for aggregated materialized views.

You can see an example of how to use the AggregatingMergeTree and Aggregate functions in the below video:
<div class='vimeo-container'>
<iframe width="1030" height="579" src="https://www.youtube.com/embed/pryhI4F_zqQ" title="Aggregation States in ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
</div>

The engine processes all columns with the following types:

- [`AggregateFunction`](../../../sql-reference/data-types/aggregatefunction.md)
- [`SimpleAggregateFunction`](../../../sql-reference/data-types/simpleaggregatefunction.md)

It is appropriate to use `AggregatingMergeTree` if it reduces the number of rows by orders.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = AggregatingMergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[TTL expr]
[SETTINGS name=value, ...]
```

For a description of request parameters, see [request description](../../../sql-reference/statements/create/table.md).

**Query clauses**

When creating an `AggregatingMergeTree` table, the same [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) are required as when creating a `MergeTree` table.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::note
Do not use this method in new projects and, if possible, switch the old projects to the method described above.
:::

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

All of the parameters have the same meaning as in `MergeTree`.
</details>

## SELECT and INSERT {#select-and-insert}

To insert data, use [INSERT SELECT](../../../sql-reference/statements/insert-into.md) query with aggregate -State- functions.
When selecting data from `AggregatingMergeTree` table, use `GROUP BY` clause and the same aggregate functions as when inserting data, but using the `-Merge` suffix.

In the results of `SELECT` query, the values of `AggregateFunction` type have implementation-specific binary representation for all of the ClickHouse output formats. For example, if you dump data into `TabSeparated` format with a `SELECT` query, then this dump can be loaded back using an `INSERT` query.

## Example of an aggregated materialized view {#example-of-an-aggregated-materialized-view}

The following example assumes that you have a database named `test`. Create it if it doesn't already exist using the command below:

```sql
CREATE DATABASE test;
```

Now create the table `test.visits` that contains the raw data:

```sql
CREATE TABLE test.visits
 (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Sign Nullable(Int32),
    UserID Nullable(Int32)
) ENGINE = MergeTree ORDER BY (StartDate, CounterID);
```

Next, you need an `AggregatingMergeTree` table that will store `AggregationFunction`s that keep track of the total number of visits and the number of unique users.

Create an `AggregatingMergeTree` materialized view that watches the `test.visits` table, and uses the [`AggregateFunction`](/sql-reference/data-types/aggregatefunction) type:

```sql
CREATE TABLE test.agg_visits (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Visits AggregateFunction(sum, Nullable(Int32)),
    Users AggregateFunction(uniq, Nullable(Int32))
)
ENGINE = AggregatingMergeTree() ORDER BY (StartDate, CounterID);
```

Create a materialized view that populates `test.agg_visits` from `test.visits`:

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    sumState(Sign) AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY StartDate, CounterID;
```

Insert data into the `test.visits` table:

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
VALUES (1667446031000, 1, 3, 4), (1667446031000, 1, 6, 3);
```

The data is inserted in both `test.visits` and `test.agg_visits`.

To get the aggregated data, execute a query such as `SELECT ... GROUP BY ...` from the materialized view `test.visits_mv`:

```sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.visits_mv
GROUP BY StartDate
ORDER BY StartDate;
```

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │      9 │     2 │
└─────────────────────────┴────────┴───────┘
```

Add another couple of records to `test.visits`, but this time try using a different timestamp for one of the records:

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
VALUES (1669446031000, 2, 5, 10), (1667446031000, 3, 7, 5);
```

Run the `SELECT` query again, which will return the following output:

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │     16 │     3 │
│ 2022-11-26 07:00:31.000 │      5 │     1 │
└─────────────────────────┴────────┴───────┘
```

In some cases, you might want to avoid pre-aggregating rows at insert time to shift the cost of aggregation from insert time
to merge time. Ordinarily, it is necessary to include the columns which are not part of the aggregation in the `GROUP BY`
clause of the materialized view definition to avoid an error. However, you can make use of the [`initializeAggregation`](/sql-reference/functions/other-functions#initializeAggregation)
function with setting `optimize_on_insert = 0` (it is turned on by default) to achieve this. Use of `GROUP BY`
is no longer required in this case:

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    initializeAggregation('sumState', Sign) AS Visits,
    initializeAggregation('uniqState', UserID) AS Users
FROM test.visits;
```

:::note
When using `initializeAggregation`, an aggregate state is created for each individual row without grouping.
Each source row produces one row in the materialized view, and the actual aggregation happens later when the
`AggregatingMergeTree` merges parts. This is only true if `optimize_on_insert = 0`.
:::

## Related content {#related-content}

- Blog: [Using Aggregate Combinators in ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
)DOCS_MD",
        .syntax = "ENGINE = AggregatingMergeTree() ORDER BY expr",
        .related = {"MergeTree", "SummingMergeTree", "ReplicatedAggregatingMergeTree"}});

    factory.registerStorage("SummingMergeTree", create, features, Documentation{
        .description = R"DOCS_MD(
The engine inherits from [MergeTree](/engines/table-engines/mergetree-family/versionedcollapsingmergetree). The difference is that when merging data parts for `SummingMergeTree` tables ClickHouse replaces all the rows with the same primary key (or more accurately, with the same [sorting key](../../../engines/table-engines/mergetree-family/mergetree.md)) with one row which contains summed values for the columns with the numeric data type. If the sorting key is composed in a way that a single key value corresponds to large number of rows, this significantly reduces storage volume and speeds up data selection.

We recommend using the engine together with `MergeTree`. Store complete data in `MergeTree` table, and use `SummingMergeTree` for aggregated data storing, for example, when preparing reports. Such an approach will prevent you from losing valuable data due to an incorrectly composed primary key.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = SummingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

For a description of request parameters, see [request description](../../../sql-reference/statements/create/table.md).

### Parameters of SummingMergeTree {#parameters-of-summingmergetree}

#### Columns {#columns}

`columns` - a tuple with the names of columns where values will be summed. Optional parameter.
    The columns must be of a numeric type and must not be in the partition or sorting key.

If `columns` is not specified, ClickHouse summarizes the values in all columns with a numeric data type that are not in the sorting key.

### Query clauses {#query-clauses}

When creating a `SummingMergeTree` table the same [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) are required, as when creating a `MergeTree` table.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::note
Do not use this method in new projects and, if possible, switch the old projects to the method described above.
:::

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
```

All of the parameters excepting `columns` have the same meaning as in `MergeTree`.

- `columns` — tuple with names of columns values of which will be summed. Optional parameter. For a description, see the text above.

</details>

## Usage example {#usage-example}

Consider the following table:

```sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

Insert data to it:

```sql
INSERT INTO summtt VALUES(1,1),(1,2),(2,1)
```

ClickHouse may sum all the rows not completely ([see below](#data-processing)), so we use an aggregate function `sum` and `GROUP BY` clause in the query.

```sql
SELECT key, sum(value) FROM summtt GROUP BY key
```

```text
┌─key─┬─sum(value)─┐
│   2 │          1 │
│   1 │          3 │
└─────┴────────────┘
```

## Data processing {#data-processing}

When data are inserted into a table, they are saved as-is. ClickHouse merges the inserted parts of data periodically and this is when rows with the same primary key are summed and replaced with one for each resulting part of data.

ClickHouse can merge the data parts so that different resulting parts of data can consist rows with the same primary key, i.e. the summation will be incomplete. Therefore (`SELECT`) an aggregate function [sum()](/sql-reference/aggregate-functions/reference/sum) and `GROUP BY` clause should be used in a query as described in the example above.

### Common rules for summation {#common-rules-for-summation}

The values in the columns with the numeric data type are summed. The set of columns is defined by the parameter `columns`.

If the values were 0 in all of the columns for summation, the row is deleted.

If column is not in the primary key and is not summed, an arbitrary value is selected from the existing ones.

The values are not summed for columns in the primary key.

### The summation in the AggregateFunction columns {#the-summation-in-the-aggregatefunction-columns}

For columns of [AggregateFunction type](../../../sql-reference/data-types/aggregatefunction.md) ClickHouse behaves as [AggregatingMergeTree](../../../engines/table-engines/mergetree-family/aggregatingmergetree.md) engine aggregating according to the function.

### Nested structures {#nested-structures}

Table can have nested data structures that are processed in a special way.

If the name of a nested table ends with `Map` and it contains at least two columns that meet the following criteria:

- the first column is numeric `(*Int*, Date, DateTime)` or a string `(String, FixedString)`, let's call it `key`,
- the other columns are arithmetic `(*Int*, Float32/64)`, let's call it `(values...)`,

then this nested table is interpreted as a mapping of `key => (values...)`, and when merging its rows, the elements of two data sets are merged by `key` with a summation of the corresponding `(values...)`.

Examples:

```text
DROP TABLE IF EXISTS nested_sum;
CREATE TABLE nested_sum
(
    date Date,
    site UInt32,
    hitsMap Nested(
        browser String,
        imps UInt32,
        clicks UInt32
    )
) ENGINE = SummingMergeTree
PRIMARY KEY (date, site);

INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['Firefox', 'Opera'], [10, 5], [2, 1]);
INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['Chrome', 'Firefox'], [20, 1], [1, 1]);
INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['IE'], [22], [0]);
INSERT INTO nested_sum VALUES ('2020-01-01', 10, ['Chrome'], [4], [3]);

OPTIMIZE TABLE nested_sum FINAL; -- emulate merge

SELECT * FROM nested_sum;
┌───────date─┬─site─┬─hitsMap.browser───────────────────┬─hitsMap.imps─┬─hitsMap.clicks─┐
│ 2020-01-01 │   10 │ ['Chrome']                        │ [4]          │ [3]            │
│ 2020-01-01 │   12 │ ['Chrome','Firefox','IE','Opera'] │ [20,11,22,5] │ [1,3,0,1]      │
└────────────┴──────┴───────────────────────────────────┴──────────────┴────────────────┘

SELECT
    site,
    browser,
    impressions,
    clicks
FROM
(
    SELECT
        site,
        sumMap(hitsMap.browser, hitsMap.imps, hitsMap.clicks) AS imps_map
    FROM nested_sum
    GROUP BY site
)
ARRAY JOIN
    imps_map.1 AS browser,
    imps_map.2 AS impressions,
    imps_map.3 AS clicks;

┌─site─┬─browser─┬─impressions─┬─clicks─┐
│   12 │ Chrome  │          20 │      1 │
│   12 │ Firefox │          11 │      3 │
│   12 │ IE      │          22 │      0 │
│   12 │ Opera   │           5 │      1 │
│   10 │ Chrome  │           4 │      3 │
└──────┴─────────┴─────────────┴────────┘
```

When requesting data, use the [sumMap(key, value)](../../../sql-reference/aggregate-functions/reference/sumMappedArrays.md) function for aggregation of `Map`.

For nested data structure, you do not need to specify its columns in the tuple of columns for summation.

## Related content {#related-content}

- Blog: [Using Aggregate Combinators in ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
)DOCS_MD",
        .syntax = "ENGINE = SummingMergeTree([columns]) ORDER BY expr",
        .related = {"MergeTree", "AggregatingMergeTree", "ReplicatedSummingMergeTree"}});

    factory.registerStorage("GraphiteMergeTree", create, features, Documentation{
        .description = R"DOCS_MD(
This engine is designed for thinning and aggregating/averaging (rollup) [Graphite](http://graphite.readthedocs.io/en/latest/index.html) data. It may be helpful to developers who want to use ClickHouse as a data store for Graphite.

You can use any ClickHouse table engine to store the Graphite data if you do not need rollup, but if you need a rollup use `GraphiteMergeTree`. The engine reduces the volume of storage and increases the efficiency of queries from Graphite.

The engine inherits properties from [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md).

## Creating a table {#creating-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    Path String,
    Time DateTime,
    Value Float64,
    Version <Numeric_type>
    ...
) ENGINE = GraphiteMergeTree(config_section)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

See a detailed description of the [CREATE TABLE](/sql-reference/statements/create/table) query.

A table for the Graphite data should have the following columns for the following data:

- Metric name (Graphite sensor). Data type: `String`.

- Time of measuring the metric. Data type: `DateTime`.

- Value of the metric. Data type: `Float64`.

- Version of the metric. Data type: any numeric (ClickHouse saves the rows with the highest version or the last written if versions are the same. Other rows are deleted during the merge of data parts).

The names of these columns should be set in the rollup configuration.

**GraphiteMergeTree parameters**

- `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

**Query clauses**

When creating a `GraphiteMergeTree` table, the same [clauses](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) are required, as when creating a `MergeTree` table.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::note
Do not use this method in new projects and, if possible, switch old projects to the method described above.
:::

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    EventDate Date,
    Path String,
    Time DateTime,
    Value Float64,
    Version <Numeric_type>
    ...
) ENGINE [=] GraphiteMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, config_section)
```

All of the parameters excepting `config_section` have the same meaning as in `MergeTree`.

- `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

</details>

## Rollup configuration {#rollup-configuration}

The settings for rollup are defined by the [graphite_rollup](../../../operations/server-configuration-parameters/settings.md#graphite) parameter in the server configuration. The name of the parameter could be any. You can create several configurations and use them for different tables.

Rollup configuration structure:

      required-columns
      patterns

### Required columns {#required-columns}

#### `path_column_name` {#path_column_name}

`path_column_name` — The name of the column storing the metric name (Graphite sensor). Default value: `Path`.

#### `time_column_name` {#time_column_name}
`time_column_name` — The name of the column storing the time of measuring the metric. Default value: `Time`.

#### `value_column_name` {#value_column_name}
`value_column_name` — The name of the column storing the value of the metric at the time set in `time_column_name`. Default value: `Value`.

#### `version_column_name` {#version_column_name}
`version_column_name` — The name of the column storing the version of the metric. Default value: `Timestamp`.

### Patterns {#patterns}

Structure of the `patterns` section:

```text
pattern
    rule_type
    regexp
    function
pattern
    rule_type
    regexp
    age + precision
    ...
pattern
    rule_type
    regexp
    function
    age + precision
    ...
pattern
    ...
default
    function
    age + precision
    ...
```

:::important
Patterns must be strictly ordered:

1. Patterns without `function` or `retention`.
1. Patterns with both `function` and `retention`.
1. Pattern `default`.
:::

When processing a row, ClickHouse checks the rules in the `pattern` sections. Each of `pattern` (including `default`) sections can contain `function` parameter for aggregation, `retention` parameters or both. If the metric name matches the `regexp`, the rules from the `pattern` section (or sections) are applied; otherwise, the rules from the `default` section are used.

Fields for `pattern` and `default` sections:

- `rule_type` - a rule's type. It's applied only to a particular metrics. The engine use it to separate plain and tagged metrics. Optional parameter. Default value: `all`.
It's unnecessary when performance is not critical, or only one metrics type is used, e.g. plain metrics. By default only one type of rules set is created. Otherwise, if any of special types is defined, two different sets are created. One for plain metrics (root.branch.leaf) and one for tagged metrics (root.branch.leaf;tag1=value1).
The default rules are ended up in both sets.
Valid values:
  - `all` (default) - a universal rule, used when `rule_type` is omitted.
  - `plain` - a rule for plain metrics. The field `regexp` is processed as regular expression.
  - `tagged` - a rule for tagged metrics (metrics are stored in DB in the format of `someName?tag1=value1&tag2=value2&tag3=value3`). Regular expression must be sorted by tags' names, first tag must be `__name__` if exists. The field `regexp` is processed as regular expression.
  - `tag_list` - a rule for tagged metrics, a simple DSL for easier metric description in graphite format `someName;tag1=value1;tag2=value2`, `someName`, or `tag1=value1;tag2=value2`. The field `regexp` is translated into a `tagged` rule. The sorting by tags' names is unnecessary, ti will be done automatically. A tag's value (but not a name) can be set as a regular expression, e.g. `env=(dev|staging)`.
- `regexp` – A pattern for the metric name (a regular or DSL).
- `age` – The minimum age of the data in seconds.
- `precision`– How precisely to define the age of the data in seconds. Should be a divisor for 86400 (seconds in a day).
- `function` – The name of the aggregating function to apply to data whose age falls within the range `[age, age + precision]`. Accepted functions: min / max / any / avg. The average is calculated imprecisely, like the average of the averages.

### Configuration Example without rules types {#configuration-example}

```xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <regexp>click_cost</regexp>
        <function>any</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
```

### Configuration Example with rules types {#configuration-typed-example}

```xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <rule_type>plain</rule_type>
        <regexp>click_cost</regexp>
        <function>any</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp>^((.*)|.)min\?</regexp>
        <function>min</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp><![CDATA[^someName\?(.*&)*tag1=value1(&|$)]]></regexp>
        <function>min</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tag_list</rule_type>
        <regexp>someName;tag2=value2</regexp>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
```

:::note
Data rollup is performed during merges. Usually, for old partitions, merges are not started, so for rollup it is necessary to trigger an unscheduled merge using [optimize](../../../sql-reference/statements/optimize.md). Or use additional tools, for example [graphite-ch-optimizer](https://github.com/innogames/graphite-ch-optimizer).
:::
)DOCS_MD",
        .syntax = "ENGINE = GraphiteMergeTree(config_section) ORDER BY expr",
        .related = {"MergeTree", "ReplicatedGraphiteMergeTree"}});

    factory.registerStorage("VersionedCollapsingMergeTree", create, features, Documentation{
        .description = R"DOCS_MD(
This engine:

- Allows quick writing of object states that are continually changing.
- Deletes old object states in the background. This significantly reduces the volume of storage.

See the section [Collapsing](#table_engines_versionedcollapsingmergetree) for details.

The engine inherits from [MergeTree](/engines/table-engines/mergetree-family/versionedcollapsingmergetree) and adds the logic for collapsing rows to the algorithm for merging data parts. `VersionedCollapsingMergeTree` serves the same purpose as [CollapsingMergeTree](../../../engines/table-engines/mergetree-family/collapsingmergetree.md) but uses a different collapsing algorithm that allows inserting the data in any order with multiple threads. In particular, the `Version` column helps to collapse the rows properly even if they are inserted in the wrong order. In contrast, `CollapsingMergeTree` allows only strictly consecutive insertion.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = VersionedCollapsingMergeTree(sign, version)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

For a description of query parameters, see the [query description](../../../sql-reference/statements/create/table.md).

### Engine parameters {#engine-parameters}

```sql
VersionedCollapsingMergeTree(sign, version)
```

| Parameter | Description                                                                            | Type                                                                                                                                                                                                                                                                                    |
|-----------|----------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `sign`    | Name of the column with the type of row: `1` is a "state" row, `-1` is a "cancel" row. | [`Int8`](/sql-reference/data-types/int-uint)                                                                                                                                                                                                                                    |
| `version` | Name of the column with the version of the object state.                               | [`Int*`](/sql-reference/data-types/int-uint), [`UInt*`](/sql-reference/data-types/int-uint), [`Date`](/sql-reference/data-types/date), [`Date32`](/sql-reference/data-types/date32), [`DateTime`](/sql-reference/data-types/datetime) or [`DateTime64`](/sql-reference/data-types/datetime64) |

### Query clauses {#query-clauses}

When creating a `VersionedCollapsingMergeTree` table, the same [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) are required as when creating a `MergeTree` table.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::note
Do not use this method in new projects. If possible, switch old projects to the method described above.
:::

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] VersionedCollapsingMergeTree(date-column [, samp#table_engines_versionedcollapsingmergetreeling_expression], (primary, key), index_granularity, sign, version)
```

All of the parameters except `sign` and `version` have the same meaning as in `MergeTree`.

- `sign` — Name of the column with the type of row: `1` is a "state" row, `-1` is a "cancel" row.

    Column Data Type — `Int8`.

- `version` — Name of the column with the version of the object state.

    The column data type should be `UInt*`.

</details>

## Collapsing {#table_engines_versionedcollapsingmergetree}

### Data {#data}

Consider a situation where you need to save continually changing data for some object. It is reasonable to have one row for an object and update the row whenever there are changes. However, the update operation is expensive and slow for a DBMS because it requires rewriting the data in the storage. Update is not acceptable if you need to write data quickly, but you can write the changes to an object sequentially as follows.

Use the `Sign` column when writing the row. If `Sign = 1` it means that the row is a state of an object (let's call it the "state" row). If `Sign = -1` it indicates the cancellation of the state of an object with the same attributes (let's call it the "cancel" row). Also use the `Version` column, which should identify each state of an object with a separate number.

For example, we want to calculate how many pages users visited on some site and how long they were there. At some point in time we write the following row with the state of user activity:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

At some point later we register the change of user activity and write it with the following two rows.

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

The first row cancels the previous state of the object (user). It should copy all of the fields of the canceled state except `Sign`.

The second row contains the current state.

Because we need only the last state of user activity, the rows

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

can be deleted, collapsing the invalid (old) state of the object. `VersionedCollapsingMergeTree` does this while merging the data parts.

To find out why we need two rows for each change, see [Algorithm](#table_engines-versionedcollapsingmergetree-algorithm).

**Notes on Usage**

1.  The program that writes the data should remember the state of an object to be able to cancel it. "Cancel" string should contain copies of the primary key fields and the version of the "state" string and the opposite `Sign`. It increases the initial size of storage but allows to write the data quickly.
2.  Long growing arrays in columns reduce the efficiency of the engine due to the load for writing. The more straightforward the data, the better the efficiency.
3.  `SELECT` results depend strongly on the consistency of the history of object changes. Be accurate when preparing data for inserting. You can get unpredictable results with inconsistent data, such as negative values for non-negative metrics like session depth.

### Algorithm {#table_engines-versionedcollapsingmergetree-algorithm}

When ClickHouse merges data parts, it deletes each pair of rows that have the same primary key and version and different `Sign`. The order of rows does not matter.

When ClickHouse inserts data, it orders rows by the primary key. If the `Version` column is not in the primary key, ClickHouse adds it to the primary key implicitly as the last field and uses it for ordering.

## Selecting data {#selecting-data}

ClickHouse does not guarantee that all of the rows with the same primary key will be in the same resulting data part or even on the same physical server. This is true both for writing the data and for subsequent merging of the data parts. In addition, ClickHouse processes `SELECT` queries with multiple threads, and it cannot predict the order of rows in the result. This means that aggregation is required if there is a need to get completely "collapsed" data from a `VersionedCollapsingMergeTree` table.

To finalize collapsing, write a query with a `GROUP BY` clause and aggregate functions that account for the sign. For example, to calculate quantity, use `sum(Sign)` instead of `count()`. To calculate the sum of something, use `sum(Sign * x)` instead of `sum(x)`, and add `HAVING sum(Sign) > 0`.

The aggregates `count`, `sum` and `avg` can be calculated this way. The aggregate `uniq` can be calculated if an object has at least one non-collapsed state. The aggregates `min` and `max` can't be calculated because `VersionedCollapsingMergeTree` does not save the history of values of collapsed states.

If you need to extract the data with "collapsing" but without aggregation (for example, to check whether rows are present whose newest values match certain conditions), you can use the `FINAL` modifier for the `FROM` clause. This approach is inefficient and should not be used with large tables.

## Example of use {#example-of-use}

Example data:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Creating the table:

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8,
    Version UInt8
)
ENGINE = VersionedCollapsingMergeTree(Sign, Version)
ORDER BY UserID
```

Inserting the data:

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2)
```

We use two `INSERT` queries to create two different data parts. If we insert the data with a single query, ClickHouse creates one data part and will never perform any merge.

Getting the data:

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

What do we see here and where are the collapsed parts?
We created two data parts using two `INSERT` queries. The `SELECT` query was performed in two threads, and the result is a random order of rows.
Collapsing did not occur because the data parts have not been merged yet. ClickHouse merges data parts at an unknown point in time which we cannot predict.

This is why we need aggregation:

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration,
    Version
FROM UAct
GROUP BY UserID, Version
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │       2 │
└─────────────────────┴───────────┴──────────┴─────────┘
```

If we do not need aggregation and want to force collapsing, we can use the `FINAL` modifier for the `FROM` clause.

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

This is a very inefficient way to select data. Don't use it for large tables.
)DOCS_MD",
        .syntax = "ENGINE = VersionedCollapsingMergeTree(sign, version) ORDER BY expr",
        .related = {"MergeTree", "CollapsingMergeTree", "ReplicatedVersionedCollapsingMergeTree"}});

    features.supports_replication = true;
    features.supports_deduplication = true;
    features.supports_schema_inference = true;
    features.supports_unique_key = false;

    factory.registerStorage("ReplicatedMergeTree", create, features, Documentation{
        .description = R"DOCS_MD(
:::note
In ClickHouse Cloud replication is managed for you. Please create your tables without adding arguments.  For example, in the text below you would replace:

```sql
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/table_name',
    '{replica}'
)
```

with:

```sql
ENGINE = ReplicatedMergeTree
```
:::

Replication is only supported for tables in the MergeTree family

- ReplicatedSummingMergeTree
- ReplicatedCoalescingMergeTree
- ReplicatedVersionedCollapsingMergeTree
- ReplicatedCollapsingMergeTree
- ReplicatedGraphiteMergeTree
- ReplicatedMergeTree
- ReplicatedReplacingMergeTree
- ReplicatedAggregatingMergeTree

Replication works at the level of an individual table, not the entire server. A server can store both replicated and non-replicated tables at the same time.

Replication does not depend on sharding. Each shard has its own independent replication.

Compressed data for `INSERT` and `ALTER` queries is replicated (for more information, see the documentation for [ALTER](/sql-reference/statements/alter).

`CREATE`, `DROP`, `ATTACH`, `DETACH` and `RENAME` queries are executed on a single server and are not replicated:

- The `CREATE TABLE` query creates a new replicatable table on the server where the query is run. If this table already exists on other servers, it adds a new replica.
- The `DROP TABLE` query deletes the replica located on the server where the query is run.
- The `RENAME` query renames the table on one of the replicas. In other words, replicated tables can have different names on different replicas.

ClickHouse uses [ClickHouse Keeper](/guides/sre/keeper/index.md) for storing replicas meta information. It is possible to use ZooKeeper version 3.4.5 or newer, but ClickHouse Keeper is recommended.

To use replication, set parameters in the [zookeeper](/operations/server-configuration-parameters/settings#zookeeper) server configuration section.

:::note
Don't neglect the security setting. ClickHouse supports the `digest` [ACL scheme](https://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#sc_ZooKeeperAccessControl) of the ZooKeeper security subsystem.
:::

Example of setting the addresses of the ClickHouse Keeper cluster:

```xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <node>
        <host>example3</host>
        <port>2181</port>
    </node>
</zookeeper>
```

ClickHouse also supports storing replicas meta information in an auxiliary ZooKeeper cluster. Do this by providing the ZooKeeper cluster name and path as engine arguments.
In other words, it supports storing the metadata of different tables in different ZooKeeper clusters.

Example of setting the addresses of the auxiliary ZooKeeper cluster:

```xml
<auxiliary_zookeepers>
    <zookeeper2>
        <node>
            <host>example_2_1</host>
            <port>2181</port>
        </node>
        <node>
            <host>example_2_2</host>
            <port>2181</port>
        </node>
        <node>
            <host>example_2_3</host>
            <port>2181</port>
        </node>
    </zookeeper2>
    <zookeeper3>
        <node>
            <host>example_3_1</host>
            <port>2181</port>
        </node>
    </zookeeper3>
</auxiliary_zookeepers>
```

To store table metadata in an auxiliary ZooKeeper cluster instead of the default ZooKeeper cluster, we can use SQL to create the table with
ReplicatedMergeTree engine as follows:

```sql
CREATE TABLE table_name (...) ENGINE = ReplicatedMergeTree('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'replica_name') ...
```
You can specify any existing ZooKeeper cluster and the system will use a directory on it for its own data (the directory is specified when creating a replicatable table).

If ZooKeeper is not set in the config file, you can't create replicated tables, and any existing replicated tables will be read-only.

ZooKeeper is not used in `SELECT` queries because replication does not affect the performance of `SELECT` and queries run just as fast as they do for non-replicated tables. When querying distributed replicated tables, ClickHouse behavior is controlled by the settings [max_replica_delay_for_distributed_queries](/operations/settings/settings.md/#max_replica_delay_for_distributed_queries) and [fallback_to_stale_replicas_for_distributed_queries](/operations/settings/settings.md/#fallback_to_stale_replicas_for_distributed_queries).

For each `INSERT` query, approximately ten entries are added to ZooKeeper through several transactions. (To be more precise, this is for each inserted block of data; an INSERT query contains one block or one block per `max_insert_block_size = 1048576` rows.) This leads to slightly longer latencies for `INSERT` compared to non-replicated tables. But if you follow the recommendations to insert data in batches of no more than one `INSERT` per second, it does not create any problems. The entire ClickHouse cluster used for coordinating one ZooKeeper cluster has a total of several hundred `INSERTs` per second. The throughput on data inserts (the number of rows per second) is just as high as for non-replicated data.

For very large clusters, you can use different ZooKeeper clusters for different shards. However, from our experience this has not proven necessary based on production clusters with approximately 300 servers.

Replication is asynchronous and multi-master. `INSERT` queries (as well as `ALTER`) can be sent to any available server. Data is inserted on the server where the query is run, and then it is copied to the other servers. Because it is asynchronous, recently inserted data appears on the other replicas with some latency. If part of the replicas are not available, the data is written when they become available. If a replica is available, the latency is the amount of time it takes to transfer the block of compressed data over the network. The number of threads performing background tasks for replicated tables can be set by [background_schedule_pool_size](/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size) setting.

`ReplicatedMergeTree` engine uses a separate thread pool for replicated fetches. Size of the pool is limited by the [background_fetches_pool_size](/operations/server-configuration-parameters/settings#background_fetches_pool_size) setting which can be tuned with a server restart.

By default, an INSERT query waits for confirmation of writing the data from only one replica. If the data was successfully written to only one replica and the server with this replica ceases to exist, the stored data will be lost. To enable getting confirmation of data writes from multiple replicas, use the `insert_quorum` option.

Each block of data is written atomically. The INSERT query is divided into blocks up to `max_insert_block_size = 1048576` rows. In other words, if the `INSERT` query has less than 1048576 rows, it is made atomically.

Data blocks are deduplicated. For multiple writes of the same data block (data blocks of the same size containing the same rows in the same order), the block is only written once. The reason for this is in case of network failures when the client application does not know if the data was written to the DB, so the `INSERT` query can simply be repeated. It does not matter which replica INSERTs were sent to with identical data. `INSERTs` are idempotent. Deduplication parameters are controlled by [merge_tree](/operations/server-configuration-parameters/settings.md/#merge_tree) server settings.

During replication, only the source data to insert is transferred over the network. Further data transformation (merging) is coordinated and performed on all the replicas in the same way. This minimizes network usage, which means that replication works well when replicas reside in different datacenters. (Note that duplicating data in different datacenters is the main goal of replication.)

You can have any number of replicas of the same data. Based on our experiences, a relatively reliable and convenient solution could use double replication in production, with each server using RAID-5 or RAID-6 (and RAID-10 in some cases).

The system monitors data synchronicity on replicas and is able to recover after a failure. Failover is automatic (for small differences in data) or semi-automatic (when data differs too much, which may indicate a configuration error).

## Creating replicated tables {#creating-replicated-tables}

:::note
In ClickHouse Cloud, replication is handled automatically.

Create tables using [`MergeTree`](/engines/table-engines/mergetree-family/mergetree) without replication arguments. The system internally rewrites [`MergeTree`](/engines/table-engines/mergetree-family/mergetree) to [`SharedMergeTree`](/cloud/reference/shared-merge-tree) for replication and data distribution.

Avoid using `ReplicatedMergeTree` or specifying replication parameters, as replication is managed by the platform.

:::

### Replicated\*MergeTree parameters {#replicatedmergetree-parameters}

| Parameter       | Description                                                                  |
|-----------------|------------------------------------------------------------------------------|
| `zoo_path`      | The path to the table in ClickHouse Keeper.                                  |
| `replica_name`  | The replica name in ClickHouse Keeper.                                       |
| `other_parameters` | Parameters of an engine used for creating the replicated version, for example, version in `ReplacingMergeTree`. |

Example:

```sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32,
    ver UInt16
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', ver)
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID);
```

<details markdown="1">

<summary>Example in deprecated syntax</summary>

```sql
CREATE TABLE table_name
(
    EventDate DateTime,
    CounterID UInt32,
    UserID UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/table_name', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192);
```

</details>

As the example shows, these parameters can contain substitutions in `{}`. The substituted values are taken from the [macros](/operations/server-configuration-parameters/settings.md/#macros) section of the configuration file.

Example:

```xml
<macros>
    <shard>02</shard>
    <replica>example05-02-1</replica>
</macros>
```

The path to the table in ClickHouse Keeper should be unique for each replicated table. Tables on different shards should have different paths.
In this case, the path consists of the following parts:

`/clickhouse/tables/` is the common prefix. We recommend using exactly this one.

`{shard}` will be expanded to the shard identifier.

`table_name` is the name of the node for the table in ClickHouse Keeper. It is a good idea to make it the same as the table name. It is defined explicitly, because in contrast to the table name, it does not change after a RENAME query.
*HINT*: you could add a database name in front of `table_name` as well. E.g. `db_name.table_name`

The two built-in substitutions `{database}` and `{table}` can be used, they expand into the table name and the database name respectively (unless these macros are defined in the `macros` section). So the zookeeper path can be specified as `'/clickhouse/tables/{shard}/{database}/{table}'`.
Be careful with table renames when using these built-in substitutions. The path in ClickHouse Keeper cannot be changed, and when the table is renamed, the macros will expand into a different path, the table will refer to a path that does not exist in ClickHouse Keeper, and will go into read-only mode.

The replica name identifies different replicas of the same table. You can use the server name for this, as in the example. The name only needs to be unique within each shard.

You can define the parameters explicitly instead of using substitutions. This might be convenient for testing and for configuring small clusters. However, you can't use distributed DDL queries (`ON CLUSTER`) in this case.

When working with large clusters, we recommend using substitutions because they reduce the probability of error.

You can specify default arguments for `Replicated` table engine in the server configuration file. For instance:

```xml
<default_replica_path>/clickhouse/tables/{shard}/{database}/{table}</default_replica_path>
<default_replica_name>{replica}</default_replica_name>
```

In this case, you can omit arguments when creating tables:

```sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree
ORDER BY x;
```

It is equivalent to:

```sql
CREATE TABLE table_name (
    x UInt32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/table_name', '{replica}')
ORDER BY x;
```

Run the `CREATE TABLE` query on each replica. This query creates a new replicated table, or adds a new replica to an existing one.

If you add a new replica after the table already contains some data on other replicas, the data will be copied from the other replicas to the new one after running the query. In other words, the new replica syncs itself with the others.

To delete a replica, run `DROP TABLE`. However, only one replica is deleted – the one that resides on the server where you run the query.

## Recovery after failures {#recovery-after-failures}

If ClickHouse Keeper is unavailable when a server starts, replicated tables switch to read-only mode. The system periodically attempts to connect to ClickHouse Keeper.

If ClickHouse Keeper is unavailable during an `INSERT`, or an error occurs when interacting with ClickHouse Keeper, an exception is thrown.

After connecting to ClickHouse Keeper, the system checks whether the set of data in the local file system matches the expected set of data (ClickHouse Keeper stores this information). If there are minor inconsistencies, the system resolves them by syncing data with the replicas.

If the system detects broken data parts (with the wrong size of files) or unrecognized parts (parts written to the file system but not recorded in ClickHouse Keeper), it moves them to the `detached` subdirectory (they are not deleted). Any missing parts are copied from the replicas.

Note that ClickHouse does not perform any destructive actions such as automatically deleting a large amount of data.

When the server starts (or establishes a new session with ClickHouse Keeper), it only checks the quantity and sizes of all files. If the file sizes match but bytes have been changed somewhere in the middle, this is not detected immediately, but only when attempting to read the data for a `SELECT` query. The query throws an exception about a non-matching checksum or size of a compressed block. In this case, data parts are added to the verification queue and copied from the replicas if necessary.

If the local set of data differs too much from the expected one, a safety mechanism is triggered. The server enters this in the log and refuses to launch. The reason for this is that this case may indicate a configuration error, such as if a replica on a shard was accidentally configured like a replica on a different shard. However, the thresholds for this mechanism are set fairly low, and this situation might occur during normal failure recovery. In this case, data is restored semi-automatically - by "pushing a button".

To start recovery, create the node `/path_to_table/replica_name/flags/force_restore_data` in ClickHouse Keeper with any content, or run the command to restore all replicated tables:

```bash
sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data
```

Then restart the server. On start, the server deletes these flags and starts recovery.

## Recovery after complete data loss {#recovery-after-complete-data-loss}

If all data and metadata disappeared from one of the servers, follow these steps for recovery:

1.  Install ClickHouse on the server. Define substitutions correctly in the config file that contains the shard identifier and replicas, if you use them.
2.  If you had unreplicated tables that must be manually duplicated on the servers, copy their data from a replica (in the directory `/var/lib/clickhouse/data/db_name/table_name/`).
3.  Copy table definitions located in `/var/lib/clickhouse/metadata/` from a replica. If a shard or replica identifier is defined explicitly in the table definitions, correct it so that it corresponds to this replica. (Alternatively, start the server and make all the `ATTACH TABLE` queries that should have been in the .sql files in `/var/lib/clickhouse/metadata/`.)
4.  To start recovery, create the ClickHouse Keeper node `/path_to_table/replica_name/flags/force_restore_data` with any content, or run the command to restore all replicated tables: `sudo -u clickhouse touch /var/lib/clickhouse/flags/force_restore_data`

Then start the server (restart, if it is already running). Data will be downloaded from replicas.

An alternative recovery option is to delete information about the lost replica from ClickHouse Keeper (`/path_to_table/replica_name`), then create the replica again as described in "[Creating replicated tables](#creating-replicated-tables)".

There is no restriction on network bandwidth during recovery. Keep this in mind if you are restoring many replicas at once.

## Converting from MergeTree to ReplicatedMergeTree {#converting-from-mergetree-to-replicatedmergetree}

We use the term `MergeTree` to refer to all table engines in the `MergeTree family`, the same as for `ReplicatedMergeTree`.

If you had a `MergeTree` table that was manually replicated, you can convert it to a replicated table. You might need to do this if you have already collected a large amount of data in a `MergeTree` table and now you want to enable replication.

[ATTACH TABLE ... AS REPLICATED](/sql-reference/statements/attach.md#attach-mergetree-table-as-replicatedmergetree) statement allows to attach detached `MergeTree` table as `ReplicatedMergeTree`.

`MergeTree` table can be automatically converted on server restart if `convert_to_replicated` flag is set at the table's data directory (`/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/` for `Atomic` database).
Create empty `convert_to_replicated` file and the table will be loaded as replicated on next server restart.

This query can be used to get the table's data path. If table has many data paths, you have to use the first one.

```sql
SELECT data_paths FROM system.tables WHERE table = 'table_name' AND database = 'database_name';
```

Note that ReplicatedMergeTree table will be created with values of `default_replica_path` and `default_replica_name` settings.
To create a converted table on other replicas, you will need to explicitly specify its path in the first argument of the `ReplicatedMergeTree` engine. The following query can be used to get its path.

```sql
SELECT zookeeper_path FROM system.replicas WHERE table = 'table_name';
```

There is also a manual way to do this.

If the data differs on various replicas, first sync it, or delete this data on all the replicas except one.

Rename the existing MergeTree table, then create a `ReplicatedMergeTree` table with the old name.
Move the data from the old table to the `detached` subdirectory inside the directory with the new table data (`/var/lib/clickhouse/data/db_name/table_name/`).
Then run `ALTER TABLE ATTACH PARTITION` on one of the replicas to add these data parts to the working set.

## Converting from ReplicatedMergeTree to MergeTree {#converting-from-replicatedmergetree-to-mergetree}

Use [ATTACH TABLE ... AS NOT REPLICATED](/sql-reference/statements/attach.md#attach-mergetree-table-as-replicatedmergetree) statement to attach detached `ReplicatedMergeTree` table as `MergeTree` on a single server.

Another way to do this involves server restart. Create a MergeTree table with a different name. Move all the data from the directory with the `ReplicatedMergeTree` table data to the new table's data directory. Then delete the `ReplicatedMergeTree` table and restart the server.

If you want to get rid of a `ReplicatedMergeTree` table without launching the server:

- Delete the corresponding `.sql` file in the metadata directory (`/var/lib/clickhouse/metadata/`).
- Delete the corresponding path in ClickHouse Keeper (`/path_to_table/replica_name`).

After this, you can launch the server, create a `MergeTree` table, move the data to its directory, and then restart the server.

## Recovery when metadata in the ClickHouse Keeper cluster is lost or damaged {#recovery-when-metadata-in-the-zookeeper-cluster-is-lost-or-damaged}

If the data in ClickHouse Keeper was lost or damaged, you can save data by moving it to an unreplicated table as described above.

**See Also**

- [background_schedule_pool_size](/operations/server-configuration-parameters/settings.md/#background_schedule_pool_size)
- [background_fetches_pool_size](/operations/server-configuration-parameters/settings.md/#background_fetches_pool_size)
- [execute_merges_on_single_replica_time_threshold](/operations/settings/merge-tree-settings#execute_merges_on_single_replica_time_threshold)
- [max_replicated_fetches_network_bandwidth](/operations/settings/merge-tree-settings.md/#max_replicated_fetches_network_bandwidth)
- [max_replicated_sends_network_bandwidth](/operations/settings/merge-tree-settings.md/#max_replicated_sends_network_bandwidth)
)DOCS_MD",
        .syntax = "ENGINE = ReplicatedMergeTree('zoo_path', 'replica_name') ORDER BY expr",
        .related = {"MergeTree"}});

    factory.registerStorage("ReplicatedCollapsingMergeTree", create, features, Documentation{
        .description = "Replicated version of the CollapsingMergeTree engine.",
        .syntax = "ENGINE = ReplicatedCollapsingMergeTree('zoo_path', 'replica_name', sign) ORDER BY expr",
        .related = {"CollapsingMergeTree"}});

    factory.registerStorage("ReplicatedReplacingMergeTree", create, features, Documentation{
        .description = "Replicated version of the ReplacingMergeTree engine.",
        .syntax = "ENGINE = ReplicatedReplacingMergeTree('zoo_path', 'replica_name'[, ver [, is_deleted]]) ORDER BY expr",
        .related = {"ReplacingMergeTree"}});

    factory.registerStorage("ReplicatedAggregatingMergeTree", create, features, Documentation{
        .description = "Replicated version of the AggregatingMergeTree engine.",
        .syntax = "ENGINE = ReplicatedAggregatingMergeTree('zoo_path', 'replica_name') ORDER BY expr",
        .related = {"AggregatingMergeTree"}});

    factory.registerStorage("ReplicatedSummingMergeTree", create, features, Documentation{
        .description = "Replicated version of the SummingMergeTree engine.",
        .syntax = "ENGINE = ReplicatedSummingMergeTree('zoo_path', 'replica_name'[, columns]) ORDER BY expr",
        .related = {"SummingMergeTree"}});

    factory.registerStorage("ReplicatedCoalescingMergeTree", create, features, Documentation{
        .description = "Replicated version of the CoalescingMergeTree engine.",
        .syntax = "ENGINE = ReplicatedCoalescingMergeTree('zoo_path', 'replica_name'[, columns]) ORDER BY expr",
        .related = {"CoalescingMergeTree"}});

    factory.registerStorage("ReplicatedGraphiteMergeTree", create, features, Documentation{
        .description = "Replicated version of the GraphiteMergeTree engine.",
        .syntax = "ENGINE = ReplicatedGraphiteMergeTree('zoo_path', 'replica_name', config_section) ORDER BY expr",
        .related = {"GraphiteMergeTree"}});

    factory.registerStorage("ReplicatedVersionedCollapsingMergeTree", create, features, Documentation{
        .description = "Replicated version of the VersionedCollapsingMergeTree engine.",
        .syntax = "ENGINE = ReplicatedVersionedCollapsingMergeTree('zoo_path', 'replica_name', sign, version) ORDER BY expr",
        .related = {"VersionedCollapsingMergeTree"}});
}

}
