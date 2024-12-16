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
#include <Common/Macros.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/DDLTask.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_deprecated_syntax_for_merge_tree;
    extern const SettingsBool allow_suspicious_primary_key;
    extern const SettingsBool allow_suspicious_ttl_expressions;
    extern const SettingsBool create_table_empty_primary_key_by_default;
    extern const SettingsUInt64 database_replicated_allow_replicated_engine_arguments;
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
    else
    {
        return {getIdentifierName(node)};
    }
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

See details in documentation: https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/. Other engines of the family support different syntax, see details in the corresponding documentation topics.

If you use the Replicated version of engines, see https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication/.
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
            arg = std::make_shared<ASTLiteral>(value);
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

    bool is_extended_storage_def = isExtendedStorageDef(query);

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
        bool is_replicated_database = local_context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY &&
            DatabaseCatalog::instance().getDatabase(table_id.database_name)->getEngineName() == "Replicated";

        if (!query.attach && is_replicated_database && local_context->getSettingsRef()[Setting::database_replicated_allow_replicated_engine_arguments] == 0)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "It's not allowed to specify explicit zookeeper_path and replica_name "
                            "for ReplicatedMergeTree arguments in Replicated database. If you really want to "
                            "specify them explicitly, enable setting "
                            "database_replicated_allow_replicated_engine_arguments.");
        }
        else if (!query.attach && is_replicated_database && local_context->getSettingsRef()[Setting::database_replicated_allow_replicated_engine_arguments] == 1)
        {
            LOG_WARNING(&Poco::Logger::get("registerStorageMergeTree"), "It's not recommended to explicitly specify "
                                                            "zookeeper_path and replica_name in ReplicatedMergeTree arguments");
        }

        /// Get path and name from engine arguments
        auto * ast_zk_path = engine_args[arg_num]->as<ASTLiteral>();
        if (!ast_zk_path || ast_zk_path->value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path in ZooKeeper must be a string literal{}", verbose_help_message);

        auto * ast_replica_name = engine_args[arg_num + 1]->as<ASTLiteral>();
        if (!ast_replica_name || ast_replica_name->value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replica name must be a string literal{}", verbose_help_message);

        if (!query.attach && is_replicated_database && local_context->getSettingsRef()[Setting::database_replicated_allow_replicated_engine_arguments] == 2)
        {
            LOG_WARNING(&Poco::Logger::get("registerStorageMergeTree"), "Replacing user-provided ZooKeeper path and replica name ({}, {}) "
                                                                     "with default arguments", ast_zk_path->value.safeGet<String>(), ast_replica_name->value.safeGet<String>());
            ast_zk_path->value = server_settings.default_replica_path;
            ast_replica_name->value = server_settings.default_replica_name;
        }

        return expand_macro(ast_zk_path, ast_replica_name, ast_zk_path->value.safeGet<String>(), ast_replica_name->value.safeGet<String>());
    }
    else if (is_extended_storage_def
        && (arg_cnt == 0
            || !engine_args[arg_num]->as<ASTLiteral>()
            || (arg_cnt == 1 && (getNamePart(engine_name) == "Graphite"))))
    {
        /// Try use default values if arguments are not specified.
        /// Note: {uuid} macro works for ON CLUSTER queries when database engine is Atomic.
        /// TODO maybe use hostname if {replica} is not defined?

        /// Modify query, so default values will be written to metadata
        assert(arg_num == 0);
        ASTs old_args;
        std::swap(engine_args, old_args);
        auto path_arg = std::make_shared<ASTLiteral>("");
        auto name_arg = std::make_shared<ASTLiteral>("");
        auto * ast_zk_path = path_arg.get();
        auto * ast_replica_name = name_arg.get();

        auto res = expand_macro(ast_zk_path, ast_replica_name, server_settings.default_replica_path, server_settings.default_replica_name);

        engine_args.emplace_back(std::move(path_arg));
        engine_args.emplace_back(std::move(name_arg));
        std::move(std::begin(old_args), std::end(old_args), std::back_inserter(engine_args));

        return res;
    }
    else
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

    bool is_extended_storage_def = isExtendedStorageDef(args.query);

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
        else
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "ORDER BY or PRIMARY KEY clause is missing. "
                            "Consider using extended storage definition syntax with ORDER BY or PRIMARY KEY clause. "
                            "With deprecated old syntax (highly not recommended) storage {} requires {}{}",
                            args.engine_name, msg, verbose_help_message);
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
        if (zookeeper_info.replica_name.find('\t') != String::npos || zookeeper_info.replica_name.find('\n') != String::npos)
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
        metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_key, metadata.columns, context);

        /// PRIMARY KEY without ORDER BY is allowed and considered as ORDER BY.
        if (!args.storage_def->order_by && args.storage_def->primary_key)
            args.storage_def->set(args.storage_def->order_by, args.storage_def->primary_key->clone());

        if (!args.storage_def->order_by)
        {
            if (local_settings[Setting::create_table_empty_primary_key_by_default])
            {
                args.storage_def->set(args.storage_def->order_by, makeASTFunction("tuple"));
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
        metadata.sorting_key = KeyDescription::getSortingKeyFromAST(
            args.storage_def->order_by->ptr(), metadata.columns, context, merging_param_key_arg);
        if (!local_settings[Setting::allow_suspicious_primary_key] && args.mode <= LoadingStrictnessLevel::CREATE)
            MergeTreeData::verifySortingKey(metadata.sorting_key);

        /// If primary key explicitly defined, than get it from AST
        if (args.storage_def->primary_key)
        {
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, context);
        }
        else /// Otherwise we don't have explicit primary key and copy it from order by
        {
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->order_by->ptr(), metadata.columns, context);
            /// and set it's definition_ast to nullptr (so isPrimaryKeyDefined()
            /// will return false but hasPrimaryKey() will return true.
            metadata.primary_key.definition_ast = nullptr;
        }

        auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
        auto partition_key = metadata.partition_key.expression_list_ast->clone();
        FunctionNameNormalizer::visit(partition_key.get());
        auto primary_key_asts = metadata.primary_key.expression_list_ast->children;
        metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
            columns, partition_key, minmax_columns, primary_key_asts, context));

        if (args.storage_def->sample_by)
            metadata.sampling_key = KeyDescription::getKeyFromAST(args.storage_def->sample_by->ptr(), metadata.columns, context);

        bool allow_suspicious_ttl
            = LoadingStrictnessLevel::SECONDARY_CREATE <= args.mode || local_settings[Setting::allow_suspicious_ttl_expressions];

        if (args.storage_def->ttl_table)
        {
            metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
                args.storage_def->ttl_table->ptr(), metadata.columns, context, metadata.primary_key, allow_suspicious_ttl);
        }

        if (args.query.columns_list && args.query.columns_list->indices)
            for (auto & index : args.query.columns_list->indices->children)
                metadata.secondary_indices.push_back(IndexDescription::getIndexFromAST(index, columns, context));

        if (args.query.columns_list && args.query.columns_list->projections)
            for (auto & projection_ast : args.query.columns_list->projections->children)
            {
                auto projection = ProjectionDescription::getProjectionFromAST(projection_ast, columns, context);
                metadata.projections.add(std::move(projection));
            }


        auto column_ttl_asts = columns.getColumnTTLs();
        for (const auto & [name, ast] : column_ttl_asts)
        {
            auto new_ttl_entry = TTLDescription::getTTLFromAST(ast, columns, context, metadata.primary_key, allow_suspicious_ttl);
            metadata.column_ttls_by_name[name] = new_ttl_entry;
        }

        storage_settings->loadFromQuery(*args.storage_def, context, LoadingStrictnessLevel::ATTACH <= args.mode);

        // updates the default storage_settings with settings specified via SETTINGS arg in a query
        if (args.storage_def->settings)
        {
            if (args.mode <= LoadingStrictnessLevel::CREATE)
                args.getLocalContext()->checkMergeTreeSettingsConstraints(initial_storage_settings, storage_settings->changes());
            metadata.settings_changes = args.storage_def->settings->ptr();
        }

        auto constraints = metadata.constraints.getConstraints();
        if (args.query.columns_list && args.query.columns_list->constraints)
            for (auto & constraint : args.query.columns_list->constraints->children)
                constraints.push_back(constraint);
        if ((merging_params.mode == MergeTreeData::MergingParams::Collapsing ||
            merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing) &&
            storage_settings->add_implicit_sign_column_constraint_for_collapsing_engine)
        {
            auto sign_column_check_constraint = std::make_unique<ASTConstraintDeclaration>();
            sign_column_check_constraint->name = "check_sign_column";
            sign_column_check_constraint->type = ASTConstraintDeclaration::Type::CHECK;

            Array valid_values_array;
            valid_values_array.emplace_back(-1);
            valid_values_array.emplace_back(1);

            auto valid_values_ast = std::make_unique<ASTLiteral>(std::move(valid_values_array));
            auto sign_column_ast = std::make_unique<ASTIdentifier>(merging_params.sign_column);
            sign_column_check_constraint->set(sign_column_check_constraint->expr, makeASTFunction("in", std::move(sign_column_ast), std::move(valid_values_ast)));

            constraints.push_back(std::move(sign_column_check_constraint));
        }
        metadata.constraints = ConstraintsDescription(constraints);
    }
    else
    {
        /// Syntax: *MergeTree(..., date, [sample_key], primary_key, index_granularity, ...)
        /// Get date:
        if (!tryGetIdentifierNameInto(engine_args[arg_num], date_column_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Date column name must be an unquoted string{}", verbose_help_message);

        auto partition_by_ast = makeASTFunction("toYYYYMM", std::make_shared<ASTIdentifier>(date_column_name));

        metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_ast, metadata.columns, context);

        ++arg_num;

        /// If there is an expression for sampling
        if (arg_cnt - arg_num == 3)
        {
            metadata.sampling_key = KeyDescription::getKeyFromAST(engine_args[arg_num], metadata.columns, context);
            ++arg_num;
        }

        /// Get sorting key from engine arguments.
        ///
        /// NOTE: store merging_param_key_arg as additional key column. We do it
        /// before storage creation. After that storage will just copy this
        /// column if sorting key will be changed.
        metadata.sorting_key
            = KeyDescription::getSortingKeyFromAST(engine_args[arg_num], metadata.columns, context, merging_param_key_arg);
        if (!local_settings[Setting::allow_suspicious_primary_key] && args.mode <= LoadingStrictnessLevel::CREATE)
            MergeTreeData::verifySortingKey(metadata.sorting_key);

        /// In old syntax primary_key always equals to sorting key.
        metadata.primary_key = KeyDescription::getKeyFromAST(engine_args[arg_num], metadata.columns, context);
        /// But it's not explicitly defined, so we evaluate definition to
        /// nullptr
        metadata.primary_key.definition_ast = nullptr;

        ++arg_num;

        auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
        auto partition_key = metadata.partition_key.expression_list_ast->clone();
        FunctionNameNormalizer::visit(partition_key.get());
        auto primary_key_asts = metadata.primary_key.expression_list_ast->children;
        metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
            columns, partition_key, minmax_columns, primary_key_asts, context));

        const auto * ast = engine_args[arg_num]->as<ASTLiteral>();
        if (ast && ast->value.getType() == Field::Types::UInt64)
        {
            storage_settings->index_granularity = ast->value.safeGet<UInt64>();
            if (args.mode <= LoadingStrictnessLevel::CREATE)
            {
                SettingsChanges changes;
                changes.emplace_back("index_granularity", Field(storage_settings->index_granularity));
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
    if (args.mode <= LoadingStrictnessLevel::CREATE && !storage_settings->allow_floating_point_partition_key)
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
            && storage_settings->deduplicate_merge_projection_mode == DeduplicateMergeProjectionMode::THROW)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Projection is fully supported in {}MergeTree with deduplicate_merge_projection_mode = throw. "
                "Use 'drop' or 'rebuild' option of deduplicate_merge_projection_mode.",
                merging_params.getModeName());
    }

    if (arg_num != arg_cnt)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong number of engine arguments.");

    if (replicated)
    {
        bool need_check_table_structure = true;
        if (auto txn = args.getLocalContext()->getZooKeeperMetadataTransaction())
            need_check_table_structure = txn->isInitialQuery();

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
            need_check_table_structure);
    }
    else
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
