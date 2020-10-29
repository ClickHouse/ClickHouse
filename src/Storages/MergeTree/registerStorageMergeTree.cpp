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


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_STORAGE;
    extern const int NO_REPLICA_NAME_GIVEN;
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

/** Read the settings for Graphite rollup from config.
  * Example
  *
  * <graphite_rollup>
  *     <path_column_name>Path</path_column_name>
  *     <pattern>
  *         <regexp>click_cost</regexp>
  *         <function>any</function>
  *         <retention>
  *             <age>0</age>
  *             <precision>3600</precision>
  *         </retention>
  *         <retention>
  *             <age>86400</age>
  *             <precision>60</precision>
  *         </retention>
  *     </pattern>
  *     <default>
  *         <function>max</function>
  *         <retention>
  *             <age>0</age>
  *             <precision>60</precision>
  *         </retention>
  *         <retention>
  *             <age>3600</age>
  *             <precision>300</precision>
  *         </retention>
  *         <retention>
  *             <age>86400</age>
  *             <precision>3600</precision>
  *         </retention>
  *     </default>
  * </graphite_rollup>
  */
static void
appendGraphitePattern(const Poco::Util::AbstractConfiguration & config, const String & config_element, Graphite::Patterns & patterns)
{
    Graphite::Pattern pattern;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_element, keys);

    for (const auto & key : keys)
    {
        if (key == "regexp")
        {
            pattern.regexp_str = config.getString(config_element + ".regexp");
            pattern.regexp = std::make_shared<OptimizedRegularExpression>(pattern.regexp_str);
        }
        else if (key == "function")
        {
            String aggregate_function_name_with_params = config.getString(config_element + ".function");
            String aggregate_function_name;
            Array params_row;
            getAggregateFunctionNameAndParametersArray(
                aggregate_function_name_with_params, aggregate_function_name, params_row, "GraphiteMergeTree storage initialization");

            /// TODO Not only Float64
            AggregateFunctionProperties properties;
            pattern.function = AggregateFunctionFactory::instance().get(
                aggregate_function_name, {std::make_shared<DataTypeFloat64>()}, params_row, properties);
        }
        else if (startsWith(key, "retention"))
        {
            pattern.retentions.emplace_back(Graphite::Retention{
                .age = config.getUInt(config_element + "." + key + ".age"),
                .precision = config.getUInt(config_element + "." + key + ".precision")});
        }
        else
            throw Exception("Unknown element in config: " + key, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
    }

    if (!pattern.function && pattern.retentions.empty())
        throw Exception(
            "At least one of an aggregate function or retention rules is mandatory for rollup patterns in GraphiteMergeTree",
            ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    if (!pattern.function)
    {
        pattern.type = pattern.TypeRetention;
    }
    else if (pattern.retentions.empty())
    {
        pattern.type = pattern.TypeAggregation;
    }
    else
    {
        pattern.type = pattern.TypeAll;
    }

    if (pattern.type & pattern.TypeAggregation) /// TypeAggregation or TypeAll
        if (pattern.function->allocatesMemoryInArena())
            throw Exception(
                "Aggregate function " + pattern.function->getName() + " isn't supported in GraphiteMergeTree", ErrorCodes::NOT_IMPLEMENTED);

    /// retention should be in descending order of age.
    if (pattern.type & pattern.TypeRetention) /// TypeRetention or TypeAll
        std::sort(pattern.retentions.begin(), pattern.retentions.end(),
            [] (const Graphite::Retention & a, const Graphite::Retention & b) { return a.age > b.age; });

    patterns.emplace_back(pattern);
}

static void setGraphitePatternsFromConfig(const Context & context, const String & config_element, Graphite::Params & params)
{
    const auto & config = context.getConfigRef();

    if (!config.has(config_element))
        throw Exception("No '" + config_element + "' element in configuration file", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    params.config_name = config_element;
    params.path_column_name = config.getString(config_element + ".path_column_name", "Path");
    params.time_column_name = config.getString(config_element + ".time_column_name", "Time");
    params.value_column_name = config.getString(config_element + ".value_column_name", "Value");
    params.version_column_name = config.getString(config_element + ".version_column_name", "Timestamp");

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_element, keys);

    for (const auto & key : keys)
    {
        if (startsWith(key, "pattern"))
        {
            appendGraphitePattern(config, config_element + "." + key, params.patterns);
        }
        else if (key == "default")
        {
            /// See below.
        }
        else if (key == "path_column_name" || key == "time_column_name" || key == "value_column_name" || key == "version_column_name")
        {
            /// See above.
        }
        else
            throw Exception("Unknown element in config: " + key, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
    }

    if (config.has(config_element + ".default"))
        appendGraphitePattern(config, config_element + "." + ".default", params.patterns);
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

See details in documentation: https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/. Other engines of the family support different syntax, see details in the corresponding documentation topics.

If you use the Replicated version of engines, see https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/replication/.
)";

    return help;
}


static void randomizePartTypeSettings(const std::unique_ptr<MergeTreeSettings> & storage_settings)
{
    static constexpr auto MAX_THRESHOLD_FOR_ROWS = 100000;
    static constexpr auto MAX_THRESHOLD_FOR_BYTES = 1024 * 1024 * 10;

    /// Create all parts in wide format with probability 1/3.
    if (thread_local_rng() % 3 == 0)
    {
        storage_settings->min_rows_for_wide_part = 0;
        storage_settings->min_bytes_for_wide_part = 0;
    }
    else
    {
        storage_settings->min_rows_for_wide_part = std::uniform_int_distribution{0, MAX_THRESHOLD_FOR_ROWS}(thread_local_rng);
        storage_settings->min_bytes_for_wide_part = std::uniform_int_distribution{0, MAX_THRESHOLD_FOR_BYTES}(thread_local_rng);
    }
}


static StoragePtr create(const StorageFactory::Arguments & args)
{
    /** [Replicated][|Summing|Collapsing|Aggregating|Replacing|Graphite]MergeTree (2 * 7 combinations) engines
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
        || args.storage_def->settings;

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
            msg += "With extended storage definition syntax storage " + args.engine_name + " requires ";
        else
            msg += "Storage " + args.engine_name + " requires ";

        if (max_num_params)
        {
            if (min_num_params == max_num_params)
                msg += toString(min_num_params) + " parameters: ";
            else
                msg += toString(min_num_params) + " to " + toString(max_num_params) + " parameters: ";
            msg += needed_params;
        }
        else
            msg += "no parameters";

        msg += getMergeTreeVerboseHelp(is_extended_storage_def);

        throw Exception(msg, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    /// For Replicated.
    String zookeeper_path;
    String replica_name;
    bool allow_renaming = true;

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
        else if (is_extended_storage_def && arg_cnt == 0)
        {
            /// Try use default values if arguments are not specified.
            /// Note: {uuid} macro works for ON CLUSTER queries when database engine is Atomic.
            zookeeper_path = args.context.getConfigRef().getString("default_replica_path", "/clickhouse/tables/{uuid}/{shard}");
            /// TODO maybe use hostname if {replica} is not defined?
            replica_name = args.context.getConfigRef().getString("default_replica_name", "{replica}");

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
            throw Exception("Expected two string literal arguments: zookeper_path and replica_name", ErrorCodes::BAD_ARGUMENTS);

        /// Allow implicit {uuid} macros only for zookeeper_path in ON CLUSTER queries
        bool is_on_cluster = args.local_context.getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
        bool allow_uuid_macro = is_on_cluster || args.query.attach;

        /// Unfold {database} and {table} macro on table creation, so table can be renamed.
        /// We also unfold {uuid} macro, so path will not be broken after moving table from Atomic to Ordinary database.
        if (!args.attach)
        {
            Macros::MacroExpansionInfo info;
            /// NOTE: it's not recursive
            info.expand_special_macros_only = true;
            info.table_id = args.table_id;
            if (!allow_uuid_macro)
                info.table_id.uuid = UUIDHelpers::Nil;
            zookeeper_path = args.context.getMacros()->expand(zookeeper_path, info);

            info.level = 0;
            info.table_id.uuid = UUIDHelpers::Nil;
            replica_name = args.context.getMacros()->expand(replica_name, info);
        }

        ast_zk_path->value = zookeeper_path;
        ast_replica_name->value = replica_name;

        /// Expand other macros (such as {shard} and {replica}). We do not expand them on previous step
        /// to make possible copying metadata files between replicas.
        Macros::MacroExpansionInfo info;
        info.table_id = args.table_id;
        if (!allow_uuid_macro)
            info.table_id.uuid = UUIDHelpers::Nil;
        zookeeper_path = args.context.getMacros()->expand(zookeeper_path, info);

        info.level = 0;
        info.table_id.uuid = UUIDHelpers::Nil;
        replica_name = args.context.getMacros()->expand(replica_name, info);

        /// We do not allow renaming table with these macros in metadata, because zookeeper_path will be broken after RENAME TABLE.
        /// NOTE: it may happen if table was created by older version of ClickHouse (< 20.10) and macros was not unfolded on table creation
        /// or if one of these macros is recursively expanded from some other macro.
        if (info.expanded_database || info.expanded_table)
            allow_renaming = false;
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
        setGraphitePatternsFromConfig(args.context, graphite_config_name, merging_params.graphite_params);
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
    metadata.columns = args.columns;

    std::unique_ptr<MergeTreeSettings> storage_settings;
    if (replicated)
        storage_settings = std::make_unique<MergeTreeSettings>(args.context.getReplicatedMergeTreeSettings());
    else
        storage_settings = std::make_unique<MergeTreeSettings>(args.context.getMergeTreeSettings());

    if (is_extended_storage_def)
    {
        ASTPtr partition_by_key;
        if (args.storage_def->partition_by)
            partition_by_key = args.storage_def->partition_by->ptr();

        /// Partition key may be undefined, but despite this we store it's empty
        /// value in partition_key structure. MergeTree checks this case and use
        /// single default partition with name "all".
        metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_key, metadata.columns, args.context);

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
            args.storage_def->order_by->ptr(), metadata.columns, args.context, merging_param_key_arg);

        /// If primary key explicitly defined, than get it from AST
        if (args.storage_def->primary_key)
        {
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.context);
        }
        else /// Otherwise we don't have explicit primary key and copy it from order by
        {
            metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->order_by->ptr(), metadata.columns, args.context);
            /// and set it's definition_ast to nullptr (so isPrimaryKeyDefined()
            /// will return false but hasPrimaryKey() will return true.
            metadata.primary_key.definition_ast = nullptr;
        }

        if (args.storage_def->sample_by)
            metadata.sampling_key = KeyDescription::getKeyFromAST(args.storage_def->sample_by->ptr(), metadata.columns, args.context);

        if (args.storage_def->ttl_table)
        {
            metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
                args.storage_def->ttl_table->ptr(), metadata.columns, args.context, metadata.primary_key);
        }

        if (args.query.columns_list && args.query.columns_list->indices)
            for (auto & index : args.query.columns_list->indices->children)
                metadata.secondary_indices.push_back(IndexDescription::getIndexFromAST(index, args.columns, args.context));

        if (args.query.columns_list && args.query.columns_list->constraints)
            for (auto & constraint : args.query.columns_list->constraints->children)
                metadata.constraints.constraints.push_back(constraint);

        auto column_ttl_asts = args.columns.getColumnTTLs();
        for (const auto & [name, ast] : column_ttl_asts)
        {
            auto new_ttl_entry = TTLDescription::getTTLFromAST(ast, args.columns, args.context, metadata.primary_key);
            metadata.column_ttls_by_name[name] = new_ttl_entry;
        }

        storage_settings->loadFromQuery(*args.storage_def);

        // updates the default storage_settings with settings specified via SETTINGS arg in a query
        if (args.storage_def->settings)
            metadata.settings_changes = args.storage_def->settings->ptr();

        size_t index_granularity_bytes = 0;
        size_t min_index_granularity_bytes = 0;

        index_granularity_bytes = storage_settings->index_granularity_bytes;
        min_index_granularity_bytes = storage_settings->min_index_granularity_bytes;

        /* the min_index_granularity_bytes value is 1024 b and index_granularity_bytes is 10 mb by default
         * if index_granularity_bytes is not disabled i.e > 0 b, then always ensure that it's greater than
         * min_index_granularity_bytes. This is mainly a safeguard against accidents whereby a really low
         * index_granularity_bytes SETTING of 1b can create really large parts with large marks.
        */
        if (index_granularity_bytes > 0 && index_granularity_bytes < min_index_granularity_bytes)
        {
            throw Exception(
                "index_granularity_bytes: " + std::to_string(index_granularity_bytes)
                    + " is lesser than specified min_index_granularity_bytes: " + std::to_string(min_index_granularity_bytes),
                ErrorCodes::BAD_ARGUMENTS);
        }
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

        metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_ast, metadata.columns, args.context);


        ++arg_num;

        /// If there is an expression for sampling
        if (arg_cnt - arg_num == 3)
        {
            metadata.sampling_key = KeyDescription::getKeyFromAST(engine_args[arg_num], metadata.columns, args.context);
            ++arg_num;
        }

        /// Get sorting key from engine arguments.
        ///
        /// NOTE: store merging_param_key_arg as additional key column. We do it
        /// before storage creation. After that storage will just copy this
        /// column if sorting key will be changed.
        metadata.sorting_key
            = KeyDescription::getSortingKeyFromAST(engine_args[arg_num], metadata.columns, args.context, merging_param_key_arg);

        /// In old syntax primary_key always equals to sorting key.
        metadata.primary_key = KeyDescription::getKeyFromAST(engine_args[arg_num], metadata.columns, args.context);
        /// But it's not explicitly defined, so we evaluate definition to
        /// nullptr
        metadata.primary_key.definition_ast = nullptr;

        ++arg_num;

        const auto * ast = engine_args[arg_num]->as<ASTLiteral>();
        if (ast && ast->value.getType() == Field::Types::UInt64)
            storage_settings->index_granularity = safeGet<UInt64>(ast->value);
        else
            throw Exception(
                "Index granularity must be a positive integer" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);
        ++arg_num;
    }

    /// Allow to randomize part type for tests to cover more cases.
    /// But if settings were set explicitly restrict it.
    if (storage_settings->randomize_part_type
        && !storage_settings->min_rows_for_wide_part.changed
        && !storage_settings->min_bytes_for_wide_part.changed)
    {
        randomizePartTypeSettings(storage_settings);
        LOG_INFO(&Poco::Logger::get(args.table_id.getNameForLogs() + " (registerStorageMergeTree)"),
            "Applied setting 'randomize_part_type'. "
            "Setting 'min_rows_for_wide_part' changed to {}. "
            "Setting 'min_bytes_for_wide_part' changed to {}.",
            storage_settings->min_rows_for_wide_part, storage_settings->min_bytes_for_wide_part);
    }

    if (arg_num != arg_cnt)
        throw Exception("Wrong number of engine arguments.", ErrorCodes::BAD_ARGUMENTS);

    if (replicated)
        return StorageReplicatedMergeTree::create(
            zookeeper_path,
            replica_name,
            args.attach,
            args.table_id,
            args.relative_data_path,
            metadata,
            args.context,
            date_column_name,
            merging_params,
            std::move(storage_settings),
            args.has_force_restore_data_flag,
            allow_renaming);
    else
        return StorageMergeTree::create(
            args.table_id,
            args.relative_data_path,
            metadata,
            args.attach,
            args.context,
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
        .supports_sort_order = true,
        .supports_ttl = true,
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

    factory.registerStorage("ReplicatedMergeTree", create, features);
    factory.registerStorage("ReplicatedCollapsingMergeTree", create, features);
    factory.registerStorage("ReplicatedReplacingMergeTree", create, features);
    factory.registerStorage("ReplicatedAggregatingMergeTree", create, features);
    factory.registerStorage("ReplicatedSummingMergeTree", create, features);
    factory.registerStorage("ReplicatedGraphiteMergeTree", create, features);
    factory.registerStorage("ReplicatedVersionedCollapsingMergeTree", create, features);
}

}
