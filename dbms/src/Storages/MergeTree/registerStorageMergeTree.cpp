#include <Storages/StorageFactory.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageReplicatedMergeTree.h>

#include <Common/typeid_cast.h>
#include <Common/OptimizedRegularExpression.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTCreateQuery.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_STORAGE;
    extern const int NO_REPLICA_NAME_GIVEN;
}


/** Get the key expression AST as an ASTExpressionList.
  * It can be specified in the tuple: (CounterID, Date),
  *  or as one column: CounterID.
  */
static ASTPtr extractKeyExpressionList(IAST & node)
{
    const ASTFunction * expr_func = typeid_cast<const ASTFunction *>(&node);

    if (expr_func && expr_func->name == "tuple")
    {
        /// Primary key is specified in tuple.
        return expr_func->children.at(0);
    }
    else
    {
        /// Primary key consists of one column.
        auto res = std::make_shared<ASTExpressionList>();
        res->children.push_back(node.ptr());
        return res;
    }
}

/** Get the list of column names.
  * It can be specified in the tuple: (Clicks, Cost),
  * or as one column: Clicks.
  */
static Names extractColumnNames(const ASTPtr & node)
{
    const ASTFunction * expr_func = typeid_cast<const ASTFunction *>(&*node);

    if (expr_func && expr_func->name == "tuple")
    {
        const auto & elements = expr_func->children.at(0)->children;
        Names res;
        res.reserve(elements.size());
        for (const auto & elem : elements)
            res.push_back(typeid_cast<const ASTIdentifier &>(*elem).name);

        return res;
    }
    else
    {
        return { typeid_cast<const ASTIdentifier &>(*node).name };
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
static void appendGraphitePattern(
    const Poco::Util::AbstractConfiguration & config, const String & config_element, Graphite::Patterns & patterns)
{
    Graphite::Pattern pattern;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_element, keys);

    for (const auto & key : keys)
    {
        if (key == "regexp")
        {
            pattern.regexp = std::make_shared<OptimizedRegularExpression>(config.getString(config_element + ".regexp"));
        }
        else if (key == "function")
        {
            String aggregate_function_name_with_params = config.getString(config_element + ".function");
            String aggregate_function_name;
            Array params_row;
            getAggregateFunctionNameAndParametersArray(aggregate_function_name_with_params,
                                                       aggregate_function_name, params_row, "GraphiteMergeTree storage initialization");

            /// TODO Not only Float64
            pattern.function = AggregateFunctionFactory::instance().get(aggregate_function_name, {std::make_shared<DataTypeFloat64>()},
                                                                        params_row);
        }
        else if (startsWith(key, "retention"))
        {
            pattern.retentions.emplace_back(
                Graphite::Retention{
                    .age        = config.getUInt(config_element + "." + key + ".age"),
                    .precision  = config.getUInt(config_element + "." + key + ".precision")});
        }
        else
            throw Exception("Unknown element in config: " + key, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
    }

    if (!pattern.function)
        throw Exception("Aggregate function is mandatory for retention patterns in GraphiteMergeTree",
            ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    if (pattern.function->allocatesMemoryInArena())
        throw Exception("Aggregate function " + pattern.function->getName() + " isn't supported in GraphiteMergeTree",
                        ErrorCodes::NOT_IMPLEMENTED);

    /// retention should be in descending order of age.
    std::sort(pattern.retentions.begin(), pattern.retentions.end(),
        [] (const Graphite::Retention & a, const Graphite::Retention & b) { return a.age > b.age; });

    patterns.emplace_back(pattern);
}

static void setGraphitePatternsFromConfig(const Context & context,
    const String & config_element, Graphite::Params & params)
{
    const auto & config = context.getConfigRef();

    if (!config.has(config_element))
        throw Exception("No '" + config_element + "' element in configuration file",
            ErrorCodes::NO_ELEMENTS_IN_CONFIG);

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
        else if (key == "path_column_name"
            || key == "time_column_name"
            || key == "value_column_name"
            || key == "version_column_name")
        {
            /// See above.
        }
        else
            throw Exception("Unknown element in config: " + key, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
    }

    if (config.has(config_element + ".default"))
        appendGraphitePattern(config, config_element + "." + ".default", params.patterns);
}


static String getMergeTreeVerboseHelp(bool is_extended_syntax)
{
    String help = R"(

MergeTree is a family of storage engines.

MergeTrees are different in two ways:
- they may be replicated and non-replicated;
- they may do different actions on merge: nothing; sign collapse; sum; apply aggregete functions.

So we have 14 combinations:
    MergeTree, CollapsingMergeTree, SummingMergeTree, AggregatingMergeTree, ReplacingMergeTree, GraphiteMergeTree, VersionedCollapsingMergeTree
    ReplicatedMergeTree, ReplicatedCollapsingMergeTree, ReplicatedSummingMergeTree, ReplicatedAggregatingMergeTree, ReplicatedReplacingMergeTree, ReplicatedGraphiteMergeTree, ReplicatedVersionedCollapsingMergeTree

In most of cases, you need MergeTree or ReplicatedMergeTree.

For replicated merge trees, you need to supply a path in ZooKeeper and a replica name as the first two parameters.
Path in ZooKeeper is like '/clickhouse/tables/01/' where /clickhouse/tables/ is a common prefix and 01 is a shard name.
Replica name is like 'mtstat01-1' - it may be the hostname or any suitable string identifying replica.
You may use macro substitutions for these parameters. It's like ReplicatedMergeTree('/clickhouse/tables/{shard}/', '{replica}'...
Look at the <macros> section in server configuration file.
)";

    if (!is_extended_syntax)
        help += R"(
Next parameter (which is the first for unreplicated tables and the third for replicated tables) is the name of date column.
Date column must exist in the table and have type Date (not DateTime).
It is used for internal data partitioning and works like some kind of index.

If your source data doesn't have a column of type Date, but has a DateTime column, you may add values for Date column while loading,
    or you may INSERT your source data to a table of type Log and then transform it with INSERT INTO t SELECT toDate(time) AS date, * FROM ...
If your source data doesn't have any date or time, you may just pass any constant for a date column while loading.

Next parameter is optional sampling expression. Sampling expression is used to implement SAMPLE clause in query for approximate query execution.
If you don't need approximate query execution, simply omit this parameter.
Sample expression must be one of the elements of the primary key tuple. For example, if your primary key is (CounterID, EventDate, intHash64(UserID)), your sampling expression might be intHash64(UserID).

Next parameter is the primary key tuple. It's like (CounterID, EventDate, intHash64(UserID)) - a list of column names or functional expressions in round brackets. If your primary key has just one element, you may omit round brackets.

Careful choice of the primary key is extremely important for processing short-time queries.

Next parameter is index (primary key) granularity. Good value is 8192. You have no reasons to use any other value.
)";

    help += R"(
For the Collapsing mode, the last parameter is the name of a sign column - a special column that is used to 'collapse' rows with the same primary key while merging.

For the Summing mode, the optional last parameter is a list of columns to sum while merging. This list is passed in round brackets, like (PageViews, Cost).
If this parameter is omitted, the storage will sum all numeric columns except columns participating in the primary key.

For the Replacing mode, the optional last parameter is the name of a 'version' column. While merging, for all rows with the same primary key, only one row is selected: the last row, if the version column was not specified, or the last row with the maximum version value, if specified.

For VersionedCollapsing mode, the last 2 parameters are the name of a sign column and the name of a 'version' column. Version column must be in primary key. While merging, a pair of rows with the same primary key and different sign may collapse.
)";

    if (is_extended_syntax)
        help += R"(
You can specify a partitioning expression in the PARTITION BY clause. It is optional but highly recommended.
A common partitioning expression is some function of the event date column e.g. PARTITION BY toYYYYMM(EventDate) will partition the table by month.
Rows with different partition expression values are never merged together. That allows manipulating partitions with ALTER commands.
Also it acts as a kind of index.

Primary key is specified in the ORDER BY clause. It is mandatory for all MergeTree types.
It is like (CounterID, EventDate, intHash64(UserID)) - a list of column names or functional expressions in round brackets.
If your primary key has just one element, you may omit round brackets.

Careful choice of the primary key is extremely important for processing short-time queries.

Optional sampling expression can be specified in the SAMPLE BY clause. It is used to implement the SAMPLE clause in a SELECT query for approximate query execution.
Sampling expression must be one of the elements of the primary key tuple. For example, if your primary key is (CounterID, EventDate, intHash64(UserID)), your sampling expression might be intHash64(UserID).

Engine settings can be specified in the SETTINGS clause. Full list is in the source code in the 'dbms/src/Storages/MergeTree/MergeTreeSettings.h' file.
E.g. you can specify the index (primary key) granularity with SETTINGS index_granularity = 8192.

Examples:

MergeTree PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate) SETTINGS index_granularity = 8192

MergeTree PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime) SAMPLE BY intHash32(UserID)

CollapsingMergeTree(Sign) PARTITION BY StartDate SAMPLE BY intHash32(UserID) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)

SummingMergeTree PARTITION BY toMonday(EventDate) ORDER BY (OrderID, EventDate, BannerID, PhraseID, ContextType, RegionID, PageID, IsFlat, TypeID, ResourceNo)

SummingMergeTree((Shows, Clicks, Cost, CostCur, ShowsSumPosition, ClicksSumPosition, SessionNum, SessionLen, SessionCost, GoalsNum, SessionDepth)) PARTITION BY toYYYYMM(EventDate) ORDER BY (OrderID, EventDate, BannerID, PhraseID, ContextType, RegionID, PageID, IsFlat, TypeID, ResourceNo)

ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/hits', '{replica}') PARTITION BY EventDate ORDER BY (CounterID, EventDate, intHash32(UserID), EventTime) SAMPLE BY intHash32(UserID)
)";
    else
        help += R"(
Examples:

MergeTree(EventDate, (CounterID, EventDate), 8192)

MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192)

CollapsingMergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192, Sign)

SummingMergeTree(EventDate, (OrderID, EventDate, BannerID, PhraseID, ContextType, RegionID, PageID, IsFlat, TypeID, ResourceNo), 8192)

SummingMergeTree(EventDate, (OrderID, EventDate, BannerID, PhraseID, ContextType, RegionID, PageID, IsFlat, TypeID, ResourceNo), 8192, (Shows, Clicks, Cost, CostCur, ShowsSumPosition, ClicksSumPosition, SessionNum, SessionLen, SessionCost, GoalsNum, SessionDepth))

ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/hits', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192)
)";

    help += R"(
For further info please read the documentation: https://clickhouse.yandex/
)";

    return help;
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
        *  - Primary key in the ORDER BY clause;
        *  - Sampling expression in the SAMPLE BY clause;
        *  - Additional MergeTreeSettings in the SETTINGS clause;
        */

    bool is_extended_storage_def =
        args.storage_def->partition_by || args.storage_def->order_by || args.storage_def->sample_by || args.storage_def->settings;

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
            "Unknown storage " + args.engine_name + getMergeTreeVerboseHelp(is_extended_storage_def),
            ErrorCodes::UNKNOWN_STORAGE);

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
        add_mandatory_param("path in ZooKeeper");
        add_mandatory_param("replica name");
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
    case MergeTreeData::MergingParams::VersionedCollapsing:
        {
            add_mandatory_param("sign column");
            add_mandatory_param("version");
            break;
        }
    }

    ASTs & engine_args = args.engine_args;

    if (engine_args.size() < min_num_params || engine_args.size() > max_num_params)
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

    if (replicated)
    {
        auto ast = typeid_cast<const ASTLiteral *>(engine_args[0].get());
        if (ast && ast->value.getType() == Field::Types::String)
            zookeeper_path = safeGet<String>(ast->value);
        else
            throw Exception(
                "Path in ZooKeeper must be a string literal" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);

        ast = typeid_cast<const ASTLiteral *>(engine_args[1].get());
        if (ast && ast->value.getType() == Field::Types::String)
            replica_name = safeGet<String>(ast->value);
        else
            throw Exception(
                "Replica name must be a string literal" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);

        if (replica_name.empty())
            throw Exception(
                "No replica name in config" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::NO_REPLICA_NAME_GIVEN);

        engine_args.erase(engine_args.begin(), engine_args.begin() + 2);
    }

    ASTPtr secondary_sorting_expr_list;

    if (merging_params.mode == MergeTreeData::MergingParams::Collapsing)
    {
        if (auto ast = typeid_cast<const ASTIdentifier *>(engine_args.back().get()))
            merging_params.sign_column = ast->name;
        else
            throw Exception(
                "Sign column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);

        engine_args.pop_back();
    }
    else if (merging_params.mode == MergeTreeData::MergingParams::Replacing)
    {
        /// If the last element is not index_granularity or replica_name (a literal), then this is the name of the version column.
        if (!engine_args.empty() && !typeid_cast<const ASTLiteral *>(engine_args.back().get()))
        {
            if (auto ast = typeid_cast<const ASTIdentifier *>(engine_args.back().get()))
                merging_params.version_column = ast->name;
            else
                throw Exception(
                    "Version column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                    ErrorCodes::BAD_ARGUMENTS);

            engine_args.pop_back();
        }
    }
    else if (merging_params.mode == MergeTreeData::MergingParams::Summing)
    {
        /// If the last element is not index_granularity or replica_name (a literal), then this is a list of summable columns.
        if (!engine_args.empty() && !typeid_cast<const ASTLiteral *>(engine_args.back().get()))
        {
            merging_params.columns_to_sum = extractColumnNames(engine_args.back());
            engine_args.pop_back();
        }
    }
    else if (merging_params.mode == MergeTreeData::MergingParams::Graphite)
    {
        String graphite_config_name;
        String error_msg = "Last parameter of GraphiteMergeTree must be name (in single quotes) of element in configuration file with Graphite options";
        error_msg += getMergeTreeVerboseHelp(is_extended_storage_def);

        if (auto ast = typeid_cast<const ASTLiteral *>(engine_args.back().get()))
        {
            if (ast->value.getType() != Field::Types::String)
                throw Exception(error_msg, ErrorCodes::BAD_ARGUMENTS);

            graphite_config_name = ast->value.get<String>();
        }
        else
            throw Exception(error_msg, ErrorCodes::BAD_ARGUMENTS);

        engine_args.pop_back();
        setGraphitePatternsFromConfig(args.context, graphite_config_name, merging_params.graphite_params);
    }
    else if (merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing)
    {
        if (auto ast = typeid_cast<const ASTIdentifier *>(engine_args.back().get()))
        {
            merging_params.version_column = ast->name;
            secondary_sorting_expr_list = std::make_shared<ASTExpressionList>();
            secondary_sorting_expr_list->children.push_back(engine_args.back());
        }
        else
            throw Exception(
                    "Version column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                    ErrorCodes::BAD_ARGUMENTS);

        engine_args.pop_back();

        if (auto ast = typeid_cast<const ASTIdentifier *>(engine_args.back().get()))
            merging_params.sign_column = ast->name;
        else
            throw Exception(
                    "Sign column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                    ErrorCodes::BAD_ARGUMENTS);

        engine_args.pop_back();
    }

    String date_column_name;
    ASTPtr partition_expr_list;
    ASTPtr primary_expr_list;
    ASTPtr sampling_expression;
    MergeTreeSettings storage_settings = args.context.getMergeTreeSettings();

    if (is_extended_storage_def)
    {
        if (args.storage_def->partition_by)
            partition_expr_list = extractKeyExpressionList(*args.storage_def->partition_by);

        if (args.storage_def->order_by)
            primary_expr_list = extractKeyExpressionList(*args.storage_def->order_by);
        else
            throw Exception("You must provide an ORDER BY expression in the table definition. "
                "If you don't want this table to be sorted, use ORDER BY tuple()",
                ErrorCodes::BAD_ARGUMENTS);

        if (args.storage_def->sample_by)
            sampling_expression = args.storage_def->sample_by->ptr();

        storage_settings.loadFromQuery(*args.storage_def);
    }
    else
    {
        /// If there is an expression for sampling. MergeTree(date, [sample_key], primary_key, index_granularity)
        if (engine_args.size() == 4)
        {
            sampling_expression = engine_args[1];
            engine_args.erase(engine_args.begin() + 1);
        }

        /// Now only three parameters remain - date (or partitioning expression), primary_key, index_granularity.

        if (auto ast = typeid_cast<const ASTIdentifier *>(engine_args[0].get()))
            date_column_name = ast->name;
        else
            throw Exception(
                "Date column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);

        primary_expr_list = extractKeyExpressionList(*engine_args[1]);

        auto ast = typeid_cast<const ASTLiteral *>(engine_args.back().get());
        if (ast && ast->value.getType() == Field::Types::UInt64)
            storage_settings.index_granularity = safeGet<UInt64>(ast->value);
        else
            throw Exception(
                "Index granularity must be a positive integer" + getMergeTreeVerboseHelp(is_extended_storage_def),
                ErrorCodes::BAD_ARGUMENTS);
    }

    if (replicated)
        return StorageReplicatedMergeTree::create(
            zookeeper_path, replica_name, args.attach, args.data_path, args.database_name, args.table_name,
            args.columns,
            args.context, primary_expr_list, secondary_sorting_expr_list, date_column_name, partition_expr_list,
            sampling_expression, merging_params, storage_settings,
            args.has_force_restore_data_flag);
    else
        return StorageMergeTree::create(
            args.data_path, args.database_name, args.table_name, args.columns, args.attach,
            args.context, primary_expr_list, secondary_sorting_expr_list, date_column_name, partition_expr_list,
            sampling_expression, merging_params, storage_settings,
            args.has_force_restore_data_flag);
}


void registerStorageMergeTree(StorageFactory & factory)
{
    factory.registerStorage("MergeTree", create);
    factory.registerStorage("CollapsingMergeTree", create);
    factory.registerStorage("ReplacingMergeTree", create);
    factory.registerStorage("AggregatingMergeTree", create);
    factory.registerStorage("SummingMergeTree", create);
    factory.registerStorage("GraphiteMergeTree", create);
    factory.registerStorage("VersionedCollapsingMergeTree", create);

    factory.registerStorage("ReplicatedMergeTree", create);
    factory.registerStorage("ReplicatedCollapsingMergeTree", create);
    factory.registerStorage("ReplicatedReplacingMergeTree", create);
    factory.registerStorage("ReplicatedAggregatingMergeTree", create);
    factory.registerStorage("ReplicatedSummingMergeTree", create);
    factory.registerStorage("ReplicatedGraphiteMergeTree", create);
    factory.registerStorage("ReplicatedVersionedCollapsingMergeTree", create);
}

}
