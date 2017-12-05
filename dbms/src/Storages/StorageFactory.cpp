#include <unistd.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Core/FieldVisitors.h>
#include <Common/StringUtils.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeTuple.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/getClusterName.h>
#include <Storages/StorageLog.h>
#include <Storages/StorageTinyLog.h>
#include <Storages/StorageStripeLog.h>
#include <Storages/StorageMemory.h>
#include <Storages/StorageBuffer.h>
#include <Storages/StorageNull.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageView.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/StorageSet.h>
#include <Storages/StorageJoin.h>
#include <Storages/StorageFile.h>
#include <Storages/StorageMySQL.h>
#include <Storages/StorageODBC.h>
#include <Storages/StorageDictionary.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>

#include <Common/config.h>
#if USE_RDKAFKA
#include <Storages/StorageKafka.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_MUST_NOT_BE_CREATED_MANUALLY;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_STORAGE;
    extern const int NO_REPLICA_NAME_GIVEN;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
    extern const int TYPE_MISMATCH;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int DATA_TYPE_CANNOT_BE_USED_IN_TABLES;
    extern const int SUPPORT_IS_DISABLED;
    extern const int INCORRECT_QUERY;
    extern const int ENGINE_REQUIRED;
}


/** For StorageMergeTree: get the key expression AST as an ASTExpressionList.
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

/** For StorageMergeTree: get the list of column names.
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


/** Read the settings for decimation of old Graphite data from config.
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
static void appendGraphitePattern(const Context & context,
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
            appendGraphitePattern(context, config, config_element + "." + key, params.patterns);
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
        appendGraphitePattern(context, config, config_element + "." + ".default", params.patterns);
}


/// Some types are only for intermediate values of expressions and cannot be used in tables.
static void checkAllTypesAreAllowedInTable(const NamesAndTypesList & names_and_types)
{
    for (const auto & elem : names_and_types)
        if (elem.type->notForTables())
            throw Exception("Data type " + elem.type->getName() + " cannot be used in tables", ErrorCodes::DATA_TYPE_CANNOT_BE_USED_IN_TABLES);
}


static String getMergeTreeVerboseHelp(bool is_extended_syntax)
{
    String help = R"(

MergeTree is a family of storage engines.

MergeTrees are different in two ways:
- they may be replicated and non-replicated;
- they may do different actions on merge: nothing; sign collapse; sum; apply aggregete functions.

So we have 14 combinations:
    MergeTree, CollapsingMergeTree, SummingMergeTree, AggregatingMergeTree, ReplacingMergeTree, UnsortedMergeTree, GraphiteMergeTree
    ReplicatedMergeTree, ReplicatedCollapsingMergeTree, ReplicatedSummingMergeTree, ReplicatedAggregatingMergeTree, ReplicatedReplacingMergeTree, ReplicatedUnsortedMergeTree, ReplicatedGraphiteMergeTree

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
)";

    if (is_extended_syntax)
        help += R"(
You can specify a partitioning expression in the PARTITION BY clause. It is optional but highly recommended.
A common partitioning expression is some function of the event date column e.g. PARTITION BY toYYYYMM(EventDate) will partition the table by month.
Rows with different partition expression values are never merged together. That allows manipulating partitions with ALTER commands.
Also it acts as a kind of index.

Primary key is specified in the ORDER BY clause. It is mandatory for all MergeTree types except UnsortedMergeTree.
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


StoragePtr StorageFactory::get(
    ASTCreateQuery & query,
    const String & data_path,
    const String & table_name,
    const String & database_name,
    Context & local_context,
    Context & context,
    NamesAndTypesListPtr columns,
    const NamesAndTypesList & materialized_columns,
    const NamesAndTypesList & alias_columns,
    const ColumnDefaults & column_defaults,
    bool attach,
    bool has_force_restore_data_flag) const
{
    if (query.is_view)
    {
        if (query.storage)
            throw Exception("Specifying ENGINE is not allowed for a View", ErrorCodes::INCORRECT_QUERY);

        return StorageView::create(
            table_name, database_name, query, columns,
            materialized_columns, alias_columns, column_defaults);
    }

    /// Check for some special types, that are not allowed to be stored in tables. Example: NULL data type.
    /// Exception: any type is allowed in View, because plain (non-materialized) View does not store anything itself.
    checkAllTypesAreAllowedInTable(*columns);
    checkAllTypesAreAllowedInTable(materialized_columns);
    checkAllTypesAreAllowedInTable(alias_columns);

    if (query.is_materialized_view)
    {
        /// Pass local_context here to convey setting for inner table
        return StorageMaterializedView::create(
            table_name, database_name, local_context, query, columns,
            materialized_columns, alias_columns, column_defaults,
            attach);
    }

    if (!query.storage)
        throw Exception("Incorrect CREATE query: ENGINE required", ErrorCodes::ENGINE_REQUIRED);

    ASTStorage & storage_def = *query.storage;
    const ASTFunction & engine_def = *storage_def.engine;

    if (engine_def.parameters)
        throw Exception(
            "Engine definition cannot take the form of a parametric function", ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS);

    ASTs * args_ptr = nullptr;
    if (engine_def.arguments)
        args_ptr = &engine_def.arguments->children;

    const String & name = engine_def.name;

    if ((storage_def.partition_by || storage_def.order_by || storage_def.sample_by || storage_def.settings)
        && !endsWith(name, "MergeTree"))
    {
        throw Exception(
            "Engine " + name + " doesn't support PARTITION BY, ORDER BY, SAMPLE BY or SETTINGS clauses. "
            "Currently only the MergeTree family of engines supports them", ErrorCodes::BAD_ARGUMENTS);
    }

    auto check_arguments_empty = [&]
    {
        if (args_ptr && !args_ptr->empty())
            throw Exception(
                "Engine " + name + " doesn't support any arguments (" + toString(args_ptr->size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    };

    if (name == "View")
    {
        throw Exception(
            "Direct creation of tables with ENGINE View is not supported, use CREATE VIEW statement",
            ErrorCodes::INCORRECT_QUERY);
    }
    else if (name == "MaterializedView")
    {
        throw Exception(
            "Direct creation of tables with ENGINE MaterializedView is not supported, use CREATE MATERIALIZED VIEW statement",
            ErrorCodes::INCORRECT_QUERY);
    }
    else if (name == "Log")
    {
        check_arguments_empty();
        return StorageLog::create(
            data_path, table_name, columns,
            materialized_columns, alias_columns, column_defaults,
            context.getSettings().max_compress_block_size);
    }
    else if (name == "Dictionary")
    {
        return StorageDictionary::create(
            table_name, context, query, columns,
            materialized_columns, alias_columns, column_defaults);
    }
    else if (name == "TinyLog")
    {
        check_arguments_empty();
        return StorageTinyLog::create(
            data_path, table_name, columns,
            materialized_columns, alias_columns, column_defaults,
            attach, context.getSettings().max_compress_block_size);
    }
    else if (name == "StripeLog")
    {
        check_arguments_empty();
        return StorageStripeLog::create(
            data_path, table_name, columns,
            materialized_columns, alias_columns, column_defaults,
            attach, context.getSettings().max_compress_block_size);
    }
    else if (name == "File")
    {
        if (!args_ptr || !(args_ptr->size() == 1 || args_ptr->size() == 2))
            throw Exception(
                "Storage File requires 1 or 2 arguments: name of used format and source.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        ASTs & args = *args_ptr;

        args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], local_context);
        String format_name = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>();

        int source_fd = -1;
        String source_path;
        if (args.size() >= 2)
        {
            /// Will use FD if args[1] is int literal or identifier with std* name

            if (ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(args[1].get()))
            {
                if (identifier->name == "stdin")
                    source_fd = STDIN_FILENO;
                else if (identifier->name == "stdout")
                    source_fd = STDOUT_FILENO;
                else if (identifier->name == "stderr")
                    source_fd = STDERR_FILENO;
                else
                    throw Exception("Unknown identifier '" + identifier->name + "' in second arg of File storage constructor",
                                    ErrorCodes::UNKNOWN_IDENTIFIER);
            }

            if (ASTLiteral * literal = typeid_cast<ASTLiteral *>(args[1].get()))
            {
                auto type = literal->value.getType();
                if (type == Field::Types::Int64)
                    source_fd = static_cast<int>(literal->value.get<Int64>());
                else if (type == Field::Types::UInt64)
                    source_fd = static_cast<int>(literal->value.get<UInt64>());
            }

            args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], local_context);
            source_path = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>();
        }

        return StorageFile::create(
            source_path, source_fd,
            data_path, table_name, format_name, columns,
            materialized_columns, alias_columns, column_defaults,
            context);
    }
    else if (name == "Set")
    {
        check_arguments_empty();
        return StorageSet::create(
            data_path, table_name, columns,
            materialized_columns, alias_columns, column_defaults);
    }
    else if (name == "Join")
    {
        /// Join(ANY, LEFT, k1, k2, ...)

        if (!args_ptr || args_ptr->size() < 3)
            throw Exception(
                "Storage Join requires at least 3 parameters: Join(ANY|ALL, LEFT|INNER, keys...).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        const ASTs & args = *args_ptr;

        const ASTIdentifier * strictness_id = typeid_cast<ASTIdentifier *>(&*args[0]);
        if (!strictness_id)
            throw Exception("First parameter of storage Join must be ANY or ALL (without quotes).", ErrorCodes::BAD_ARGUMENTS);

        const String strictness_str = Poco::toLower(strictness_id->name);
        ASTTableJoin::Strictness strictness;
        if (strictness_str == "any")
            strictness = ASTTableJoin::Strictness::Any;
        else if (strictness_str == "all")
            strictness = ASTTableJoin::Strictness::All;
        else
            throw Exception("First parameter of storage Join must be ANY or ALL (without quotes).", ErrorCodes::BAD_ARGUMENTS);

        const ASTIdentifier * kind_id = typeid_cast<ASTIdentifier *>(&*args[1]);
        if (!kind_id)
            throw Exception("Second parameter of storage Join must be LEFT or INNER (without quotes).", ErrorCodes::BAD_ARGUMENTS);

        const String kind_str = Poco::toLower(kind_id->name);
        ASTTableJoin::Kind kind;
        if (kind_str == "left")
            kind = ASTTableJoin::Kind::Left;
        else if (kind_str == "inner")
            kind = ASTTableJoin::Kind::Inner;
        else if (kind_str == "right")
            kind = ASTTableJoin::Kind::Right;
        else if (kind_str == "full")
            kind = ASTTableJoin::Kind::Full;
        else
            throw Exception("Second parameter of storage Join must be LEFT or INNER or RIGHT or FULL (without quotes).", ErrorCodes::BAD_ARGUMENTS);

        Names key_names;
        key_names.reserve(args.size() - 2);
        for (size_t i = 2, size = args.size(); i < size; ++i)
        {
            const ASTIdentifier * key = typeid_cast<ASTIdentifier *>(&*args[i]);
            if (!key)
                throw Exception("Parameter №" + toString(i + 1) + " of storage Join don't look like column name.", ErrorCodes::BAD_ARGUMENTS);

            key_names.push_back(key->name);
        }

        return StorageJoin::create(
            data_path, table_name,
            key_names, kind, strictness,
            columns, materialized_columns, alias_columns, column_defaults);
    }
    else if (name == "MySQL") {
        if (!args_ptr || args_ptr->size() != 5)
            throw Exception(
                "Storage MySQL requires exactly 5 parameters: MySQL('host:port', database, table, user, password).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        ASTs & args = *args_ptr;
        for (int i = 0; i < 5; i++)
            args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], local_context);
        return StorageMySQL::create(table_name, static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>(),
            static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>(),
            static_cast<const ASTLiteral &>(*args[2]).value.safeGet<String>(),
            static_cast<const ASTLiteral &>(*args[3]).value.safeGet<String>(),
            static_cast<const ASTLiteral &>(*args[4]).value.safeGet<String>(),
            columns, materialized_columns, alias_columns, column_defaults, context);
    }
    else if (name == "ODBC") {
        if (!args_ptr || args_ptr->size() != 2)
            throw Exception(
                "Storage ODBC requires exactly 2 parameters: ODBC(database, table).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        ASTs & args = *args_ptr;
        for (int i = 0; i < 2; i++)
            args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], local_context);
        return StorageODBC::create(table_name, static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>(),
            static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>(),
            columns, materialized_columns, alias_columns, column_defaults, context);
    }
    else if (name == "Memory")
    {
        check_arguments_empty();
        return StorageMemory::create(table_name, columns, materialized_columns, alias_columns, column_defaults);
    }
    else if (name == "Null")
    {
        return StorageNull::create(table_name, columns, materialized_columns, alias_columns, column_defaults);
    }
    else if (name == "Merge")
    {
        /** In query, the name of database is specified as table engine argument which contains source tables,
          *  as well as regex for source-table names.
          */

        if (!args_ptr || args_ptr->size() != 2)
            throw Exception("Storage Merge requires exactly 2 parameters"
                " - name of source database and regexp for table names.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        ASTs & args = *args_ptr;

        args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], local_context);
        args[1] = evaluateConstantExpressionAsLiteral(args[1], local_context);

        String source_database         = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>();
        String table_name_regexp    = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>();

        return StorageMerge::create(
            table_name, columns,
            materialized_columns, alias_columns, column_defaults,
            source_database, table_name_regexp, context);
    }
    else if (name == "Distributed")
    {
        /** Arguments of engine is following:
          * - name of cluster in configuration;
          * - name of remote database;
          * - name of remote table;
          *
          * Remote database may be specified in following form:
          * - identifier;
          * - constant expression with string result, like currentDatabase();
          * -- string literal as specific case;
          * - empty string means 'use default database from cluster'.
          */

        if (!args_ptr || !(args_ptr->size() == 3 || args_ptr->size() == 4))
            throw Exception("Storage Distributed requires 3 or 4 parameters"
                " - name of configuration section with list of remote servers, name of remote database, name of remote table,"
                " sharding key expression (optional).", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        ASTs & args = *args_ptr;

        String cluster_name = getClusterName(*args[0]);

        args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], local_context);
        args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], local_context);

        String remote_database     = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>();
        String remote_table     = static_cast<const ASTLiteral &>(*args[2]).value.safeGet<String>();

        const auto & sharding_key = args.size() == 4 ? args[3] : nullptr;

        /// Check that sharding_key exists in the table and has numeric type.
        if (sharding_key)
        {
            auto sharding_expr = ExpressionAnalyzer(sharding_key, context, nullptr, *columns).getActions(true);
            const Block & block = sharding_expr->getSampleBlock();

            if (block.columns() != 1)
                throw Exception("Sharding expression must return exactly one column", ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS);

            auto type = block.getByPosition(0).type;

            if (!type->isNumeric())
                throw Exception("Sharding expression has type " + type->getName() +
                    ", but should be one of integer type", ErrorCodes::TYPE_MISMATCH);
        }

        return StorageDistributed::create(
            table_name, columns,
            materialized_columns, alias_columns, column_defaults,
            remote_database, remote_table, cluster_name,
            context, sharding_key, data_path);
    }
    else if (name == "Buffer")
    {
        /** Buffer(db, table, num_buckets, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)
          *
          * db, table - in which table to put data from buffer.
          * num_buckets - level of parallelism.
          * min_time, max_time, min_rows, max_rows, min_bytes, max_bytes - conditions for flushing the buffer.
          */

        if (!args_ptr || args_ptr->size() != 9)
            throw Exception("Storage Buffer requires 9 parameters: "
                " destination_database, destination_table, num_buckets, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        ASTs & args = *args_ptr;

        args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], local_context);
        args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], local_context);

        String destination_database = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>();
        String destination_table     = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>();

        UInt64 num_buckets = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), typeid_cast<ASTLiteral &>(*args[2]).value);

        Int64 min_time = applyVisitor(FieldVisitorConvertToNumber<Int64>(), typeid_cast<ASTLiteral &>(*args[3]).value);
        Int64 max_time = applyVisitor(FieldVisitorConvertToNumber<Int64>(), typeid_cast<ASTLiteral &>(*args[4]).value);
        UInt64 min_rows = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), typeid_cast<ASTLiteral &>(*args[5]).value);
        UInt64 max_rows = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), typeid_cast<ASTLiteral &>(*args[6]).value);
        UInt64 min_bytes = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), typeid_cast<ASTLiteral &>(*args[7]).value);
        UInt64 max_bytes = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), typeid_cast<ASTLiteral &>(*args[8]).value);

        return StorageBuffer::create(
            table_name, columns,
            materialized_columns, alias_columns, column_defaults,
            context,
            num_buckets,
            StorageBuffer::Thresholds{min_time, min_rows, min_bytes},
            StorageBuffer::Thresholds{max_time, max_rows, max_bytes},
            destination_database, destination_table);
    }
    else if (name == "Kafka")
    {
#if USE_RDKAFKA
        /** Arguments of engine is following:
          * - Kafka broker list
          * - List of topics
          * - Group ID (may be a constaint expression with a string result)
          * - Message format (string)
          * - Schema (optional, if the format supports it)
          */

        if (!args_ptr || !(args_ptr->size() == 4 || args_ptr->size() == 5))
            throw Exception(
                "Storage Kafka requires 4 parameters"
                " - Kafka broker list, list of topics to consume, consumer group ID, message format",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        ASTs & args = *args_ptr;

        String brokers;
        auto ast = typeid_cast<ASTLiteral *>(&*args[0]);
        if (ast && ast->value.getType() == Field::Types::String)
            brokers = safeGet<String>(ast->value);
        else
            throw Exception(String("Kafka broker list must be a string"), ErrorCodes::BAD_ARGUMENTS);

        args[1] = evaluateConstantExpressionAsLiteral(args[1], local_context);
        args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], local_context);
        args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(args[3], local_context);

        // Additionally parse schema if supported
        String schema;
        if (args.size() == 5)
        {
            args[4] = evaluateConstantExpressionOrIdentifierAsLiteral(args[4], local_context);
            schema = static_cast<const ASTLiteral &>(*args[4]).value.safeGet<String>();
        }

        // Parse topic list and consumer group
        Names topics;
        topics.push_back(static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>());
        String group = static_cast<const ASTLiteral &>(*args[2]).value.safeGet<String>();

        // Parse format from string
        String format;
        ast = typeid_cast<ASTLiteral *>(&*args[3]);
        if (ast && ast->value.getType() == Field::Types::String)
            format = safeGet<String>(ast->value);
        else
            throw Exception(String("Format must be a string"), ErrorCodes::BAD_ARGUMENTS);

        return StorageKafka::create(
            table_name, database_name, context, columns,
            materialized_columns, alias_columns, column_defaults,
            brokers, group, topics, format, schema);
#else
            throw Exception{"Storage `Kafka` disabled because ClickHouse built without kafka support.", ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
    else if (endsWith(name, "MergeTree"))
    {
        /** [Replicated][|Summing|Collapsing|Aggregating|Unsorted|Replacing|Graphite]MergeTree (2 * 7 combinations) engines
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
          * UnsortedMergeTree(date, index_granularity)  TODO Add description below.
          *
          * Alternatively, if experimental_allow_extended_storage_definition_syntax setting is specified,
          * you can specify:
          *  - Partitioning expression in the PARTITION BY clause;
          *  - Primary key in the ORDER BY clause;
          *  - Sampling expression in the SAMPLE BY clause;
          *  - Additional MergeTreeSettings in the SETTINGS clause;
          */

        String name_part = name.substr(0, name.size() - strlen("MergeTree"));

        bool replicated = startsWith(name_part, "Replicated");
        if (replicated)
            name_part = name_part.substr(strlen("Replicated"));

        MergeTreeData::MergingParams merging_params;
        merging_params.mode = MergeTreeData::MergingParams::Ordinary;

        const bool allow_extended_storage_def =
            attach || local_context.getSettingsRef().experimental_allow_extended_storage_definition_syntax;

        if (name_part == "Collapsing")
            merging_params.mode = MergeTreeData::MergingParams::Collapsing;
        else if (name_part == "Summing")
            merging_params.mode = MergeTreeData::MergingParams::Summing;
        else if (name_part == "Aggregating")
            merging_params.mode = MergeTreeData::MergingParams::Aggregating;
        else if (name_part == "Unsorted")
            merging_params.mode = MergeTreeData::MergingParams::Unsorted;
        else if (name_part == "Replacing")
            merging_params.mode = MergeTreeData::MergingParams::Replacing;
        else if (name_part == "Graphite")
            merging_params.mode = MergeTreeData::MergingParams::Graphite;
        else if (!name_part.empty())
            throw Exception(
                "Unknown storage " + name + getMergeTreeVerboseHelp(allow_extended_storage_def),
                ErrorCodes::UNKNOWN_STORAGE);

        ASTs args;
        if (args_ptr)
            args = *args_ptr;

        /// NOTE Quite complicated.

        bool is_extended_storage_def =
            storage_def.partition_by || storage_def.order_by || storage_def.sample_by || storage_def.settings;

        if (is_extended_storage_def && !allow_extended_storage_def)
            throw Exception(
                "Extended storage definition syntax (PARTITION BY, ORDER BY, SAMPLE BY and SETTINGS clauses) "
                "is disabled. Enable it with experimental_allow_extended_storage_definition_syntax user setting");

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
            if (merging_params.mode == MergeTreeData::MergingParams::Unsorted)
            {
                if (args.size() == min_num_params && allow_extended_storage_def)
                    is_extended_storage_def = true;
                else
                {
                    add_mandatory_param("name of column with date");
                    add_mandatory_param("index granularity");
                }
            }
            else
            {
                add_mandatory_param("name of column with date");
                add_optional_param("sampling element of primary key");
                add_mandatory_param("primary key expression");
                add_mandatory_param("index granularity");
            }
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
        }

        if (args.size() < min_num_params || args.size() > max_num_params)
        {
            String msg;
            if (is_extended_storage_def)
                msg += "With extended storage definition syntax storage " + name + " requires ";
            else
                msg += "Storage " + name + " requires ";

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
            auto ast = typeid_cast<ASTLiteral *>(&*args[0]);
            if (ast && ast->value.getType() == Field::Types::String)
                zookeeper_path = safeGet<String>(ast->value);
            else
                throw Exception(
                    "Path in ZooKeeper must be a string literal" + getMergeTreeVerboseHelp(is_extended_storage_def),
                    ErrorCodes::BAD_ARGUMENTS);

            ast = typeid_cast<ASTLiteral *>(&*args[1]);
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

            args.erase(args.begin(), args.begin() + 2);
        }

        if (merging_params.mode == MergeTreeData::MergingParams::Collapsing)
        {
            if (auto ast = typeid_cast<ASTIdentifier *>(&*args.back()))
                merging_params.sign_column = ast->name;
            else
                throw Exception(
                    "Sign column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                    ErrorCodes::BAD_ARGUMENTS);

            args.pop_back();
        }
        else if (merging_params.mode == MergeTreeData::MergingParams::Replacing)
        {
            /// If the last element is not index_granularity or replica_name (a literal), then this is the name of the version column.
            if (!args.empty() && !typeid_cast<const ASTLiteral *>(&*args.back()))
            {
                if (auto ast = typeid_cast<ASTIdentifier *>(&*args.back()))
                    merging_params.version_column = ast->name;
                else
                    throw Exception(
                        "Version column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                        ErrorCodes::BAD_ARGUMENTS);

                args.pop_back();
            }
        }
        else if (merging_params.mode == MergeTreeData::MergingParams::Summing)
        {
            /// If the last element is not index_granularity or replica_name (a literal), then this is a list of summable columns.
            if (!args.empty() && !typeid_cast<const ASTLiteral *>(&*args.back()))
            {
                merging_params.columns_to_sum = extractColumnNames(args.back());
                args.pop_back();
            }
        }
        else if (merging_params.mode == MergeTreeData::MergingParams::Graphite)
        {
            String graphite_config_name;
            String error_msg = "Last parameter of GraphiteMergeTree must be name (in single quotes) of element in configuration file with Graphite options";
            error_msg += getMergeTreeVerboseHelp(is_extended_storage_def);

            if (auto ast = typeid_cast<ASTLiteral *>(&*args.back()))
            {
                if (ast->value.getType() != Field::Types::String)
                    throw Exception(error_msg, ErrorCodes::BAD_ARGUMENTS);

                graphite_config_name = ast->value.get<String>();
            }
            else
                throw Exception(error_msg, ErrorCodes::BAD_ARGUMENTS);

            args.pop_back();
            setGraphitePatternsFromConfig(context, graphite_config_name, merging_params.graphite_params);
        }

        String date_column_name;
        ASTPtr partition_expr_list;
        ASTPtr primary_expr_list;
        ASTPtr sampling_expression;
        MergeTreeSettings storage_settings = context.getMergeTreeSettings();

        if (is_extended_storage_def)
        {
            if (storage_def.partition_by)
                partition_expr_list = extractKeyExpressionList(*storage_def.partition_by);

            if (storage_def.order_by)
                primary_expr_list = extractKeyExpressionList(*storage_def.order_by);

            if (storage_def.sample_by)
                sampling_expression = storage_def.sample_by->ptr();

            storage_settings.loadFromQuery(storage_def);
        }
        else
        {
            /// If there is an expression for sampling. MergeTree(date, [sample_key], primary_key, index_granularity)
            if (args.size() == 4)
            {
                sampling_expression = args[1];
                args.erase(args.begin() + 1);
            }

            /// Now only three parameters remain - date (or partitioning expression), primary_key, index_granularity.

            if (auto ast = typeid_cast<ASTIdentifier *>(args[0].get()))
                date_column_name = ast->name;
            else
                throw Exception(
                    "Date column name must be an unquoted string" + getMergeTreeVerboseHelp(is_extended_storage_def),
                    ErrorCodes::BAD_ARGUMENTS);

            if (merging_params.mode != MergeTreeData::MergingParams::Unsorted)
                primary_expr_list = extractKeyExpressionList(*args[1]);

            auto ast = typeid_cast<ASTLiteral *>(&*args.back());
            if (ast && ast->value.getType() == Field::Types::UInt64)
                storage_settings.index_granularity = safeGet<UInt64>(ast->value);
            else
                throw Exception(
                    "Index granularity must be a positive integer" + getMergeTreeVerboseHelp(is_extended_storage_def),
                    ErrorCodes::BAD_ARGUMENTS);
        }

        if (replicated)
            return StorageReplicatedMergeTree::create(
                zookeeper_path, replica_name, attach, data_path, database_name, table_name,
                columns, materialized_columns, alias_columns, column_defaults,
                context, primary_expr_list, date_column_name, partition_expr_list,
                sampling_expression, merging_params, storage_settings,
                has_force_restore_data_flag);
        else
            return StorageMergeTree::create(
                data_path, database_name, table_name,
                columns, materialized_columns, alias_columns, column_defaults, attach,
                context, primary_expr_list, date_column_name, partition_expr_list,
                sampling_expression, merging_params, storage_settings,
                has_force_restore_data_flag);
    }
    else
        throw Exception("Unknown storage " + name, ErrorCodes::UNKNOWN_STORAGE);
}


}
