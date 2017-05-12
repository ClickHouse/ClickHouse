#include <Poco/Util/Application.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Core/FieldVisitors.h>
#include <Common/StringUtils.h>

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
#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <unistd.h>


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
}


/** For StorageMergeTree: get the primary key as an ASTExpressionList.
  * It can be specified in the tuple: (CounterID, Date),
  *  or as one column: CounterID.
  */
static ASTPtr extractPrimaryKey(const ASTPtr & node)
{
    const ASTFunction * primary_expr_func = typeid_cast<const ASTFunction *>(&*node);

    if (primary_expr_func && primary_expr_func->name == "tuple")
    {
        /// Primary key is specified in tuple.
        return primary_expr_func->children.at(0);
    }
    else
    {
        /// Primary key consists of one column.
        auto res = std::make_shared<ASTExpressionList>();
        res->children.push_back(node);
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
            /// TODO Not only Float64
            pattern.function = AggregateFunctionFactory::instance().get(
                config.getString(config_element + ".function"), { std::make_shared<DataTypeFloat64>() });
        }
        else if (startsWith(key, "retention"))
        {
            pattern.retentions.emplace_back(
                Graphite::Retention{
                    .age         = config.getUInt(config_element + "." + key + ".age"),
                    .precision     = config.getUInt(config_element + "." + key + ".precision")});
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
    const Poco::Util::AbstractConfiguration & config = Poco::Util::Application::instance().config();

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


StoragePtr StorageFactory::get(
    const String & name,
    const String & data_path,
    const String & table_name,
    const String & database_name,
    Context & local_context,
    Context & context,
    ASTPtr & query,
    NamesAndTypesListPtr columns,
    const NamesAndTypesList & materialized_columns,
    const NamesAndTypesList & alias_columns,
    const ColumnDefaults & column_defaults,
    bool attach,
    bool has_force_restore_data_flag) const
{
    /// Check for some special types, that are not allowed to be stored in tables. Example: NULL data type.
    /// Exception: any type is allowed in View, because plain (non-materialized) View does not store anything itself.
    if (name != "View")
    {
        checkAllTypesAreAllowedInTable(*columns);
        checkAllTypesAreAllowedInTable(materialized_columns);
        checkAllTypesAreAllowedInTable(alias_columns);
    }

    if (name == "Log")
    {
        return StorageLog::create(
            data_path, table_name, columns,
            materialized_columns, alias_columns, column_defaults,
            context.getSettings().max_compress_block_size);
    }
    else if (name == "View")
    {
        return StorageView::create(
            table_name, database_name, context, query, columns,
            materialized_columns, alias_columns, column_defaults);
    }
    else if (name == "MaterializedView")
    {
        return StorageMaterializedView::create(
            table_name, database_name, context, query, columns,
            materialized_columns, alias_columns, column_defaults,
            attach);
    }
    else if (name == "TinyLog")
    {
        return StorageTinyLog::create(
            data_path, table_name, columns,
            materialized_columns, alias_columns, column_defaults,
            attach, context.getSettings().max_compress_block_size);
    }
    else if (name == "StripeLog")
    {
        return StorageStripeLog::create(
            data_path, table_name, columns,
            materialized_columns, alias_columns, column_defaults,
            attach, context.getSettings().max_compress_block_size);
    }
    else if (name == "File")
    {
        auto & func = typeid_cast<ASTFunction &>(*typeid_cast<ASTCreateQuery &>(*query).storage);
        auto & args = typeid_cast<ASTExpressionList &>(*func.arguments).children;

        constexpr auto error_msg = "Storage File requires 1 or 2 arguments: name of used format and source.";

        if (func.parameters)
            throw Exception(error_msg, ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS);

        if (args.empty() || args.size() > 2)
            throw Exception(error_msg, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        args[0] = evaluateConstantExpressionOrIdentidierAsLiteral(args[0], local_context);
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

            args[1] = evaluateConstantExpressionOrIdentidierAsLiteral(args[1], local_context);
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
        return StorageSet::create(
            data_path, table_name, columns,
            materialized_columns, alias_columns, column_defaults);
    }
    else if (name == "Join")
    {
        /// Join(ANY, LEFT, k1, k2, ...)

        ASTs & args_func = typeid_cast<ASTFunction &>(*typeid_cast<ASTCreateQuery &>(*query).storage).children;

        constexpr auto params_error_message = "Storage Join requires at least 3 parameters: Join(ANY|ALL, LEFT|INNER, keys...).";

        if (args_func.size() != 1)
            throw Exception(params_error_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

        if (args.size() < 3)
            throw Exception(params_error_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

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
    else if (name == "Memory")
    {
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
        ASTs & args_func = typeid_cast<ASTFunction &>(*typeid_cast<ASTCreateQuery &>(*query).storage).children;

        if (args_func.size() != 1)
            throw Exception("Storage Merge requires exactly 2 parameters"
                " - name of source database and regexp for table names.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

        if (args.size() != 2)
            throw Exception("Storage Merge requires exactly 2 parameters"
                " - name of source database and regexp for table names.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        args[0] = evaluateConstantExpressionOrIdentidierAsLiteral(args[0], local_context);
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
        ASTs & args_func = typeid_cast<ASTFunction &>(*typeid_cast<ASTCreateQuery &>(*query).storage).children;

        const auto params_error_message = "Storage Distributed requires 3 or 4 parameters"
            " - name of configuration section with list of remote servers, name of remote database, name of remote table,"
            " sharding key expression (optional).";

        if (args_func.size() != 1)
            throw Exception(params_error_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

        if (args.size() != 3 && args.size() != 4)
            throw Exception(params_error_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String cluster_name = getClusterName(*args[0]);

        args[1] = evaluateConstantExpressionOrIdentidierAsLiteral(args[1], local_context);
        args[2] = evaluateConstantExpressionOrIdentidierAsLiteral(args[2], local_context);

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
          * min_time, max_time, min_rows, max_rows, min_bytes, max_bytes - conditions for pushing out from the buffer.
          */

        ASTs & args_func = typeid_cast<ASTFunction &>(*typeid_cast<ASTCreateQuery &>(*query).storage).children;

        if (args_func.size() != 1)
            throw Exception("Storage Buffer requires 9 parameters: "
                " destination database, destination table, num_buckets, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

        if (args.size() != 9)
            throw Exception("Storage Buffer requires 9 parameters: "
                " destination_database, destination_table, num_buckets, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        args[0] = evaluateConstantExpressionOrIdentidierAsLiteral(args[0], local_context);
        args[1] = evaluateConstantExpressionOrIdentidierAsLiteral(args[1], local_context);

        String destination_database = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>();
        String destination_table     = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>();

        size_t num_buckets = applyVisitor(FieldVisitorConvertToNumber<size_t>(), typeid_cast<ASTLiteral &>(*args[2]).value);

        time_t min_time = applyVisitor(FieldVisitorConvertToNumber<size_t>(), typeid_cast<ASTLiteral &>(*args[3]).value);
        time_t max_time = applyVisitor(FieldVisitorConvertToNumber<size_t>(), typeid_cast<ASTLiteral &>(*args[4]).value);
        size_t min_rows = applyVisitor(FieldVisitorConvertToNumber<size_t>(), typeid_cast<ASTLiteral &>(*args[5]).value);
        size_t max_rows = applyVisitor(FieldVisitorConvertToNumber<size_t>(), typeid_cast<ASTLiteral &>(*args[6]).value);
        size_t min_bytes = applyVisitor(FieldVisitorConvertToNumber<size_t>(), typeid_cast<ASTLiteral &>(*args[7]).value);
        size_t max_bytes = applyVisitor(FieldVisitorConvertToNumber<size_t>(), typeid_cast<ASTLiteral &>(*args[8]).value);

        return StorageBuffer::create(
            table_name, columns,
            materialized_columns, alias_columns, column_defaults,
            context,
            num_buckets, {min_time, min_rows, min_bytes}, {max_time, max_rows, max_bytes},
            destination_database, destination_table);
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
          *  - an expression for sorting (either a scalar expression or a tuple from several);
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
          */

        const char * verbose_help = R"(

MergeTree is family of storage engines.

MergeTrees is different in two ways:
- it may be replicated and non-replicated;
- it may do different actions on merge: nothing; sign collapse; sum; apply aggregete functions.

So we have 14 combinations:
    MergeTree, CollapsingMergeTree, SummingMergeTree, AggregatingMergeTree, ReplacingMergeTree, UnsortedMergeTree, GraphiteMergeTree
    ReplicatedMergeTree, ReplicatedCollapsingMergeTree, ReplicatedSummingMergeTree, ReplicatedAggregatingMergeTree, ReplicatedReplacingMergeTree, ReplicatedUnsortedMergeTree, ReplicatedGraphiteMergeTree

In most of cases, you need MergeTree or ReplicatedMergeTree.

For replicated merge trees, you need to supply path in ZooKeeper and replica name as first two parameters.
Path in ZooKeeper is like '/clickhouse/tables/01/' where /clickhouse/tables/ is common prefix and 01 is shard name.
Replica name is like 'mtstat01-1' - it may be hostname or any suitable string identifying replica.
You may use macro substitutions for these parameters. It's like ReplicatedMergeTree('/clickhouse/tables/{shard}/', '{replica}'...
Look at <macros> section in server configuration file.

Next parameter (which is first for unreplicated tables and third for replicated tables) is name of date column.
Date column must exist in table and have type Date (not DateTime).
It is used for internal data partitioning and works like some kind of index.

If your source data doesn't have column of type Date, but have DateTime column, you may add values for Date column while loading,
 or you may INSERT your source data to table of type Log and then transform it with INSERT INTO t SELECT toDate(time) AS date, * FROM ...
If your source data doesn't have any date or time, you may just pass any constant for date column while loading.

Next parameter is optional sampling expression. Sampling expression is used to implement SAMPLE clause in query for approximate query execution.
If you don't need approximate query execution, simply omit this parameter.
Sample expression must be one of elements of primary key tuple. For example, if your primary key is (CounterID, EventDate, intHash64(UserID)), your sampling expression might be intHash64(UserID).

Next parameter is primary key tuple. It's like (CounterID, EventDate, intHash64(UserID)) - list of column names or functional expressions in round brackets. If your primary key have just one element, you may omit round brackets.

Careful choice of primary key is extremely important for processing short-time queries.

Next parameter is index (primary key) granularity. Good value is 8192. You have no reasons to use any other value.

For Collapsing mode, last parameter is name of sign column - special column that is used to 'collapse' rows with same primary key while merge.

For Summing mode, last parameter is optional list of columns to sum while merge. List is passed in round brackets, like (PageViews, Cost).
If this parameter is omitted, storage will sum all numeric columns except columns participated in primary key.

For Replacing mode, last parameter is optional name of 'version' column. While merging, for all rows with same primary key, only one row is selected: last row, if version column was not specified, or last row with maximum version value, if specified.


Examples:

MergeTree(EventDate, (CounterID, EventDate), 8192)

MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192)

CollapsingMergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192, Sign)

SummingMergeTree(EventDate, (OrderID, EventDate, BannerID, PhraseID, ContextType, RegionID, PageID, IsFlat, TypeID, ResourceNo), 8192)

SummingMergeTree(EventDate, (OrderID, EventDate, BannerID, PhraseID, ContextType, RegionID, PageID, IsFlat, TypeID, ResourceNo), 8192, (Shows, Clicks, Cost, CostCur, ShowsSumPosition, ClicksSumPosition, SessionNum, SessionLen, SessionCost, GoalsNum, SessionDepth))

ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/hits', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192)


For further info please read the documentation: https://clickhouse.yandex/
)";

        String name_part = name.substr(0, name.size() - strlen("MergeTree"));

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
        else if (name_part == "Unsorted")
            merging_params.mode = MergeTreeData::MergingParams::Unsorted;
        else if (name_part == "Replacing")
            merging_params.mode = MergeTreeData::MergingParams::Replacing;
        else if (name_part == "Graphite")
            merging_params.mode = MergeTreeData::MergingParams::Graphite;
        else if (!name_part.empty())
            throw Exception("Unknown storage " + name + verbose_help, ErrorCodes::UNKNOWN_STORAGE);

        ASTs & args_func = typeid_cast<ASTFunction &>(*typeid_cast<ASTCreateQuery &>(*query).storage).children;

        ASTs args;

        if (args_func.size() == 1)
            args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

        /// NOTE Quite complicated.
        size_t num_additional_params = (replicated ? 2 : 0)
            + (merging_params.mode == MergeTreeData::MergingParams::Collapsing)
            + (merging_params.mode == MergeTreeData::MergingParams::Graphite);

        String params_for_replicated;
        if (replicated)
            params_for_replicated =
                "\npath in ZooKeeper,"
                "\nreplica name,";

        if (merging_params.mode == MergeTreeData::MergingParams::Unsorted
            && args.size() != num_additional_params + 2)
        {
            String params =
                "\nname of column with date,"
                "\nindex granularity\n";

            throw Exception("Storage " + name + " requires "
                + toString(num_additional_params + 2) + " parameters: " + params_for_replicated + params + verbose_help,
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        if (merging_params.mode != MergeTreeData::MergingParams::Summing
            && merging_params.mode != MergeTreeData::MergingParams::Replacing
            && merging_params.mode != MergeTreeData::MergingParams::Unsorted
            && args.size() != num_additional_params + 3
            && args.size() != num_additional_params + 4)
        {
            String params =
                "\nname of column with date,"
                "\n[sampling element of primary key],"
                "\nprimary key expression,"
                "\nindex granularity\n";

            if (merging_params.mode == MergeTreeData::MergingParams::Collapsing)
                params += ", sign column\n";

            if (merging_params.mode == MergeTreeData::MergingParams::Graphite)
                params += ", 'config_element_for_graphite_schema'\n";

            throw Exception("Storage " + name + " requires "
                + toString(num_additional_params + 3) + " or "
                + toString(num_additional_params + 4) + " parameters: " + params_for_replicated + params + verbose_help,
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        if ((merging_params.mode == MergeTreeData::MergingParams::Summing
                || merging_params.mode == MergeTreeData::MergingParams::Replacing)
            && args.size() != num_additional_params + 3
            && args.size() != num_additional_params + 4
            && args.size() != num_additional_params + 5)
        {
            String params =
                "\nname of column with date,"
                "\n[sampling element of primary key],"
                "\nprimary key expression,"
                "\nindex granularity,";

            if (merging_params.mode == MergeTreeData::MergingParams::Summing)
                params += "\n[list of columns to sum]\n";
            else
                params += "\n[version]\n";

            throw Exception("Storage " + name + " requires "
                + toString(num_additional_params + 3) + " or "
                + toString(num_additional_params + 4) + " or "
                + toString(num_additional_params + 5) + " parameters: " + params_for_replicated + params + verbose_help,
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        /// For Replicated.
        String zookeeper_path;
        String replica_name;

        /// For all.
        String date_column_name;
        ASTPtr primary_expr_list;
        ASTPtr sampling_expression;
        UInt64 index_granularity;

        if (replicated)
        {
            auto ast = typeid_cast<ASTLiteral *>(&*args[0]);
            if (ast && ast->value.getType() == Field::Types::String)
                zookeeper_path = safeGet<String>(ast->value);
            else
                throw Exception(String("Path in ZooKeeper must be a string literal") + verbose_help, ErrorCodes::BAD_ARGUMENTS);

            ast = typeid_cast<ASTLiteral *>(&*args[1]);
            if (ast && ast->value.getType() == Field::Types::String)
                replica_name = safeGet<String>(ast->value);
            else
                throw Exception(String("Replica name must be a string literal") + verbose_help, ErrorCodes::BAD_ARGUMENTS);

            if (replica_name.empty())
                throw Exception(String("No replica name in config") + verbose_help, ErrorCodes::NO_REPLICA_NAME_GIVEN);

            args.erase(args.begin(), args.begin() + 2);
        }

        if (merging_params.mode == MergeTreeData::MergingParams::Collapsing)
        {
            if (auto ast = typeid_cast<ASTIdentifier *>(&*args.back()))
                merging_params.sign_column = ast->name;
            else
                throw Exception(String("Sign column name must be an unquoted string") + verbose_help, ErrorCodes::BAD_ARGUMENTS);

            args.pop_back();
        }
        else if (merging_params.mode == MergeTreeData::MergingParams::Replacing)
        {
            /// If the last element is not an index granularity (literal), then this is the name of the version column.
            if (!typeid_cast<const ASTLiteral *>(&*args.back()))
            {
                if (auto ast = typeid_cast<ASTIdentifier *>(&*args.back()))
                    merging_params.version_column = ast->name;
                else
                    throw Exception(String("Version column name must be an unquoted string") + verbose_help, ErrorCodes::BAD_ARGUMENTS);

                args.pop_back();
            }
        }
        else if (merging_params.mode == MergeTreeData::MergingParams::Summing)
        {
            /// If the last element is not an index granularity (literal), then this is a list of summable columns.
            if (!typeid_cast<const ASTLiteral *>(&*args.back()))
            {
                merging_params.columns_to_sum = extractColumnNames(args.back());
                args.pop_back();
            }
        }
        else if (merging_params.mode == MergeTreeData::MergingParams::Graphite)
        {
            String graphite_config_name;
            String error_msg = "Last parameter of GraphiteMergeTree must be name (in single quotes) of element in configuration file with Graphite options";
            error_msg += verbose_help;

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

        /// If there is an expression for sampling. MergeTree(date, [sample_key], primary_key, index_granularity)
        if (args.size() == 4)
        {
            sampling_expression = args[1];
            args.erase(args.begin() + 1);
        }

        /// Now only three parameters remain - date, primary_key, index_granularity.

        if (auto ast = typeid_cast<ASTIdentifier *>(&*args[0]))
            date_column_name = ast->name;
        else
            throw Exception(String("Date column name must be an unquoted string") + verbose_help, ErrorCodes::BAD_ARGUMENTS);

        if (merging_params.mode != MergeTreeData::MergingParams::Unsorted)
            primary_expr_list = extractPrimaryKey(args[1]);

        auto ast = typeid_cast<ASTLiteral *>(&*args.back());
        if (ast && ast->value.getType() == Field::Types::UInt64)
                index_granularity = safeGet<UInt64>(ast->value);
        else
            throw Exception(String("Index granularity must be a positive integer") + verbose_help, ErrorCodes::BAD_ARGUMENTS);

        if (replicated)
            return StorageReplicatedMergeTree::create(
                zookeeper_path, replica_name, attach, data_path, database_name, table_name,
                columns, materialized_columns, alias_columns, column_defaults,
                context, primary_expr_list, date_column_name,
                sampling_expression, index_granularity, merging_params,
                has_force_restore_data_flag,
                context.getMergeTreeSettings());
        else
            return StorageMergeTree::create(
                data_path, database_name, table_name,
                columns, materialized_columns, alias_columns, column_defaults, attach,
                context, primary_expr_list, date_column_name,
                sampling_expression, index_granularity, merging_params,
                has_force_restore_data_flag,
                context.getMergeTreeSettings());
    }
    else
        throw Exception("Unknown storage " + name, ErrorCodes::UNKNOWN_STORAGE);
}


}
