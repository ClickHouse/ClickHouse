#include <Storages/StorageTimeSeriesSelector.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesIDGenerator.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Read a required String literal argument as a value, without materializing a `Field`.
String getStringConstArgument(const ASTPtr & arg, const ContextPtr & context, std::string_view arg_name)
{
    auto [column, type] = evaluateConstantExpressionAsColumn(arg, context);
    /// Accept `Nullable`/`LowCardinality` wrappers: the previous `Field`-based code read the value
    /// via `operator[]`, which flattens wrappers, so a non-NULL `Nullable(String)`/
    /// `LowCardinality(String)` constant passed the String check. Preserve that, and still reject a
    /// NULL value as before.
    if (!isStringOrFixedString(removeLowCardinalityAndNullable(type)))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument '{}' must be a literal with type String, got {}", arg_name, type->getName());
    if (column->isNullAt(0))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument '{}' must be a literal with type String, got NULL", arg_name);
    return String(column->getDataAt(0));
}

}

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsBool filter_by_min_time_and_max_time;
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
}

namespace
{

/// An identifier for the raw `id` column of a table, qualified so that it resolves
/// to the table column and not to a same-named alias of the SELECT list.
ASTPtr makeQualifiedIDIdentifier(const StorageID & table_id)
{
    return make_intrusive<ASTIdentifier>(
        std::vector<String>{table_id.database_name, table_id.table_name, TimeSeriesColumnNames::ID});
}

/// An expression extracting the specified component of a multi-component series id.
ASTPtr makeIDComponent(ASTPtr id, UInt64 component_index)
{
    return makeASTFunction("tupleElement", std::move(id), make_intrusive<ASTLiteral>(component_index));
}

/// Returns the canonical id generator for `id_data_type` if it is also the effective id
/// generator of the table, null otherwise. The resolution order of the effective generator
/// mirrors `TimeSeriesSink`: the `id_generator` setting, then the DEFAULT of the tags-table
/// `id` column, then the canonical generator.
ASTPtr tryGetCanonicalIDGenerator(
    const DataTypePtr & id_data_type,
    const TimeSeriesSettings & time_series_settings,
    const ColumnsDescription & tags_table_columns)
{
    ASTPtr canonical_generator = TimeSeriesIDGenerator::tryGetDefault(id_data_type);
    if (!canonical_generator)
        return nullptr;

    ASTPtr id_generator = time_series_settings[TimeSeriesSetting::id_generator].value;
    if (!id_generator)
    {
        if (const auto * tags_id_column = tags_table_columns.tryGet(TimeSeriesColumnNames::ID))
            id_generator = tags_id_column->default_desc.expression;
    }
    if (id_generator && (id_generator->getTreeHash(/*ignore_aliases=*/true) != canonical_generator->getTreeHash(/*ignore_aliases=*/true)))
        return nullptr;

    return canonical_generator;
}

}

StorageTimeSeriesSelector::Configuration StorageTimeSeriesSelector::getConfiguration(ASTs & args, const ContextPtr & context)
{
    std::string_view function_name = "timeSeriesSelector";

    size_t min_num_args = 4;
    size_t max_num_args = 5;

    if ((args.size() < min_num_args) || (args.size() > max_num_args))
    {
        std::string_view expected_args = "[database, ] time_series_table, selector, min_time, max_time";
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function '{}' requires {}..{} arguments: {}({})",
                        function_name, min_num_args, max_num_args, function_name, expected_args);
    }

    size_t argument_index = 0;

    StorageID time_series_storage_id = StorageID::createEmpty();

    if (args.size() == min_num_args)
    {
        /// timeSeriesSelector( [my_db.]my_time_series_table, ... )
        if (const auto * id = args[argument_index]->as<ASTIdentifier>())
        {
            if (auto table_id = id->createTable())
            {
                time_series_storage_id = table_id->getTableId();
                ++argument_index;
            }
        }
    }

    if (time_series_storage_id.empty())
    {
        if (args.size() == min_num_args)
        {
            /// timeSeriesSelector( 'my_time_series_table', ... )
            time_series_storage_id.table_name = getStringConstArgument(args[argument_index++], context, "table_name");
        }
        else
        {
            /// timeSeriesSelector( 'mydb', 'my_time_series_table', ... )
            time_series_storage_id.database_name = getStringConstArgument(args[argument_index++], context, "database_name");
            time_series_storage_id.table_name = getStringConstArgument(args[argument_index++], context, "table_name");
        }
    }

    time_series_storage_id = context->resolveStorageID(time_series_storage_id);

    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(time_series_storage_id, context));
    auto time_series_metadata = time_series_storage->getInMemoryMetadataPtr(context, false);
    auto [timestamp_data_type, scalar_data_type] = splitTimeSeriesType(
        time_series_metadata->columns.get(TimeSeriesColumnNames::TimeSeries).type);
    auto tags_target = time_series_storage->getTargetTable(ViewTarget::Tags, context);
    auto tags_target_metadata = tags_target->getInMemoryMetadataPtr(context, false);
    DataTypePtr id_data_type = tags_target_metadata->columns.get(TimeSeriesColumnNames::ID).type;

    /// With the canonical id generator for a two-component id `Tuple(F, S)` the second component
    /// alone (a hash of all the tags) identifies a time series, so the selector can read and
    /// return only that component. This narrowing is applied when S is a LowCardinality type:
    /// the identifiers then stay dictionary-encoded end-to-end, and both the `IN <ids>` filter
    /// over the samples table and `timeSeriesIdToGroup` run per dictionary key instead of per row.
    DataTypePtr table_id_data_type = id_data_type;
    bool narrow_id_to_second_component = false;
    if (const auto * id_tuple_type = typeid_cast<const DataTypeTuple *>(table_id_data_type.get());
        id_tuple_type && (id_tuple_type->getElements().size() == 2)
        && typeid_cast<const DataTypeLowCardinality *>(id_tuple_type->getElements()[1].get()))
    {
        auto samples_target_metadata = time_series_storage->getTargetTable(ViewTarget::Samples, context)->getInMemoryMetadataPtr(context, false);
        auto samples_id_column = samples_target_metadata->getColumns().tryGetPhysical(TimeSeriesColumnNames::ID);
        bool samples_table_stores_same_id_type = samples_id_column && (samples_id_column->type->getName() == table_id_data_type->getName());

        if (samples_table_stores_same_id_type
            && tryGetCanonicalIDGenerator(table_id_data_type, *time_series_storage->getStorageSettings(), tags_target_metadata->columns))
        {
            narrow_id_to_second_component = true;
            id_data_type = id_tuple_type->getElements()[1];
        }
    }

    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);

    PrometheusQueryTree selector{getStringConstArgument(args[argument_index++], context, "selector")};

    auto [min_time_field, min_time_type] = evaluateConstantExpression(args[argument_index++], context);
    auto [max_time_field, max_time_type] = evaluateConstantExpression(args[argument_index++], context);

    auto min_time = parseTimeSeriesTimestamp(min_time_field, min_time_type, timestamp_scale);
    auto max_time = parseTimeSeriesTimestamp(max_time_field, max_time_type, timestamp_scale);

    chassert(argument_index == args.size());

    Configuration config;
    config.time_series_storage_id = std::move(time_series_storage_id);
    config.id_data_type = std::move(id_data_type);
    config.timestamp_data_type = std::move(timestamp_data_type);
    config.scalar_data_type = std::move(scalar_data_type);
    config.table_id_data_type = std::move(table_id_data_type);
    config.narrow_id_to_second_component = narrow_id_to_second_component;
    config.selector = std::move(selector);
    config.min_time = min_time;
    config.max_time = max_time;
    return config;
}

StorageTimeSeriesSelector::StorageTimeSeriesSelector(
    const StorageID & table_id_, const ColumnsDescription & columns_, const Configuration & config_)
    : StorageWithCommonVirtualColumns{table_id_}
    , config(config_)
    , log(getLogger("StorageTimeSeriesSelector"))
{
    const auto * node = config.selector.getRoot();
    if (!node || (node->node_type != PrometheusQueryTree::NodeType::InstantSelector))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} is not an instant selector", quoteString(config.selector.toString()));

    if (config.min_time > config.max_time)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Max time {} is less than min time {}",
                        Field{config.min_time}, Field{config.max_time});

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageTimeSeriesSelector::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}


namespace
{
    /// Makes an AST for the expression referencing a tag value.
    ASTPtr tagNameToAST(const String & tag_name, const std::unordered_map<String, String> & column_name_by_tag_name)
    {
        if (tag_name == TimeSeriesTagNames::MetricName)
            return make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName);

        auto it = column_name_by_tag_name.find(tag_name);
        if (it != column_name_by_tag_name.end())
            return make_intrusive<ASTIdentifier>(it->second);

        /// arrayElement() can be used to extract a value from a Map too.
        return makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags), make_intrusive<ASTLiteral>(tag_name));
    }

    ASTPtr matcherToAST(const PrometheusQueryTree::Matcher & matcher, const std::unordered_map<String, String> & column_name_by_tag_name)
    {
        std::string_view function_name;
        bool add_anchors = false;
        bool add_not = false;

        auto matcher_type = matcher.matcher_type;
        switch (matcher_type)
        {
            case PrometheusQueryTree::MatcherType::EQ:  function_name = "equals"; break;
            case PrometheusQueryTree::MatcherType::NE:  function_name = "notEquals"; break;
            case PrometheusQueryTree::MatcherType::RE:  function_name = "match"; add_anchors = true; break;
            case PrometheusQueryTree::MatcherType::NRE: function_name = "match"; add_anchors = true; add_not = true; break;
        }

        String value = matcher.label_value;
        if (add_anchors)
        {
            if (!value.starts_with('^'))
                value = '^' + value;
            if (!value.ends_with('$'))
                value += '$';
        }
        ASTPtr res = makeASTFunction(function_name, tagNameToAST(matcher.label_name, column_name_by_tag_name), make_intrusive<ASTLiteral>(value));
        if (add_not)
            res = makeASTFunction("not", res);
        return res;
    }

    ASTPtr makeWhereFilterForTagsTable(
        const PrometheusQueryTree::MatcherList & matchers,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        const std::optional<DateTime64> & min_time,
        const std::optional<DateTime64> & max_time,
        const DataTypePtr & timestamp_data_type)
    {
        ASTs asts;
        for (const auto & matcher : matchers)
            asts.push_back(matcherToAST(matcher, column_name_by_tag_name));

        if (asts.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Instant selector without matchers is not allowed");

        if (min_time)
        {
            /// tags_table.max_time >= min_time
            asts.push_back(makeASTFunction(
                "greaterOrEquals",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MaxTime),
                timeSeriesTimestampToAST(*min_time, timestamp_data_type)));
        }

        if (max_time)
        {
            /// tags_table.min_time <= max_time
            asts.push_back(makeASTFunction(
                "lessOrEquals",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MinTime),
                timeSeriesTimestampToAST(*max_time, timestamp_data_type)));
        }

        return makeASTForLogicalAnd(std::move(asts));
    }

    /// What the tags-table subquery returns (and stores in the per-query tags collector)
    /// as the identifier of a time series.
    enum class TagsSubqueryIDForm
    {
        /// timeSeriesStoreTags(id, ...) - the raw `id` column.
        Full,

        /// timeSeriesStoreTags(tupleElement(id, 2), ...) - only the second id component
        /// (see `Configuration::narrow_id_to_second_component`).
        SecondComponent,

        /// tuple(tupleElement(id, 1), timeSeriesStoreTags(tupleElement(id, 2), ...)) - the second
        /// component is stored in the collector, but the returned ids are full-shaped, so the set
        /// built from them can serve primary-key index analysis over the samples table.
        SecondComponentInFullShapedID,
    };

    ASTPtr makeSelectQueryFromTagsTable(
        const StorageID & tags_table_id,
        const PrometheusQueryTree::MatcherList & matchers,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        const std::optional<DateTime64> & min_time,
        const std::optional<DateTime64> & max_time,
        const DataTypePtr & timestamp_data_type,
        TagsSubqueryIDForm id_form)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        /// SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name, tag_name1, tag_column1, ...)
        /// (with the identifier shaped according to `id_form`, see `TagsSubqueryIDForm`)
        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            auto & select_list = select_list_exp->children;

            ASTs args;
            ASTPtr stored_id = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID);
            if (id_form != TagsSubqueryIDForm::Full)
                stored_id = makeIDComponent(std::move(stored_id), 2);
            args.push_back(std::move(stored_id));
            args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));
            args.push_back(make_intrusive<ASTLiteral>(TimeSeriesTagNames::MetricName));
            args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

            for (const auto & [tag_name, column_name] : column_name_by_tag_name)
            {
                args.push_back(make_intrusive<ASTLiteral>(tag_name));
                args.push_back(make_intrusive<ASTIdentifier>(column_name));
            }

            ASTPtr select_expression = makeASTFunction("timeSeriesStoreTags", std::move(args));
            if (id_form == TagsSubqueryIDForm::SecondComponentInFullShapedID)
            {
                /// `timeSeriesStoreTags` returns its identifier argument with the dictionary
                /// encoding stripped (the function is executed on materialized arguments), so the
                /// reassembled ids have a fully-materialized element type. This is intentional:
                /// probing a set of materialized tuples (the column is converted once per block)
                /// measured faster than probing dictionary-encoded tuples row by row.
                select_expression = makeASTFunction(
                    "tuple",
                    makeIDComponent(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID), 1),
                    std::move(select_expression));
            }

            select_list.push_back(std::move(select_expression));
            select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
        }

        /// FROM tags_table_id
        auto tables = make_intrusive<ASTTablesInSelectQuery>();

        {
            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(tags_table_id);
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table->table_expression = table_exp;
            tables->children.push_back(table);

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        /// WHERE <filter>
        {
            auto where_filter = makeWhereFilterForTagsTable(matchers, column_name_by_tag_name, min_time, max_time, timestamp_data_type);
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_filter));
        }

        /// Wrap the select query into ASTSelectWithUnionQuery.
        auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
        {
            select_with_union_query->union_mode = SelectUnionMode::UNION_DEFAULT;
            auto list_of_selects = make_intrusive<ASTExpressionList>();
            list_of_selects->children.push_back(std::move(select_query));
            select_with_union_query->children.push_back(std::move(list_of_selects));
            select_with_union_query->list_of_selects = select_with_union_query->children.back();
        }

        return select_with_union_query;
    }

    ASTPtr makeWhereFilterForDataTable(
        ASTPtr select_query_from_tags_table,
        DateTime64 min_time,
        DateTime64 max_time,
        const DataTypePtr & timestamp_data_type,
        ASTPtr id_in_left_arg,
        ASTs whole_metric_id_range_conditions)
    {
        ASTs conditions;

        /// Emit the timestamp range BEFORE the `id IN <set>` condition: on the default schema
        /// (ORDER BY (id, timestamp)) primary-key pruning already returns mostly granules of matched
        /// series, so the `id IN <set>` check passes almost all read rows, while the timestamp range
        /// is the selective condition (e.g. a few rows per 32768-row granule for a short lookback).
        /// The PREWHERE optimizer keeps this order whenever its selectivity estimation is inconclusive,
        /// and running the cheap timestamp comparison before the hash-set probe of `in` significantly
        /// reduces the scan CPU of short-window selectors.

        /// timestamp >= min_time
        conditions.push_back(makeASTFunction(
            "greaterOrEquals",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
            timeSeriesTimestampToAST(min_time, timestamp_data_type)));

        /// timestamp <= max_time
        conditions.push_back(makeASTFunction(
            "lessOrEquals",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
            timeSeriesTimestampToAST(max_time, timestamp_data_type)));

        /// id IN (SELECT id FROM (select_id_query))
        /// Wrap the SELECT in ASTSubquery so it formats with surrounding parentheses.
        /// The left argument is passed by the caller: it is the bare `id` identifier normally
        /// (which binds to the narrowed alias of the SELECT list when the id is narrowed),
        /// or the qualified raw column when the set is full-shaped for index analysis.
        auto select_as_subquery = make_intrusive<ASTSubquery>(std::move(select_query_from_tags_table));
        conditions.push_back(makeASTFunction("in", std::move(id_in_left_arg), std::move(select_as_subquery)));

        /// For a whole-metric selector over a metric-clustered id layout two more conditions are
        /// added: <raw id column> >= tuple(hash(metric_name), min) AND <raw id column> <= tuple(
        /// hash(metric_name), max). They are a superset of the `id IN <set>` condition above, so
        /// the returned rows do not change; their purpose is to give the primary-key index
        /// analysis a continuous key range instead of the large set (see readImpl).
        for (auto & condition : whole_metric_id_range_conditions)
            conditions.push_back(std::move(condition));

        return makeASTForLogicalAnd(std::move(conditions));
    }

    ASTPtr makeSelectQueryFromDataTable(const StorageID & data_table_id,
                                        ASTPtr select_query_from_tags_table,
                                        DateTime64 min_time,
                                        DateTime64 max_time,
                                        const DataTypePtr & timestamp_data_type,
                                        ASTPtr id_select_expression,
                                        ASTPtr id_in_left_arg,
                                        ASTs whole_metric_id_range_conditions)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        /// SELECT id, timestamp, value
        ///
        /// The columns are read as is, without casts to the data types declared by this storage.
        /// A cast aliased in the SELECT list (e.g. `toDateTime64(timestamp, 3) AS timestamp`) would
        /// shadow the raw column, and the WHERE conditions below would wrap the primary key
        /// columns, degrading the index analysis and the ordering of the PREWHERE conditions.
        /// The casts to the declared types are applied by an outer SELECT instead
        /// (see `makeSelectQuery`).
        ///
        /// The `id` entry of the SELECT list is supplied by the caller: it is the bare column
        /// normally, or an expression aliased `AS id` when the id is narrowed.
        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            auto & select_list = select_list_exp->children;

            select_list.push_back(std::move(id_select_expression));
            select_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
            select_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value));

            select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
        }

        /// FROM data_table_id
        auto tables = make_intrusive<ASTTablesInSelectQuery>();

        {
            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(data_table_id);
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table->table_expression = table_exp;
            tables->children.push_back(table);

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        /// WHERE (timestamp >= min_time) AND (timestamp <= max_time) AND (id IN <select_query_from_tags_table>)
        ///
        /// where <select_query_from_tags_table> is roughly:
        ///   SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name, ...) FROM tags_table WHERE <matchers>
        {
            auto where_filter = makeWhereFilterForDataTable(
                select_query_from_tags_table, min_time, max_time, timestamp_data_type,
                std::move(id_in_left_arg), std::move(whole_metric_id_range_conditions));
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_filter));
        }

        /// Wrap the select query into ASTSelectWithUnionQuery.
        auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
        select_with_union_query->union_mode = SelectUnionMode::UNION_DEFAULT;
        auto list_of_selects = make_intrusive<ASTExpressionList>();
        list_of_selects->children.push_back(std::move(select_query));
        select_with_union_query->children.push_back(std::move(list_of_selects));
        select_with_union_query->list_of_selects = select_with_union_query->children.back();

        return select_with_union_query;
    }

    /// Makes the final select query by wrapping the select query from the data table into an outer
    /// SELECT which casts the columns to the data types expected by this storage:
    ///
    /// SELECT _CAST(id, 'UInt64') AS id, _CAST(timestamp, 'DateTime64(3)') AS timestamp, _CAST(value, 'Float64') AS value
    /// FROM (select_query_from_data_table)
    ///
    /// The inner query reads the samples table columns as is (see makeSelectQueryFromDataTable()),
    /// so its result types are the physical column types, which can differ from the expected ones
    /// (e.g. a samples table can store `timestamp` with a different timezone). Casting in an outer
    /// SELECT keeps the WHERE conditions of the inner query on the bare primary key columns, and
    /// the casts run only for the rows which passed the filter. The internal `_CAST` is used here
    /// because it returns exactly the specified type (`CAST` and conversion functions like
    /// `toDateTime64` keep the timezone of the casted expression), and it is free when the type
    /// already matches.
    ASTPtr makeSelectQuery(ASTPtr select_query_from_data_table,
                           const DataTypePtr & id_data_type,
                           const DataTypePtr & timestamp_data_type,
                           const DataTypePtr & scalar_data_type)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        /// SELECT _CAST(id, 'UInt64') AS id, _CAST(timestamp, 'DateTime64(3)') AS timestamp, _CAST(value, 'Float64') AS value
        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            auto & select_list = select_list_exp->children;

            select_list.push_back(makeASTFunction(
                "_CAST", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID), make_intrusive<ASTLiteral>(id_data_type->getName())));
            select_list.back()->setAlias(TimeSeriesColumnNames::ID);

            select_list.push_back(makeASTFunction(
                "_CAST",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                make_intrusive<ASTLiteral>(timestamp_data_type->getName())));
            select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

            select_list.push_back(makeASTFunction(
                "_CAST", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value), make_intrusive<ASTLiteral>(scalar_data_type->getName())));
            select_list.back()->setAlias(TimeSeriesColumnNames::Value);

            select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
        }

        /// FROM (select_query_from_data_table)
        {
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->subquery = make_intrusive<ASTSubquery>(std::move(select_query_from_data_table));
            table_exp->children.push_back(table_exp->subquery);

            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            table->table_expression = table_exp;
            table->children.push_back(table->table_expression);

            auto tables = make_intrusive<ASTTablesInSelectQuery>();
            tables->children.push_back(table);

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        /// Wrap the select query into ASTSelectWithUnionQuery.
        auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
        select_with_union_query->union_mode = SelectUnionMode::UNION_DEFAULT;
        auto list_of_selects = make_intrusive<ASTExpressionList>();
        list_of_selects->children.push_back(std::move(select_query));
        select_with_union_query->children.push_back(std::move(list_of_selects));
        select_with_union_query->list_of_selects = select_with_union_query->children.back();

        return select_with_union_query;
    }

    /// Makes a mapping from a tag name to a column name.
    std::unordered_map<String, String> makeColumnNameByTagNameMap(const TimeSeriesSettings & storage_settings)
    {
        std::unordered_map<String, String> res;
        const Map & tags_to_columns = storage_settings[TimeSeriesSetting::tags_to_columns];
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
            const auto & tag_name = tuple.at(0).safeGet<String>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            res[tag_name] = column_name;
        }
        return res;
    }

    /// Constant ASTs for the minimum and the maximum value of one component of a multi-component
    /// series id. The supported types are the ones `TimeSeriesIDGenerator` can generate hashes for.
    std::optional<std::pair<ASTPtr, ASTPtr>> makeMinMaxLiteralsForIDComponent(const IDataType & type)
    {
        /// A LowCardinality component has the value space of its dictionary type: the range bounds
        /// are the dictionary type's bounds (constants have no dictionary encoding of their own).
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(&type))
            return makeMinMaxLiteralsForIDComponent(*low_cardinality_type->getDictionaryType());

        WhichDataType which(type);

        if (which.isUInt64())
            return {{make_intrusive<ASTLiteral>(UInt64{0}), make_intrusive<ASTLiteral>(std::numeric_limits<UInt64>::max())}};

        if (which.isUInt128())
            return {{make_intrusive<ASTLiteral>(UInt128{0}), make_intrusive<ASTLiteral>(std::numeric_limits<UInt128>::max())}};

        if (which.isUUID())
        {
            return {{makeASTFunction("toUUID", make_intrusive<ASTLiteral>("00000000-0000-0000-0000-000000000000")),
                     makeASTFunction("toUUID", make_intrusive<ASTLiteral>("ffffffff-ffff-ffff-ffff-ffffffffffff"))}};
        }

        if (which.isFixedString() && (typeid_cast<const DataTypeFixedString &>(type).getN() == 16))
        {
            auto fixed_string_16 = [](char c)
            {
                return makeASTFunction("CAST",
                    makeASTFunction("unhex", make_intrusive<ASTLiteral>(String(32, c))),
                    make_intrusive<ASTLiteral>("FixedString(16)"));
            };
            return {{fixed_string_16('0'), fixed_string_16('f')}};
        }

        return {};
    }

    /// Replaces references to the `metric_name` column with a string literal, in place.
    void substituteMetricNameInPlace(ASTPtr & node, const String & metric_name_value)
    {
        if (const auto * identifier = node->as<ASTIdentifier>(); identifier && (identifier->name() == TimeSeriesColumnNames::MetricName))
        {
            node = make_intrusive<ASTLiteral>(metric_name_value);
            return;
        }
        for (auto & child : node->children)
            substituteMetricNameInPlace(child, metric_name_value);
    }

    /// Checks whether the selector can carry a primary-key range on the samples table's `id`
    /// column covering the whole metric, and makes the two range conditions if it can.
    ///
    /// With the canonical id generator for a two-component id type `Tuple(F, S)` (see
    /// `TimeSeriesIDGenerator::getDefault`) the first id component is a hash of the metric name
    /// alone, so all series of one metric occupy one continuous range of the samples table's
    /// primary key: tuple(hash(metric_name), min_S) <= id <= tuple(hash(metric_name), max_S).
    /// When additionally the selector's matchers select ALL (time-eligible) series of the metric
    /// - the dominant shape of dashboard and recording-rule queries - the large `id IN <set>`
    /// condition adds nothing to primary-key index analysis over that range, while costing a
    /// generic exclusion search over the set (hundreds of milliseconds per part per query for
    /// tens of thousands of series, single-threaded). The range conditions returned here select
    /// the same granules through the cheap continuous-range path.
    ///
    /// The decision is advisory with respect to correctness: the returned range is a SUPERSET of
    /// the resolved id set (verified over the existing series by the probe below and guaranteed
    /// for future inserts by the id generator), and the `id IN <set>` condition is kept in the
    /// WHERE for exact row-level filtering. Any rows a hash collision could add to the range are
    /// still rejected row-by-row.
    ///
    /// Returns an empty list (= emit today's SQL) unless ALL of the following hold:
    /// 1. The matchers contain exactly one EQ matcher on `__name__` with a non-empty value.
    /// 2. The id type is a two-component tuple of types supported by `TimeSeriesIDGenerator`,
    ///    and the id generator used by the table is the canonical one for that type (a custom
    ///    generator gives no metric clustering).
    /// 3. The samples table physically stores `id` with exactly this type.
    /// 4. A probe query on the tags table finds NO time-eligible series of the metric that either
    ///    fails the remaining matchers (the matcher does not select the whole metric) or has an
    ///    id outside the range (rows written before an `ALTER ... MODIFY SETTING id_generator`).
    ASTs tryMakeWholeMetricIDRangeConditions(
        const PrometheusQueryTree::MatcherList & matchers,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        const StorageID & data_table_id,
        const ColumnsDescription & data_table_columns,
        const StorageID & tags_table_id,
        const ColumnsDescription & tags_table_columns,
        const TimeSeriesSettings & time_series_settings,
        const DataTypePtr & id_data_type,
        const DataTypePtr & timestamp_data_type,
        const std::optional<DateTime64> & min_time_to_filter_ids,
        const std::optional<DateTime64> & max_time_to_filter_ids,
        const ContextPtr & context,
        const LoggerPtr & log)
    {
        /// 1. Exactly one EQ matcher on `__name__`, remember the rest for the probe.
        const PrometheusQueryTree::Matcher * name_matcher = nullptr;
        std::vector<const PrometheusQueryTree::Matcher *> other_matchers;
        for (const auto & matcher : matchers)
        {
            if ((matcher.matcher_type == PrometheusQueryTree::MatcherType::EQ) && (matcher.label_name == TimeSeriesTagNames::MetricName))
            {
                if (name_matcher)
                    return {};
                name_matcher = &matcher;
            }
            else
                other_matchers.push_back(&matcher);
        }
        if (!name_matcher || name_matcher->label_value.empty())
            return {};
        const String & metric_name = name_matcher->label_value;

        /// 2a. The id is a two-component tuple of supported types.
        const auto * id_tuple_type = typeid_cast<const DataTypeTuple *>(id_data_type.get());
        if (!id_tuple_type || (id_tuple_type->getElements().size() != 2))
            return {};
        if (!makeMinMaxLiteralsForIDComponent(*id_tuple_type->getElements()[0]))
            return {};
        auto min_max_second_component = makeMinMaxLiteralsForIDComponent(*id_tuple_type->getElements()[1]);
        if (!min_max_second_component)
            return {};

        /// 3. The samples table stores `id` physically with exactly this type: the range conditions
        /// compare the raw column (bypassing the identity-cast alias of the SELECT list).
        auto data_table_id_column = data_table_columns.tryGetPhysical(TimeSeriesColumnNames::ID);
        if (!data_table_id_column || (data_table_id_column->type->getName() != id_data_type->getName()))
            return {};

        /// 2b. The id generator is the canonical one for this id type.
        ASTPtr canonical_generator = tryGetCanonicalIDGenerator(id_data_type, time_series_settings, tags_table_columns);
        if (!canonical_generator)
            return {};

        /// The first id component for this metric, e.g. sipHash64('my_metric'), as a constant
        /// expression: the canonical generator's first tuple element with `metric_name` replaced
        /// by the metric name literal.
        ASTPtr first_component = canonical_generator->as<ASTFunction &>().arguments->children.at(0)->clone();
        substituteMetricNameInPlace(first_component, metric_name);

        /// 4. The probe: find one time-eligible series of the metric that contradicts the range
        /// emission, i.e. fails the remaining matchers or does not hash into the range.
        ///
        ///     SELECT 1 FROM tags_table
        ///     WHERE <__name__ matcher and the same time conditions as the tags subquery>
        ///       AND (NOT (<other matchers>) OR tupleElement(id, 1) != <first_component>)
        ///     LIMIT 1
        ///
        /// One such series means the id set is not the whole metric's primary-key range: fall back.
        /// No such series means every series the tags subquery can select lies in the range. The
        /// probe result cannot be raced into incorrectness: series inserted after the probe get
        /// their ids from the current (canonical) generator, so they stay inside the range, and
        /// the `id IN <set>` condition keeps doing the exact row-level filtering either way.
        {
            ASTPtr counterexample = makeASTFunction(
                "notEquals",
                makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID), make_intrusive<ASTLiteral>(UInt64{1})),
                first_component->clone());

            if (!other_matchers.empty())
            {
                ASTs other_matcher_asts;
                for (const auto * matcher : other_matchers)
                    other_matcher_asts.push_back(matcherToAST(*matcher, column_name_by_tag_name));
                counterexample = makeASTFunction(
                    "or", makeASTFunction("not", makeASTForLogicalAnd(std::move(other_matcher_asts))), std::move(counterexample));
            }

            PrometheusQueryTree::MatcherList name_matcher_only{*name_matcher};
            ASTPtr probe_where = makeASTForLogicalAnd(
                {makeWhereFilterForTagsTable(name_matcher_only, column_name_by_tag_name, min_time_to_filter_ids, max_time_to_filter_ids, timestamp_data_type),
                 std::move(counterexample)});

            auto probe_select = make_intrusive<ASTSelectQuery>();
            {
                auto select_list_exp = make_intrusive<ASTExpressionList>();
                select_list_exp->children.push_back(make_intrusive<ASTLiteral>(UInt64{1}));
                probe_select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list_exp));

                auto tables = make_intrusive<ASTTablesInSelectQuery>();
                auto table = make_intrusive<ASTTablesInSelectQueryElement>();
                auto table_exp = make_intrusive<ASTTableExpression>();
                table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(tags_table_id);
                table_exp->children.emplace_back(table_exp->database_and_table_name);
                table->table_expression = table_exp;
                tables->children.push_back(table);
                probe_select->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

                probe_select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(probe_where));
                probe_select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, make_intrusive<ASTLiteral>(UInt64{1}));
            }

            auto probe_query = make_intrusive<ASTSelectWithUnionQuery>();
            probe_query->union_mode = SelectUnionMode::UNION_DEFAULT;
            auto list_of_selects = make_intrusive<ASTExpressionList>();
            list_of_selects->children.push_back(std::move(probe_select));
            probe_query->children.push_back(std::move(list_of_selects));
            probe_query->list_of_selects = probe_query->children.back();

            LOG_DEBUG(log, "Probing whether selector matches the whole metric {}: {}", quoteString(metric_name), probe_query->formatForLogging());

            try
            {
                InterpreterSelectQueryAnalyzer interpreter(probe_query, context, SelectQueryOptions{});
                auto io = interpreter.execute();
                PullingPipelineExecutor executor(io.pipeline);
                Block block;
                while (executor.pull(block))
                {
                    if (block.rows() > 0)
                        return {};
                }
            }
            catch (...)
            {
                /// The probe only chooses between two emissions with identical results; an error
                /// here must not fail a query that works without this optimization (and an error
                /// the main query would also hit, e.g. a missing access right on the tags table,
                /// still surfaces when the main query runs the tags subquery).
                LOG_DEBUG(log, "Keeping the id set condition for index analysis: the whole-metric probe failed with {}", getCurrentExceptionMessage(false));
                return {};
            }
        }

        /// The range conditions on the raw samples-table `id` column, qualified so that they
        /// resolve to the table column and not to the same-named alias of the SELECT list.
        ASTs conditions;
        conditions.push_back(makeASTFunction(
            "greaterOrEquals",
            makeQualifiedIDIdentifier(data_table_id),
            makeASTFunction("tuple", first_component->clone(), std::move(min_max_second_component->first))));
        conditions.push_back(makeASTFunction(
            "lessOrEquals",
            makeQualifiedIDIdentifier(data_table_id),
            makeASTFunction("tuple", std::move(first_component), std::move(min_max_second_component->second))));
        return conditions;
    }
}


void StorageTimeSeriesSelector::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(config.time_series_storage_id, context));
    auto time_series_settings = time_series_storage->getStorageSettings();

    const auto & matchers = typeid_cast<const PrometheusQueryTree::InstantSelector &>(*config.selector.getRoot()).matchers;

    auto samples_table_id = time_series_storage->getTargetTableID(ViewTarget::Samples, context);
    auto tags_table_id = time_series_storage->getTargetTableID(ViewTarget::Tags, context);

    auto column_name_by_tag_name = makeColumnNameByTagNameMap(*time_series_settings);

    std::optional<DateTime64> min_time_to_filter_ids;
    std::optional<DateTime64> max_time_to_filter_ids;
    if ((*time_series_settings)[TimeSeriesSetting::filter_by_min_time_and_max_time]
        && (*time_series_settings)[TimeSeriesSetting::store_min_time_and_max_time])
    {
        min_time_to_filter_ids = config.min_time;
        max_time_to_filter_ids = config.max_time;
    }

    auto samples_table_metadata = time_series_storage->getTargetTable(ViewTarget::Samples, context)->getInMemoryMetadataPtr(context, false);
    auto tags_table_metadata = time_series_storage->getTargetTable(ViewTarget::Tags, context)->getInMemoryMetadataPtr(context, false);

    ASTs whole_metric_id_range_conditions = tryMakeWholeMetricIDRangeConditions(
        matchers,
        column_name_by_tag_name,
        samples_table_id,
        samples_table_metadata->getColumns(),
        tags_table_id,
        tags_table_metadata->getColumns(),
        *time_series_settings,
        config.table_id_data_type,
        config.timestamp_data_type,
        min_time_to_filter_ids,
        max_time_to_filter_ids,
        context,
        log);

    const bool narrow_id = config.narrow_id_to_second_component;
    const bool whole_metric = !whole_metric_id_range_conditions.empty();

    /// A narrowed id set (a set of second id components) cannot serve primary-key index analysis
    /// over the samples table: the sorting key there starts with the whole `id` tuple, and a set
    /// of second components alone does not constrain any prefix of it. So the narrowed set is
    /// used only when the whole-metric range conditions handle the index analysis instead (the
    /// set is also excluded from it below). Other narrowed selectors get a full-shaped set (see
    /// `TagsSubqueryIDForm::SecondComponentInFullShapedID`) compared with the raw `id` column,
    /// which keeps the index analysis exactly as without narrowing. (Filtering those selectors
    /// by a narrowed set as well would require a second, index-analysis-only set, and therefore
    /// a second scan of the tags table per selector - a fixed cost every cheap selector would pay.)
    const bool use_narrowed_id_set = narrow_id && whole_metric;

    TagsSubqueryIDForm tags_subquery_id_form = TagsSubqueryIDForm::Full;
    if (narrow_id)
        tags_subquery_id_form = use_narrowed_id_set ? TagsSubqueryIDForm::SecondComponent : TagsSubqueryIDForm::SecondComponentInFullShapedID;

    ASTPtr select_query_from_tags_table = makeSelectQueryFromTagsTable(
        tags_table_id, matchers, column_name_by_tag_name, min_time_to_filter_ids, max_time_to_filter_ids, config.timestamp_data_type,
        tags_subquery_id_form);

    /// The `id` entry of the samples-table SELECT list. With id narrowing it is
    /// `tupleElement(<qualified id>, 2) AS id`: the alias makes the bare `id` of the WHERE
    /// conditions and of the outer SELECT resolve to the narrowed component, while the qualified
    /// argument still resolves to the raw table column.
    ASTPtr id_select_expression;
    if (narrow_id)
    {
        id_select_expression = makeIDComponent(makeQualifiedIDIdentifier(samples_table_id), 2);
        id_select_expression->setAlias(TimeSeriesColumnNames::ID);
    }
    else
        id_select_expression = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID);

    /// The left argument of the `IN <ids>` condition: the bare `id` (which binds to the narrowed
    /// alias above when the id is narrowed), or the qualified raw column when the set is
    /// full-shaped.
    ASTPtr id_in_left_arg;
    if (tags_subquery_id_form == TagsSubqueryIDForm::SecondComponentInFullShapedID)
        id_in_left_arg = makeQualifiedIDIdentifier(samples_table_id);
    else
        id_in_left_arg = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID);

    if (narrow_id)
        LOG_DEBUG(log, "Reading only the second id component for selector {}", quoteString(config.selector.toString()));

    if (use_narrowed_id_set)
    {
        /// The narrowed id set filters the rows exactly (the second component alone identifies a
        /// time series), so the range conditions are pure index-analysis conditions here: wrapping
        /// them in `indexHint` removes them from the runtime filter, and the first id component
        /// is then not read at all (the narrowed conditions read only the second-component
        /// subcolumn).
        for (auto & condition : whole_metric_id_range_conditions)
            condition = makeASTFunction("indexHint", std::move(condition));
    }

    ContextPtr interpreter_context = context;
    if (whole_metric)
    {
        /// The `id IN <tags subquery>` condition stays in the WHERE for exact row-level filtering
        /// (and its subquery keeps collecting the tags of the matched series), but its set must
        /// not enter primary-key index analysis: `KeyCondition` runs a generic exclusion search
        /// with the whole set, which costs hundreds of milliseconds per part for tens of
        /// thousands of series, single-threaded, while the whole-metric range conditions select
        /// the same granules through the cheap continuous-range path. Setting
        /// `use_index_for_in_with_subqueries_max_values = 1` makes the set unusable for index
        /// analysis without affecting the row-level filter.
        auto modified_context = Context::createCopy(context);
        modified_context->setSetting("use_index_for_in_with_subqueries_max_values", UInt64{1});
        interpreter_context = modified_context;
        LOG_DEBUG(log, "Selector {} matches the whole metric: adding a primary-key range on id and excluding the id set from index analysis",
                  quoteString(config.selector.toString()));
    }
    else if (narrow_id)
    {
        /// The full-shaped id set filters the raw `id` column, which is therefore read anyway:
        /// keep `tupleElement(id, 2)` of the SELECT list evaluated on the read column (a zero-copy
        /// extraction) instead of being rewritten to a subcolumn, which would read the
        /// second-component streams from disk twice.
        auto modified_context = Context::createCopy(context);
        modified_context->setSetting("optimize_functions_to_subcolumns", false);
        interpreter_context = modified_context;
    }

    ASTPtr select_query_from_data_table = makeSelectQueryFromDataTable(
        samples_table_id,
        select_query_from_tags_table,
        config.min_time,
        config.max_time,
        config.timestamp_data_type,
        std::move(id_select_expression),
        std::move(id_in_left_arg),
        std::move(whole_metric_id_range_conditions));

    ASTPtr select_query = makeSelectQuery(
        std::move(select_query_from_data_table),
        config.id_data_type,
        config.timestamp_data_type,
        config.scalar_data_type);

    LOG_DEBUG(log, "Building SQL for selector: {}", config.selector.toString());
    LOG_DEBUG(log, "Will execute query:\n{}", select_query->formatForLogging());

    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);

    InterpreterSelectQueryAnalyzer interpreter(select_query, interpreter_context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    query_plan = std::move(interpreter).extractQueryPlan();
}

}
