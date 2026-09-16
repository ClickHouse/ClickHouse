#include <Storages/StorageTimeSeriesSelector.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Core/DecimalFunctions.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Core/ConstantValue.h>
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
#include <Storages/TimeSeries/checkTimeSeriesVersion.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>

#include <fmt/format.h>

#include <base/insertAtEnd.h>

#include <algorithm>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool filter_by_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsUInt64 recent_samples_ttl_seconds;
}

namespace Setting
{
    extern const SettingsBool time_series_prefer_recent_samples_table;
}

namespace
{

/// Read a required String literal argument as a value, without materializing a `Field`.
String getStringConstArgument(const ASTPtr & arg, const ContextPtr & context, std::string_view arg_name)
{
    const auto value = evaluateConstantExpressionAsColumn(arg, context);
    /// Accept `Nullable`/`LowCardinality` wrappers: the previous `Field`-based code read the value
    /// via `operator[]`, which flattens wrappers, so a non-NULL `Nullable(String)`/
    /// `LowCardinality(String)` constant passed the String check. Preserve that, and still reject a
    /// NULL value as before.
    if (!isStringOrFixedString(removeLowCardinalityAndNullable(value.getType())))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument '{}' must be a literal with type String, got {}", arg_name, value.getType()->getName());
    if (value.isNull())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument '{}' must be a literal with type String, got NULL", arg_name);
    return String(value.getDataAt());
}

/// The time range has at least millisecond precision, so fractional arguments `min_time` and `max_time` are not truncated
/// to whole seconds when the timestamps in the table have a coarser scale.
constexpr UInt32 MIN_TIME_SCALE = 3;

/// Returns the scale of `min_time` and `max_time` (see `Configuration::time_scale`).
UInt32 getTimeScale(UInt32 table_timestamp_scale, const DataTypePtr & min_time_type, const DataTypePtr & max_time_type)
{
    UInt32 time_scale = std::max(table_timestamp_scale, MIN_TIME_SCALE);
    for (const auto & type : {min_time_type, max_time_type})
        time_scale = std::max(time_scale, tryGetDecimalScale(*removeNullable(type)).value_or(0));
    return time_scale;
}

/// Converts a timestamp from `timestamp_scale` to `target_scale` (which is not greater) rounding it up or down.
/// The bounds of a time range are rounded towards the inside of the range, so the converted range contains the same samples.
DateTime64 convertToScale(DateTime64 timestamp, UInt32 timestamp_scale, UInt32 target_scale, bool round_up)
{
    chassert(timestamp_scale >= target_scale);
    if (timestamp_scale == target_scale)
        return timestamp;

    const Int64 divisor = DecimalUtils::scaleMultiplier<Int64>(timestamp_scale - target_scale);
    Int64 quotient = timestamp.value / divisor;
    const Int64 remainder = timestamp.value % divisor;
    if (round_up && (remainder > 0))
        ++quotient;
    else if (!round_up && (remainder < 0))
        --quotient;
    return DateTime64{quotient};
}

/// Whether a sample with the specified timestamp is kept in the recent samples table. A safety margin covers the asynchrony of
/// the TTL and its whole-second precision. Compares in seconds to avoid an overflow for big scales.
bool isWithinRecentSamplesTTL(UInt64 recent_samples_ttl_seconds, Int64 now_seconds, DateTime64 timestamp, UInt32 timestamp_scale)
{
    static constexpr Int64 safety_margin_seconds = 60;
    const Int64 timestamp_seconds = convertToScale(timestamp, timestamp_scale, /* target_scale = */ 0, /* round_up = */ false).value;
    return timestamp_seconds >= now_seconds - static_cast<Int64>(recent_samples_ttl_seconds) + safety_margin_seconds;
}

/// The smallest and the largest timestamps representable in the timestamp type of the table:
/// `DateTime` and `UInt32` can't hold timestamps before 1970 or after 2106.
DateTime64 minRepresentableTime(const DataTypePtr & table_timestamp_type)
{
    if (isDateTime64(table_timestamp_type))
        return DateTime64{std::numeric_limits<Int64>::min()};
    chassert(isDateTime(table_timestamp_type) || isUInt32(table_timestamp_type));
    return DateTime64{0};
}

DateTime64 maxRepresentableTime(const DataTypePtr & table_timestamp_type)
{
    if (isDateTime64(table_timestamp_type))
        return DateTime64{std::numeric_limits<Int64>::max()};
    chassert(isDateTime(table_timestamp_type) || isUInt32(table_timestamp_type));
    return DateTime64{std::numeric_limits<UInt32>::max()};
}

/// A closed time range with the type of the timestamps in the table, see `makeTableTimeRange`.
/// An absent bound is unlimited: the range contains all timestamps if both bounds are absent,
/// and no timestamps if `min_time > max_time`.
struct TableTimeRange
{
    std::optional<DateTime64> min_time;
    std::optional<DateTime64> max_time;

    /// Every timestamp of the table is in the range.
    bool containsAllTimestamps() const { return !min_time && !max_time; }

    /// No timestamp of the table is in the range.
    bool containsNoTimestamps() const { return min_time && max_time && (*min_time > *max_time); }

    static const TableTimeRange ALL_TIMESTAMPS;
    static const TableTimeRange NO_TIMESTAMPS;
};

constexpr TableTimeRange TableTimeRange::ALL_TIMESTAMPS{};
constexpr TableTimeRange TableTimeRange::NO_TIMESTAMPS{DateTime64{1}, DateTime64{0}};

/// Converts a closed time range with the scale `time_scale` to the type of the timestamps in the table: the bounds are rounded
/// inwards and clipped to the range of the type, which keeps the same samples inside.
/// The result contains no timestamps if no timestamp of the table is in the requested range.
TableTimeRange makeTableTimeRange(
    const DataTypePtr & table_timestamp_type, const std::optional<DateTime64> & min_time, const std::optional<DateTime64> & max_time, UInt32 time_scale)
{
    const UInt32 table_timestamp_scale = tryGetDecimalScale(*table_timestamp_type).value_or(0);
    const DateTime64 min_representable_time = minRepresentableTime(table_timestamp_type);
    const DateTime64 max_representable_time = maxRepresentableTime(table_timestamp_type);

    TableTimeRange range;
    if (min_time)
    {
        const DateTime64 table_min_time = convertToScale(*min_time, time_scale, table_timestamp_scale, /* round_up = */ true);
        if (table_min_time > max_representable_time)
            return TableTimeRange::NO_TIMESTAMPS;
        range.min_time = std::max(table_min_time, min_representable_time);
    }
    if (max_time)
    {
        const DateTime64 table_max_time = convertToScale(*max_time, time_scale, table_timestamp_scale, /* round_up = */ false);
        if (table_max_time < min_representable_time)
            return TableTimeRange::NO_TIMESTAMPS;
        range.max_time = std::min(table_max_time, max_representable_time);
    }
    return range;
}

/// Returns the time range to filter the identifiers of time series by the stored time ranges of the series (`min_time`, `max_time`):
/// `table_time_range` if the table stores them in the "time ranges" table (`time_ranges_table_id` is set), or in the "tags" table
/// (tables of earlier versions) with the `filter_by_min_time_and_max_time` setting enabled. Otherwise the range with all timestamps:
/// the identifiers aren't filtered by time, only the samples are filtered by their timestamps.
/// A `table_time_range` without timestamps is returned as is: no series can have samples in it, whatever the table stores.
TableTimeRange getTimeRangeToFilterIDs(
    const TableTimeRange & table_time_range, const StorageID & time_ranges_table_id, const TimeSeriesSettings & settings)
{
    if (table_time_range.containsNoTimestamps() || time_ranges_table_id)
        return table_time_range;

    if (settings.hasMinTimeAndMaxTimeInTagsTable() && settings[TimeSeriesSetting::filter_by_min_time_and_max_time])
        return table_time_range;

    return TableTimeRange::ALL_TIMESTAMPS;
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
    checkTimeSeriesVersionSupportedByPromQL(*time_series_storage);
    auto time_series_metadata = time_series_storage->getInMemoryMetadataPtr(context, false);
    auto [table_timestamp_type, table_value_type] = splitTimeSeriesType(
        time_series_metadata->columns.get(TimeSeriesColumnNames::getOuterSamples(time_series_storage->getVersion())).type);
    auto tags_target = time_series_storage->getTargetTable(ViewTarget::Tags, context);
    auto tags_target_metadata = tags_target->getInMemoryMetadataPtr(context, false);
    DataTypePtr table_id_type = tags_target_metadata->columns.get(TimeSeriesColumnNames::ID).type;

    PrometheusQueryTree selector{getStringConstArgument(args[argument_index++], context, "selector")};

    auto [min_time_field, min_time_type] = evaluateConstantExpression(args[argument_index++], context);
    auto [max_time_field, max_time_type] = evaluateConstantExpression(args[argument_index++], context);

    UInt32 table_timestamp_scale = tryGetDecimalScale(*table_timestamp_type).value_or(0);
    UInt32 parameters_scale = getTimeScale(table_timestamp_scale, min_time_type, max_time_type);
    auto min_time = parseTimeSeriesTimestamp(min_time_field, min_time_type, parameters_scale);
    auto max_time = parseTimeSeriesTimestamp(max_time_field, max_time_type, parameters_scale);

    chassert(argument_index == args.size());

    Configuration config;
    config.time_series_storage_id = std::move(time_series_storage_id);
    config.table_id_type = std::move(table_id_type);
    config.table_timestamp_type = std::move(table_timestamp_type);
    config.table_value_type = std::move(table_value_type);
    config.selector = std::move(selector);
    config.time_scale = parameters_scale;
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
                        Field{config.max_time}, Field{config.min_time});

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

    /// Makes the conditions checking that the stored time range [min_time, max_time] of a time series
    /// intersects the requested one: `max_time >= <requested min_time> AND min_time <= <requested max_time>`.
    /// The conditions hold for the rows of an unmerged aggregating table too: a time series has a sample in the
    /// requested range only if the row written with that sample intersects the range.
    ASTs makeTimeRangeConditions(
        const TableTimeRange & time_range_to_filter_ids,
        const DataTypePtr & table_timestamp_type)
    {
        ASTs conditions;

        if (time_range_to_filter_ids.min_time)
        {
            conditions.push_back(makeASTFunction(
                "greaterOrEquals",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MaxTime),
                timeSeriesTimestampToAST(*time_range_to_filter_ids.min_time, table_timestamp_type)));
        }

        if (time_range_to_filter_ids.max_time)
        {
            conditions.push_back(makeASTFunction(
                "lessOrEquals",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MinTime),
                timeSeriesTimestampToAST(*time_range_to_filter_ids.max_time, table_timestamp_type)));
        }

        return conditions;
    }

    /// Wraps a select query into ASTSelectWithUnionQuery.
    ASTPtr wrapIntoSelectWithUnionQuery(ASTPtr select_query)
    {
        auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
        select_with_union_query->union_mode = SelectUnionMode::UNION_DEFAULT;
        auto list_of_selects = make_intrusive<ASTExpressionList>();
        list_of_selects->children.push_back(std::move(select_query));
        select_with_union_query->children.push_back(std::move(list_of_selects));
        select_with_union_query->list_of_selects = select_with_union_query->children.back();
        return select_with_union_query;
    }

    /// Makes the FROM clause of a select query reading a table (`table` is an ASTTableIdentifier) or a subquery (`table` is an ASTSubquery).
    ASTPtr makeTablesInSelectQuery(ASTPtr table)
    {
        auto table_exp = make_intrusive<ASTTableExpression>();
        if (table->as<ASTSubquery>())
            table_exp->subquery = table;
        else
            table_exp->database_and_table_name = table;
        table_exp->children.push_back(std::move(table));

        auto element = make_intrusive<ASTTablesInSelectQueryElement>();
        element->table_expression = table_exp;
        element->children.push_back(element->table_expression);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(element);
        return tables;
    }

    /// Wraps a query selecting identifiers of time series into a query keeping only the identifiers whose stored
    /// time range intersects the requested one:
    ///
    ///     SELECT id FROM time_ranges_table
    ///     WHERE max_time >= <min_time> AND min_time <= <max_time> AND id IN (<select_ids_query>)
    ///
    /// The identifiers are selected from the tags table first: the matchers usually select a small part of all
    /// the time series, so the time ranges table is read by its primary key (`id`) instead of being scanned.
    ASTPtr makeSelectIDsFilteredByTimeRangesTable(
        ASTPtr select_ids_query,
        const StorageID & time_ranges_table_id,
        ASTs time_range_conditions)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        /// SELECT id
        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            select_list_exp->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
            select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
        }

        /// FROM time_ranges_table
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, makeTablesInSelectQuery(make_intrusive<ASTTableIdentifier>(time_ranges_table_id)));

        /// WHERE max_time >= <min_time> AND min_time <= <max_time> AND id IN (<select_ids_query>)
        /// The cheap comparisons go before the probe of the `id IN` set, the same way as in makeWhereFilterForSamplesTable.
        {
            ASTs conditions = std::move(time_range_conditions);
            conditions.push_back(makeASTFunction("in", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID), make_intrusive<ASTSubquery>(std::move(select_ids_query))));
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, makeASTForLogicalAnd(std::move(conditions)));
        }

        return wrapIntoSelectWithUnionQuery(std::move(select_query));
    }

    /// Applies the time range filter to a query selecting identifiers from the tags table:
    /// if the time ranges are stored in the time ranges table, the query is wrapped (see makeSelectIDsFilteredByTimeRangesTable);
    /// if the tags table stores them itself (tables of earlier versions), the conditions are already in the WHERE clause
    /// of the query (see makeWhereFilterForTagsTable), so the query is returned as is.
    ASTPtr applyTimeRangeFilterToSelectIDsQuery(
        ASTPtr select_ids_query,
        const StorageID & time_ranges_table_id,
        const TableTimeRange & time_range_to_filter_ids,
        const DataTypePtr & table_timestamp_type)
    {
        if (!time_ranges_table_id || time_range_to_filter_ids.containsAllTimestamps())
            return select_ids_query;

        return makeSelectIDsFilteredByTimeRangesTable(
            std::move(select_ids_query), time_ranges_table_id, makeTimeRangeConditions(time_range_to_filter_ids, table_timestamp_type));
    }

    /// Makes the WHERE clause of a query selecting identifiers from the tags table.
    /// The conditions on the time range are added only if the tags table stores it itself (tables of earlier versions),
    /// otherwise the time range filter is applied by applyTimeRangeFilterToSelectIDsQuery.
    /// The bounds of `time_range_to_filter_ids` must have the scale of `table_timestamp_type`.
    ASTPtr makeWhereFilterForTagsTable(
        const PrometheusQueryTree::MatcherList & matchers,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        const StorageID & time_ranges_table_id,
        const TableTimeRange & time_range_to_filter_ids,
        const DataTypePtr & table_timestamp_type)
    {
        ASTs asts;
        for (const auto & matcher : matchers)
            asts.push_back(matcherToAST(matcher, column_name_by_tag_name));

        if (asts.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Instant selector without matchers is not allowed");

        if (!time_ranges_table_id)
            insertAtEnd(asts, makeTimeRangeConditions(time_range_to_filter_ids, table_timestamp_type));

        return makeASTForLogicalAnd(std::move(asts));
    }

    /// Makes `SELECT id FROM null('id <type>')`: a query returning no ids, with the result type of `makeSelectQueryFromTagsTable`.
    ASTPtr makeSelectNoIDsQuery(const DataTypePtr & table_id_type)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        auto select_list_exp = make_intrusive<ASTExpressionList>();
        select_list_exp->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);

        auto table_exp = make_intrusive<ASTTableExpression>();
        table_exp->table_function = makeASTFunction("null", make_intrusive<ASTLiteral>(fmt::format("{} {}", TimeSeriesColumnNames::ID, table_id_type->getName())));
        table_exp->children.emplace_back(table_exp->table_function);
        auto table = make_intrusive<ASTTablesInSelectQueryElement>();
        table->table_expression = table_exp;
        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(table);
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        return wrapIntoSelectWithUnionQuery(select_query);
    }

    ASTPtr makeSelectQueryFromTagsTable(
        const StorageID & tags_table_id,
        const PrometheusQueryTree::MatcherList & matchers,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        const StorageID & time_ranges_table_id,
        const TableTimeRange & time_range_to_filter_ids,
        const DataTypePtr & table_timestamp_type)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        /// SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name, tag_name1, tag_column1, ...)
        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            auto & select_list = select_list_exp->children;

            ASTs args;
            args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
            args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));
            args.push_back(make_intrusive<ASTLiteral>(TimeSeriesTagNames::MetricName));
            args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

            for (const auto & [tag_name, column_name] : column_name_by_tag_name)
            {
                args.push_back(make_intrusive<ASTLiteral>(tag_name));
                args.push_back(make_intrusive<ASTIdentifier>(column_name));
            }

            select_list.push_back(makeASTFunction("timeSeriesStoreTags", std::move(args)));
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
            auto where_filter = makeWhereFilterForTagsTable(matchers, column_name_by_tag_name, time_ranges_table_id, time_range_to_filter_ids, table_timestamp_type);
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_filter));
        }

        /// If there is a time ranges table: `SELECT id FROM time_ranges_table WHERE <time range conditions> AND id IN (<this query>)`.
        return applyTimeRangeFilterToSelectIDsQuery(
            wrapIntoSelectWithUnionQuery(std::move(select_query)), time_ranges_table_id, time_range_to_filter_ids, table_timestamp_type);
    }

    /// The bounds of `time_range_to_filter_samples` must have the scale of `table_timestamp_type`.
    ASTPtr makeWhereFilterForSamplesTable(
        ASTPtr select_query_from_tags_table,
        const TableTimeRange & time_range_to_filter_samples,
        const DataTypePtr & table_timestamp_type,
        ASTPtr whole_metric_id_range_condition)
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
        if (time_range_to_filter_samples.min_time)
        {
            conditions.push_back(makeASTFunction(
                "greaterOrEquals",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                timeSeriesTimestampToAST(*time_range_to_filter_samples.min_time, table_timestamp_type)));
        }

        /// timestamp <= max_time
        if (time_range_to_filter_samples.max_time)
        {
            conditions.push_back(makeASTFunction(
                "lessOrEquals",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                timeSeriesTimestampToAST(*time_range_to_filter_samples.max_time, table_timestamp_type)));
        }

        /// id IN (SELECT id FROM (select_id_query))
        /// Wrap the SELECT in ASTSubquery so it formats with surrounding parentheses.
        auto select_as_subquery = make_intrusive<ASTSubquery>(std::move(select_query_from_tags_table));
        conditions.push_back(makeASTFunction("in", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID), std::move(select_as_subquery)));

        /// For a whole-metric selector over a metric-clustered id layout one more condition is
        /// added: indexHint(<raw id column> >= tuple(hash(metric_name), min) AND <raw id column>
        /// <= tuple(hash(metric_name), max)). `indexHint` keeps it out of the row-level filter, so
        /// its only purpose is to give the primary-key index analysis a continuous key range
        /// instead of the large set (see readImpl).
        if (whole_metric_id_range_condition)
            conditions.push_back(std::move(whole_metric_id_range_condition));

        return makeASTForLogicalAnd(std::move(conditions));
    }

    ASTPtr makeSelectQueryFromSamplesTable(const StorageID & samples_table_id,
                                           ASTPtr select_query_from_tags_table,
                                           const TableTimeRange & time_range_to_filter_samples,
                                           const DataTypePtr & table_timestamp_type,
                                           ASTPtr whole_metric_id_range_condition)
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
        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            auto & select_list = select_list_exp->children;

            select_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
            select_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
            select_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value));

            select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
        }

        /// FROM samples_table_id
        auto tables = make_intrusive<ASTTablesInSelectQuery>();

        {
            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(samples_table_id);
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table->table_expression = table_exp;
            tables->children.push_back(table);

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        /// WHERE (timestamp >= min_time) AND (timestamp <= max_time) AND (id IN <select_query_from_tags_table>)
        ///
        /// where <select_query_from_tags_table> is roughly:
        ///   SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name, ...) FROM tags_table WHERE <matchers>
        /// optionally wrapped into a query filtering the identifiers by the time ranges table
        /// (see makeSelectIDsFilteredByTimeRangesTable).
        {
            auto where_filter = makeWhereFilterForSamplesTable(
                select_query_from_tags_table, time_range_to_filter_samples, table_timestamp_type, std::move(whole_metric_id_range_condition));
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

    /// Makes the final select query by wrapping the select query from the samples table into an outer
    /// SELECT which casts the columns to the data types expected by this storage:
    ///
    /// SELECT _CAST(id, 'UInt64') AS id, _CAST(timestamp, 'DateTime64(3)') AS timestamp, _CAST(value, 'Float64') AS value
    /// FROM (select_query_from_samples_table)
    ///
    /// The inner query reads the samples table columns as is (see makeSelectQueryFromSamplesTable()),
    /// so its result types are the physical column types, which can differ from the expected ones
    /// (e.g. a samples table can store `timestamp` with a different timezone). Casting in an outer
    /// SELECT keeps the WHERE conditions of the inner query on the bare primary key columns, and
    /// the casts run only for the rows which passed the filter. The internal `_CAST` is used here
    /// because it returns exactly the specified type (`CAST` and conversion functions like
    /// `toDateTime64` keep the timezone of the casted expression), and it is free when the type
    /// already matches.
    ASTPtr makeSelectQuery(ASTPtr select_query_from_samples_table,
                           const DataTypePtr & table_id_type,
                           const DataTypePtr & table_timestamp_type,
                           const DataTypePtr & table_value_type)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        /// SELECT _CAST(id, 'UInt64') AS id, _CAST(timestamp, 'DateTime64(3)') AS timestamp, _CAST(value, 'Float64') AS value
        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            auto & select_list = select_list_exp->children;

            select_list.push_back(makeASTFunction(
                "_CAST", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID), make_intrusive<ASTLiteral>(table_id_type->getName())));
            select_list.back()->setAlias(TimeSeriesColumnNames::ID);

            select_list.push_back(makeASTFunction(
                "_CAST",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                make_intrusive<ASTLiteral>(table_timestamp_type->getName())));
            select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

            select_list.push_back(makeASTFunction(
                "_CAST", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value), make_intrusive<ASTLiteral>(table_value_type->getName())));
            select_list.back()->setAlias(TimeSeriesColumnNames::Value);

            select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
        }

        /// FROM (select_query_from_samples_table)
        {
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->subquery = make_intrusive<ASTSubquery>(std::move(select_query_from_samples_table));
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
    /// column covering the whole metric, and makes the range condition if it can:
    /// `indexHint(tuple(hash(metric_name), min_S) <= id AND id <= tuple(hash(metric_name), max_S))`.
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
    /// Returns nullptr (= emit today's SQL) unless ALL of the following hold:
    /// 1. The matchers contain exactly one EQ matcher on `__name__` with a non-empty value.
    /// 2. The id type is a two-component tuple of types supported by `TimeSeriesIDGenerator`,
    ///    and the id generator used by the table is the canonical one for that type (a custom
    ///    generator gives no metric clustering).
    /// 3. The samples table physically stores `id` with exactly this type.
    /// 4. A probe query on the tags table (and the time ranges table if any) finds NO time-eligible series
    ///    of the metric that either fails the remaining matchers (the matcher does not select the whole metric)
    ///    or has an id outside the range (rows written before an `ALTER ... MODIFY SETTING id_generator`).
    ASTPtr tryMakeWholeMetricIDRangeConditions(
        const PrometheusQueryTree::MatcherList & matchers,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        const StorageID & samples_table_id,
        const ColumnsDescription & samples_table_columns,
        const StorageID & tags_table_id,
        const ColumnsDescription & tags_table_columns,
        const StorageID & time_ranges_table_id,
        const TimeSeriesSettings & time_series_settings,
        const StorageID & time_series_storage_id,
        const DataTypePtr & table_id_type,
        const DataTypePtr & table_timestamp_type,
        const TableTimeRange & time_range_to_filter_ids,
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
        const auto * id_tuple_type = typeid_cast<const DataTypeTuple *>(table_id_type.get());
        if (!id_tuple_type || (id_tuple_type->getElements().size() != 2))
            return {};
        if (!makeMinMaxLiteralsForIDComponent(*id_tuple_type->getElements()[0]))
            return {};
        auto min_max_second_component = makeMinMaxLiteralsForIDComponent(*id_tuple_type->getElements()[1]);
        if (!min_max_second_component)
            return {};

        /// 3. The samples table stores `id` physically with exactly this type: the range conditions
        /// compare the raw column (bypassing the identity-cast alias of the SELECT list).
        auto samples_table_id_column = samples_table_columns.tryGetPhysical(TimeSeriesColumnNames::ID);
        if (!samples_table_id_column || (samples_table_id_column->type->getName() != table_id_type->getName()))
            return {};

        /// 2b. The id generator is the canonical one for this id type. The resolution order mirrors
        /// `TimeSeriesSink`: the `id_generator` setting, then the DEFAULT of the tags-table `id`
        /// column, then the canonical generator.
        /// `getDefault` cannot throw here: two-component tuples of the types accepted above are
        /// exactly the tuple types it supports.
        ASTPtr canonical_generator = TimeSeriesIDGenerator::getDefault(table_id_type, time_series_storage_id);
        ASTPtr id_generator = time_series_settings[TimeSeriesSetting::id_generator].value;
        if (!id_generator)
        {
            if (const auto * tags_id_column = tags_table_columns.tryGet(TimeSeriesColumnNames::ID))
                id_generator = tags_id_column->default_desc.expression;
        }
        if (id_generator && (id_generator->getTreeHash(/*ignore_aliases=*/true) != canonical_generator->getTreeHash(/*ignore_aliases=*/true)))
            return {};

        /// The first id component for this metric, e.g. sipHash64('my_metric'), as a constant
        /// expression: the canonical generator's first tuple element with `metric_name` replaced
        /// by the metric name literal.
        ASTPtr first_component = canonical_generator->as<ASTFunction &>().arguments->children.at(0)->clone();
        substituteMetricNameInPlace(first_component, metric_name);

        /// 4. The probe: find one time-eligible series of the metric that contradicts the range
        /// emission, i.e. fails the remaining matchers or does not hash into the range.
        ///
        ///     SELECT 1 FROM (
        ///         SELECT id FROM tags_table
        ///         WHERE <__name__ matcher and the same time conditions as the tags subquery>
        ///           AND (NOT (<other matchers>) OR tupleElement(id, 1) != <first_component>))
        ///     LIMIT 1
        ///
        /// The time conditions are applied the same way as in the tags subquery: in the WHERE clause above if
        /// the tags table stores the time ranges, or by the time ranges table wrapping the inner query otherwise
        /// (see applyTimeRangeFilterToSelectIDsQuery).
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
                {makeWhereFilterForTagsTable(name_matcher_only, column_name_by_tag_name, time_ranges_table_id, time_range_to_filter_ids, table_timestamp_type),
                 std::move(counterexample)});

            /// SELECT id FROM tags_table WHERE <probe_where>
            auto probe_select_ids = make_intrusive<ASTSelectQuery>();
            {
                auto select_list_exp = make_intrusive<ASTExpressionList>();
                select_list_exp->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
                probe_select_ids->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list_exp));
                probe_select_ids->setExpression(ASTSelectQuery::Expression::TABLES, makeTablesInSelectQuery(make_intrusive<ASTTableIdentifier>(tags_table_id)));
                probe_select_ids->setExpression(ASTSelectQuery::Expression::WHERE, std::move(probe_where));
            }
            ASTPtr probe_select_ids_query = applyTimeRangeFilterToSelectIDsQuery(
                wrapIntoSelectWithUnionQuery(std::move(probe_select_ids)), time_ranges_table_id, time_range_to_filter_ids, table_timestamp_type);

            /// SELECT 1 FROM (<probe_select_ids_query>) LIMIT 1
            auto probe_select = make_intrusive<ASTSelectQuery>();
            {
                auto select_list_exp = make_intrusive<ASTExpressionList>();
                select_list_exp->children.push_back(make_intrusive<ASTLiteral>(UInt64{1}));
                probe_select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list_exp));
                probe_select->setExpression(ASTSelectQuery::Expression::TABLES, makeTablesInSelectQuery(make_intrusive<ASTSubquery>(std::move(probe_select_ids_query))));
                probe_select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, make_intrusive<ASTLiteral>(UInt64{1}));
            }

            ASTPtr probe_query = wrapIntoSelectWithUnionQuery(std::move(probe_select));

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

        /// The range conditions on the raw `id` column of the samples table, qualified so that they
        /// resolve to the table column and not to the same-named alias of the SELECT list.
        /// Wrapped in `indexHint` so the range reaches index analysis but is not evaluated per row:
        /// the `id IN <set>` condition the caller keeps is the exact filter, and on this path every
        /// id of that set is inside the range (that is what the probe establishes).
        auto make_qualified_id = [&]
        {
            return make_intrusive<ASTIdentifier>(std::vector<String>{samples_table_id.database_name, samples_table_id.table_name, TimeSeriesColumnNames::ID});
        };

        ASTs range_conditions;
        range_conditions.push_back(makeASTFunction(
            "greaterOrEquals",
            make_qualified_id(),
            makeASTFunction("tuple", first_component->clone(), min_max_second_component->first->clone())));
        range_conditions.push_back(makeASTFunction(
            "lessOrEquals",
            make_qualified_id(),
            makeASTFunction("tuple", first_component->clone(), min_max_second_component->second->clone())));
        return makeASTFunction("indexHint", makeASTForLogicalAnd(std::move(range_conditions)));
    }
}


ASTPtr StorageTimeSeriesSelector::makeSelectIDsQuery(
    const StorageTimeSeries & time_series_storage,
    const StorageID & tags_table_id,
    const TimeSeriesSettings & time_series_settings,
    const DataTypePtr & table_timestamp_type,
    const DataTypePtr & table_id_type,
    const PrometheusQueryTree::MatcherList & matchers,
    const std::optional<DateTime64> & min_time,
    const std::optional<DateTime64> & max_time,
    UInt32 time_scale,
    const ContextPtr & context)
{
    /// If the time range contains no timestamp of the table, no series can have samples in it: the tags table isn't read.
    const StorageID time_ranges_table_id = time_series_storage.hasTarget(ViewTarget::TimeRanges)
        ? time_series_storage.getTargetTableID(ViewTarget::TimeRanges, context)
        : StorageID::createEmpty();
    const auto time_range_to_filter_ids = getTimeRangeToFilterIDs(
        makeTableTimeRange(table_timestamp_type, min_time, max_time, time_scale), time_ranges_table_id, time_series_settings);
    auto select_query = time_range_to_filter_ids.containsNoTimestamps()
        ? makeSelectNoIDsQuery(table_id_type)
        : makeSelectQueryFromTagsTable(
            tags_table_id, matchers, makeColumnNameByTagNameMap(time_series_settings),
            time_ranges_table_id, time_range_to_filter_ids, table_timestamp_type);

    /// Alias the returned expression (`timeSeriesStoreTags(...)`, which returns `id`) so callers can reference the column by a fixed name.
    const auto & select_with_union = typeid_cast<const ASTSelectWithUnionQuery &>(*select_query);
    auto & select = typeid_cast<ASTSelectQuery &>(*select_with_union.list_of_selects->children.at(0));
    select.select()->children.at(0)->setAlias("series_id");

    return select_query;
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
    checkTimeSeriesVersionSupportedByPromQL(*time_series_storage);
    auto time_series_settings = time_series_storage->getStorageSettings();

    const auto & matchers = typeid_cast<const PrometheusQueryTree::InstantSelector &>(*config.selector.getRoot()).matchers;

    /// Prefer the recent samples table when the whole range fits in its TTL window: it's a much smaller copy of the recent samples.
    auto samples_table_kind = ViewTarget::Samples;
    const auto recent_samples_ttl_seconds = (*time_series_settings)[TimeSeriesSetting::recent_samples_ttl_seconds].value;
    if (recent_samples_ttl_seconds && context->getSettingsRef()[Setting::time_series_prefer_recent_samples_table])
    {
        /// If the start of the range is within the TTL, the whole range is: the recent samples table keeps everything after that.
        if (isWithinRecentSamplesTTL(recent_samples_ttl_seconds, std::time(nullptr), config.min_time, config.time_scale)
            && time_series_storage->tryGetTargetTable(ViewTarget::RecentSamples, context))
        {
            samples_table_kind = ViewTarget::RecentSamples;
            LOG_DEBUG(log, "Selector {} time range [{}, {}] fits in the recent samples TTL window: reading from the recent samples table",
                      quoteString(config.selector.toString()), config.min_time.value, config.max_time.value);
        }
    }

    auto samples_table_id = time_series_storage->getTargetTableID(samples_table_kind, context);
    auto tags_table_id = time_series_storage->getTargetTableID(ViewTarget::Tags, context);

    auto column_name_by_tag_name = makeColumnNameByTagNameMap(*time_series_settings);

    /// The samples are compared with the bounds at the scale of the table, so the index of the samples table is used as is.
    const auto table_time_range = makeTableTimeRange(config.table_timestamp_type, config.min_time, config.max_time, config.time_scale);
    if (table_time_range.containsNoTimestamps())
    {
        /// No timestamp of the table is in the time range. Nothing is read - an uninitialized query plan reads from an empty source.
        LOG_DEBUG(log, "Selector {} time range [{}, {}] contains no timestamp of the table: returning no samples",
                  quoteString(config.selector.toString()), config.min_time.value, config.max_time.value);
        return;
    }
    const TableTimeRange time_range_to_filter_samples = table_time_range;

    const StorageID time_ranges_table_id = time_series_storage->hasTarget(ViewTarget::TimeRanges)
        ? time_series_storage->getTargetTableID(ViewTarget::TimeRanges, context)
        : StorageID::createEmpty();
    const TableTimeRange time_range_to_filter_ids = getTimeRangeToFilterIDs(table_time_range, time_ranges_table_id, *time_series_settings);

    auto samples_table_metadata = time_series_storage->getTargetTable(samples_table_kind, context)->getInMemoryMetadataPtr(context, false);
    auto tags_table_metadata = time_series_storage->getTargetTable(ViewTarget::Tags, context)->getInMemoryMetadataPtr(context, false);

    ASTPtr whole_metric_id_range_condition = tryMakeWholeMetricIDRangeConditions(
        matchers,
        column_name_by_tag_name,
        samples_table_id,
        samples_table_metadata->getColumns(),
        tags_table_id,
        tags_table_metadata->getColumns(),
        time_ranges_table_id,
        *time_series_settings,
        config.time_series_storage_id,
        config.table_id_type,
        config.table_timestamp_type,
        time_range_to_filter_ids,
        context,
        log);

    /// A selector matching the whole metric doesn't need its ids filtered by time: the index analysis of the samples table
    /// uses the range condition instead of the id set, the id set is only checked per row, and the rows of a time series
    /// without samples in the time range are rejected by the timestamp conditions anyway.
    /// Filtering the ids by time ranges in that case would mean adding a redundant subquery reading the time ranges table.
    const TableTimeRange & time_range_to_filter_ids_in_tags_query
        = whole_metric_id_range_condition ? TableTimeRange::ALL_TIMESTAMPS : time_range_to_filter_ids;

    ASTPtr select_query_from_tags_table = makeSelectQueryFromTagsTable(
        tags_table_id, matchers, column_name_by_tag_name, time_ranges_table_id, time_range_to_filter_ids_in_tags_query, config.table_timestamp_type);

    auto modified_context = Context::createCopy(context);
    ContextPtr interpreter_context = modified_context;

    if (!context->getSettingsRef().isChanged("merge_tree_min_bytes_for_concurrent_read"))
        modified_context->setSetting("merge_tree_min_bytes_for_concurrent_read", UInt64{4 * 1024 * 1024});

    if (!context->getSettingsRef().isChanged("merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem"))
        modified_context->setSetting("merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem", UInt64{4 * 1024 * 1024});

    if (whole_metric_id_range_condition)
    {
        /// The `id IN <tags subquery>` condition stays in the WHERE for exact row-level filtering,
        /// but its set is excluded from the primary-key index analysis: `KeyCondition` would run an expensive
        /// exclusion search with the whole set, while the range conditions select the same granules cheaply.
        modified_context->setSetting("use_index_for_in_with_subqueries_max_values", UInt64{1});
        LOG_DEBUG(log, "Selector {} matches the whole metric: adding a primary-key range on id and excluding the id set from index analysis",
                  quoteString(config.selector.toString()));
    }

    ASTPtr select_query_from_samples_table = makeSelectQueryFromSamplesTable(
        samples_table_id,
        select_query_from_tags_table,
        time_range_to_filter_samples,
        config.table_timestamp_type,
        std::move(whole_metric_id_range_condition));

    ASTPtr select_query = makeSelectQuery(
        std::move(select_query_from_samples_table),
        config.table_id_type,
        config.table_timestamp_type,
        config.table_value_type);

    LOG_DEBUG(log, "Building SQL for selector: {}", config.selector.toString());
    LOG_DEBUG(log, "Will execute query:\n{}", select_query->formatForLogging());

    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);

    InterpreterSelectQueryAnalyzer interpreter(select_query, interpreter_context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    query_plan = std::move(interpreter).extractQueryPlan();
}

}
