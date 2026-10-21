#include <Storages/StorageTimeSeriesSelector.h>

#include <Columns/IColumn.h>
#include <Core/ConstantValue.h>
#include <Core/DecimalFunctions.h>
#include <Core/Joins.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesIDGenerator.h>
#include <Storages/TimeSeries/TimeSeriesIDRange.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/checkTimeSeriesVersion.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <Storages/TimeSeries/tryGetLiteralAlternatives.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <base/insertAtEnd.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/re2.h>

#include <fmt/format.h>

#include <algorithm>
#include <limits>
#include <unordered_set>


namespace DB
{

namespace FailPoints
{
extern const char time_series_selector_pause_after_target_metadata[];
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int UNFINISHED;
}

namespace TimeSeriesSetting
{
extern const TimeSeriesSettingsMap tags_to_columns;
extern const TimeSeriesSettingsBool filter_by_min_time_and_max_time;
extern const TimeSeriesSettingsASTFunction id_generator;
extern const TimeSeriesSettingsUInt64 recent_samples_bucket_step_seconds;
extern const TimeSeriesSettingsUInt64 recent_samples_ttl_seconds;
extern const TimeSeriesSettingsUInt64 samples_bucket_step_seconds;
}

namespace Setting
{
extern const SettingsUInt64 max_promql_native_rate_samples_per_series;
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
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Argument '{}' must be a literal with type String, got {}", arg_name, value.getType()->getName());
    if (value.isNull())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument '{}' must be a literal with type String, got NULL", arg_name);
    return String(value.getDataAt());
}

/// Keep at least millisecond precision for fractional selector bounds even when the table stores coarser timestamps.
constexpr UInt32 MIN_TIME_SCALE = 3;

UInt32 getTimeScale(UInt32 table_timestamp_scale, const DataTypePtr & min_time_type, const DataTypePtr & max_time_type)
{
    UInt32 time_scale = std::max(table_timestamp_scale, MIN_TIME_SCALE);
    for (const auto & type : {min_time_type, max_time_type})
        time_scale = std::max(time_scale, tryGetDecimalScale(*removeNullable(type)).value_or(0));
    return time_scale;
}

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

bool isWithinRecentSamplesTTL(UInt64 recent_samples_ttl_seconds, Int64 now_seconds, DateTime64 timestamp, UInt32 timestamp_scale)
{
    static constexpr Int64 safety_margin_seconds = 60;
    const Int64 timestamp_seconds = convertToScale(timestamp, timestamp_scale, 0, false).value;
    return timestamp_seconds >= now_seconds - static_cast<Int64>(recent_samples_ttl_seconds) + safety_margin_seconds;
}

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

ASTPtr makeOrderByIDAndBucket()
{
    auto order_by = make_intrusive<ASTExpressionList>();
    for (const auto * column_name : {TimeSeriesColumnNames::ID, TimeSeriesColumnNames::Bucket})
    {
        auto order_by_element = make_intrusive<ASTOrderByElement>();
        order_by_element->children.push_back(make_intrusive<ASTIdentifier>(column_name));
        order_by_element->direction = 1;
        order_by->children.push_back(std::move(order_by_element));
    }
    return order_by;
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
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires {}..{} arguments: {}({})",
            function_name,
            min_num_args,
            max_num_args,
            function_name,
            expected_args);
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
    config.time_series_version = time_series_storage->getVersion();
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Max time {} is less than min time {}", Field{config.max_time}, Field{config.min_time});

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

bool StorageTimeSeriesSelector::hasEmptyTimeRange() const
{
    return makeTableTimeRange(config.table_timestamp_type, config.min_time, config.max_time, config.time_scale).containsNoTimestamps();
}

std::optional<StorageTimeSeriesSelector::SampleTimeBounds> StorageTimeSeriesSelector::getSampleTimeBounds() const
{
    const auto range = makeTableTimeRange(config.table_timestamp_type, config.min_time, config.max_time, config.time_scale);
    if (range.containsNoTimestamps())
        return std::nullopt;

    chassert(range.min_time && range.max_time);
    if (isDateTime64(config.table_timestamp_type))
    {
        const auto scale = getDecimalScale(*config.table_timestamp_type);
        return SampleTimeBounds{DecimalField<DateTime64>(*range.min_time, scale), DecimalField<DateTime64>(*range.max_time, scale)};
    }

    chassert(isDateTime(config.table_timestamp_type) || isUInt32(config.table_timestamp_type));
    return SampleTimeBounds{UInt64(range.min_time->value), UInt64(range.max_time->value)};
}

VirtualColumnsDescription StorageTimeSeriesSelector::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral(
        "_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral(
        "_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
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
    return makeASTFunction(
        "arrayElement", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags), make_intrusive<ASTLiteral>(tag_name));
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
        /// `IN` matches exactly what the fully anchored regex matches, and the primary key can filter by it.
        if (auto terms = tryGetLiteralAlternatives(value))
        {
            /// A single term is an equality, which the text index on tags can serve too.
            if (terms->size() == 1)
                return makeASTFunction(add_not ? "notEquals" : "equals",
                    tagNameToAST(matcher.label_name, column_name_by_tag_name), make_intrusive<ASTLiteral>(std::move(terms->front())));

            ASTs literals;
            for (auto & term : *terms)
                literals.push_back(make_intrusive<ASTLiteral>(std::move(term)));
            return makeASTFunction(add_not ? "notIn" : "in",
                tagNameToAST(matcher.label_name, column_name_by_tag_name), makeASTFunction("tuple", std::move(literals)));
        }

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

/// Returns the expression generating the identifiers of the table, resolved the same way as in `TimeSeriesSink`:
/// the `id_generator` setting, then the DEFAULT of the `id` column of the tags table, then the default generator.
ASTPtr getIDGenerator(const TimeSeriesSettings & time_series_settings, const ColumnsDescription & tags_table_columns, const DataTypePtr & table_id_type)
{
    ASTPtr id_generator = time_series_settings[TimeSeriesSetting::id_generator].value;
    if (!id_generator)
    {
        if (const auto * tags_id_column = tags_table_columns.tryGet(TimeSeriesColumnNames::ID))
            id_generator = tags_id_column->default_desc.expression;
    }
    if (!id_generator)
        id_generator = TimeSeriesIDGenerator::tryGetDefault(table_id_type);
    return id_generator;
}

/// Adds to a query selecting from the tags table the filter of the time series by their time ranges stored in the time ranges table:
///
///     SELECT ... FROM tags_table
///     LEFT JOIN (SELECT id, 1 AS matched FROM time_ranges_table
///                WHERE max_time >= <min_time> AND min_time <= <max_time> [AND <id is in one of metric_id_ranges>]) AS time_ranges_subquery USING (id)
///     WHERE <conditions of the query> AND (time_ranges_subquery.matched = 1 [OR <id is outside all of metric_id_ranges>])
///
/// The join is a merge join over both tables read in the order of `id` (see StorageTimeSeriesSelector::applySettingsForSelectIDsQuery):
/// no set is built, the reading stops as soon as the query has enough rows, and the expressions of the SELECT list see only
/// the time series passing the filter (`timeSeriesStoreTags` registers the tags of the time series it's applied to).
/// With `metric_id_ranges` the time ranges table is read by its primary key over the ranges of the selected metrics, and the time series
/// with identifiers outside the ranges (written with another id generator) are kept without filtering by time.
void addTimeRangesTableJoin(
    ASTSelectQuery & select_query,
    const StorageID & time_ranges_table_id,
    ASTs time_range_conditions,
    const std::optional<TimeSeriesIDRanges> & metric_id_ranges)
{
    const char * subquery_alias = "time_ranges_subquery";
    const char * matched_column = "matched";
    ASTPtr id = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID);

    /// SELECT id, 1 AS matched FROM time_ranges_table WHERE <time_range_conditions> [AND <id is in one of metric_id_ranges>]
    auto subquery_select = make_intrusive<ASTSelectQuery>();
    {
        auto select_list_exp = make_intrusive<ASTExpressionList>();
        select_list_exp->children.push_back(id->clone());
        select_list_exp->children.push_back(make_intrusive<ASTLiteral>(UInt64{1}));
        select_list_exp->children.back()->setAlias(matched_column);
        subquery_select->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
        subquery_select->setExpression(ASTSelectQuery::Expression::TABLES, makeTablesInSelectQuery(make_intrusive<ASTTableIdentifier>(time_ranges_table_id)));
        if (metric_id_ranges)
            time_range_conditions.push_back(makeIDInRangesCondition(id, *metric_id_ranges));
        subquery_select->setExpression(ASTSelectQuery::Expression::WHERE, makeASTForLogicalAnd(std::move(time_range_conditions)));
    }

    /// LEFT JOIN (<subquery>) AS time_ranges_subquery USING (id)
    {
        auto table_join = make_intrusive<ASTTableJoin>();
        table_join->kind = JoinKind::Left;
        table_join->strictness = JoinStrictness::All;
        table_join->using_expression_list = make_intrusive<ASTExpressionList>();
        table_join->using_expression_list->children.push_back(id->clone());
        table_join->children.push_back(table_join->using_expression_list);

        auto table_exp = make_intrusive<ASTTableExpression>();
        table_exp->subquery = make_intrusive<ASTSubquery>(wrapIntoSelectWithUnionQuery(std::move(subquery_select)));
        table_exp->subquery->setAlias(subquery_alias);
        table_exp->children.push_back(table_exp->subquery);

        auto element = make_intrusive<ASTTablesInSelectQueryElement>();
        element->table_expression = table_exp;
        element->table_join = table_join;
        element->children.push_back(element->table_expression);
        element->children.push_back(element->table_join);

        select_query.tables()->children.push_back(std::move(element));
    }

    /// WHERE ... AND (time_ranges_subquery.matched = 1 [OR <id is outside all of metric_id_ranges>])
    {
        ASTPtr condition = makeASTFunction(
            "equals", make_intrusive<ASTIdentifier>(std::vector<String>{subquery_alias, matched_column}), make_intrusive<ASTLiteral>(UInt64{1}));
        if (metric_id_ranges)
            condition = makeASTForLogicalOr({std::move(condition), makeIDOutOfRangesCondition(id, *metric_id_ranges)});
        if (select_query.where())
            condition = makeASTForLogicalAnd({select_query.where(), std::move(condition)});
        select_query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(condition));
    }
}

/// Applies the time range filter to a query selecting identifiers from the tags table:
/// if the time ranges are stored in the time ranges table, that table is joined (see addTimeRangesTableJoin);
/// if the tags table stores them itself (tables of earlier versions), the conditions are already in the WHERE clause
/// of the query (see makeWhereFilterForTagsTable), so the query is left as is.
void applyTimeRangeFilterToSelectIDsQuery(
    ASTSelectQuery & select_query,
    const StorageID & time_ranges_table_id,
    const TableTimeRange & time_range_to_filter_ids,
    const std::optional<TimeSeriesIDRanges> & metric_id_ranges,
    const DataTypePtr & table_timestamp_type)
{
    if (!time_ranges_table_id || time_range_to_filter_ids.containsAllTimestamps())
        return;

    addTimeRangesTableJoin(select_query, time_ranges_table_id, makeTimeRangeConditions(time_range_to_filter_ids, table_timestamp_type), metric_id_ranges);
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
    const std::optional<TimeSeriesIDRanges> & metric_id_ranges,
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

    /// If there is a time ranges table: LEFT JOIN it and keep the time series with a matching time range (see addTimeRangesTableJoin).
    applyTimeRangeFilterToSelectIDsQuery(*select_query, time_ranges_table_id, time_range_to_filter_ids, metric_id_ranges, table_timestamp_type);

    return wrapIntoSelectWithUnionQuery(std::move(select_query));
}

ASTPtr makeWhereFilterForRowSamplesTable(
    ASTPtr select_query_from_tags_table,
    DateTime64 min_time,
    DateTime64 max_time,
    const DataTypePtr & timestamp_data_type,
    ASTs whole_metric_id_range_conditions)
{
    ASTs conditions;

    /// Keep the timestamp range before `id IN <set>`: for the default row schema
    /// (ORDER BY (id, timestamp)) the timestamp predicate is typically more selective.
    conditions.push_back(makeASTFunction(
        "greaterOrEquals",
        make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
        timeSeriesTimestampToAST(min_time, timestamp_data_type)));
    conditions.push_back(makeASTFunction(
        "lessOrEquals",
        make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
        timeSeriesTimestampToAST(max_time, timestamp_data_type)));

    auto select_as_subquery = make_intrusive<ASTSubquery>(std::move(select_query_from_tags_table));
    conditions.push_back(makeASTFunction("in", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID), std::move(select_as_subquery)));

    for (auto & condition : whole_metric_id_range_conditions)
        conditions.push_back(std::move(condition));

    return makeASTForLogicalAnd(std::move(conditions));
}

ASTPtr makeSelectQueryFromRowSamplesTable(
    const StorageID & samples_table_id,
    ASTPtr select_query_from_tags_table,
    DateTime64 min_time,
    DateTime64 max_time,
    const DataTypePtr & timestamp_data_type,
    ASTs whole_metric_id_range_conditions)
{
    auto select_query = make_intrusive<ASTSelectQuery>();

    auto select_list_exp = make_intrusive<ASTExpressionList>();
    auto & select_list = select_list_exp->children;
    select_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
    select_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
    select_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value));
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);

    auto tables = make_intrusive<ASTTablesInSelectQuery>();
    auto table = make_intrusive<ASTTablesInSelectQueryElement>();
    auto table_exp = make_intrusive<ASTTableExpression>();
    table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(samples_table_id);
    table_exp->children.emplace_back(table_exp->database_and_table_name);
    table->table_expression = table_exp;
    tables->children.push_back(table);
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

    auto where_filter = makeWhereFilterForRowSamplesTable(
        std::move(select_query_from_tags_table),
        min_time,
        max_time,
        timestamp_data_type,
        std::move(whole_metric_id_range_conditions));
    select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_filter));

    auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
    select_with_union_query->union_mode = SelectUnionMode::UNION_DEFAULT;
    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select_query));
    select_with_union_query->children.push_back(std::move(list_of_selects));
    select_with_union_query->list_of_selects = select_with_union_query->children.back();
    return select_with_union_query;
}

ASTPtr makeSelectQueryForRowSamples(
    ASTPtr select_query_from_samples_table,
    const DataTypePtr & id_data_type,
    const DataTypePtr & timestamp_data_type,
    const DataTypePtr & scalar_data_type)
{
    auto select_query = make_intrusive<ASTSelectQuery>();
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

    auto nested_table_exp = make_intrusive<ASTTableExpression>();
    nested_table_exp->subquery = make_intrusive<ASTSubquery>(std::move(select_query_from_samples_table));
    nested_table_exp->children.push_back(nested_table_exp->subquery);
    auto nested_table = make_intrusive<ASTTablesInSelectQueryElement>();
    nested_table->table_expression = nested_table_exp;
    nested_table->children.push_back(nested_table->table_expression);
    auto nested_tables = make_intrusive<ASTTablesInSelectQuery>();
    nested_tables->children.push_back(nested_table);
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, nested_tables);

    auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
    select_with_union_query->union_mode = SelectUnionMode::UNION_DEFAULT;
    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select_query));
    select_with_union_query->children.push_back(std::move(list_of_selects));
    select_with_union_query->list_of_selects = select_with_union_query->children.back();
    return select_with_union_query;
}

/// Makes the conditions `bucket >= <the bucket of min_time> AND bucket <= <max_time>` selecting the buckets
/// which can contain samples with timestamps in [min_time, max_time].
ASTs makeBucketRangeConditions(
    DateTime64 min_time, DateTime64 max_time, const DataTypePtr & timestamp_data_type, UInt64 bucket_step_seconds)
{
    /// The step of the buckets with the scale of the timestamp type.
    Int64 scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(tryGetDecimalScale(*timestamp_data_type).value_or(0));
    Decimal64 bucket_step{static_cast<Int64>(bucket_step_seconds) * scale_multiplier};

    /// The same rounding as in TimeSeriesSink: towards negative infinity, so that a bucket starts at or before its samples.
    DateTime64 min_bucket{DateLUTImpl::roundDownToMultiple(min_time.value, bucket_step.value)};
    DateTime64 max_bucket = max_time;

    ASTs conditions;

    /// bucket >= <min_bucket>
    conditions.push_back(makeASTFunction(
        "greaterOrEquals",
        make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Bucket),
        timeSeriesTimestampToAST(min_bucket, timestamp_data_type)));

    /// bucket <= <max_bucket>
    conditions.push_back(makeASTFunction(
        "lessOrEquals",
        make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Bucket),
        timeSeriesTimestampToAST(max_bucket, timestamp_data_type)));

    return conditions;
}

ASTPtr makeFilterForSamplesTable(
    ASTPtr select_query_from_tags_table,
    DateTime64 min_time,
    DateTime64 max_time,
    const DataTypePtr & timestamp_data_type,
    UInt64 bucket_step_seconds,
    std::optional<UInt64> max_raw_samples_per_row,
    ASTs whole_metric_id_range_conditions)
{
    ASTs conditions;

    /// Emit the time range conditions BEFORE the `id IN <set>` condition: on the default schema
    /// (ORDER BY (id, bucket)) primary-key pruning already returns mostly granules of matched
    /// series, so the `id IN <set>` check passes almost all read rows, while the time range
    /// is the selective condition (e.g. a few rows per granule for a short lookback).
    /// The PREWHERE optimizer keeps this order whenever its selectivity estimation is inconclusive,
    /// and running the cheap comparisons before the hash-set probe of `in` significantly
    /// reduces the scan CPU of short-window selectors.

    /// The conditions on `bucket` are used by the primary key index; the conditions on `min_time` and `max_time`
    /// skip the rows whose samples are all outside of the requested range.

    /// bucket >= <min_bucket> AND bucket <= <max_bucket>
    insertAtEnd(conditions, makeBucketRangeConditions(min_time, max_time, timestamp_data_type, bucket_step_seconds));

    /// max_time >= min_time
    conditions.push_back(makeASTFunction(
        "greaterOrEquals",
        make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MaxTime),
        timeSeriesTimestampToAST(min_time, timestamp_data_type)));

    /// min_time <= max_time
    conditions.push_back(makeASTFunction(
        "lessOrEquals",
        make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MinTime),
        timeSeriesTimestampToAST(max_time, timestamp_data_type)));

    /// id IN (SELECT id FROM (select_id_query))
    /// Wrap the SELECT in ASTSubquery so it formats with surrounding parentheses.
    auto select_as_subquery = make_intrusive<ASTSubquery>(std::move(select_query_from_tags_table));
    auto id_is_selected = makeASTFunction("in", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID), std::move(select_as_subquery));

    if (max_raw_samples_per_row)
    {
        /// The raw native path intentionally reads the physical `samples` array and slices it in
        /// its transform. Reject an oversized selected row from its offsets subcolumn before the
        /// nested timestamps and values are materialized. Silently filtering a selected row would
        /// change the PromQL result, so the guard must fail closed.
        auto samples_size = make_intrusive<ASTIdentifier>(String{TimeSeriesColumnNames::Samples} + ".size0");
        auto exceeds_limit = makeASTFunction(
            "greater", std::move(samples_size), make_intrusive<ASTLiteral>(*max_raw_samples_per_row));
        /// Keep the full row selection inside `throwIf`: PREWHERE may reorder its outer conditions,
        /// and disabling short-circuit evaluation must not reject an oversized row outside the
        /// selected series or time range.
        ASTs selected_and_oversized_conditions;
        selected_and_oversized_conditions.reserve(conditions.size() + 2);
        for (const auto & condition : conditions)
            selected_and_oversized_conditions.push_back(condition->clone());
        selected_and_oversized_conditions.push_back(id_is_selected->clone());
        selected_and_oversized_conditions.push_back(std::move(exceeds_limit));
        auto selected_and_oversized = makeASTForLogicalAnd(std::move(selected_and_oversized_conditions));
        auto throw_if_oversized = makeASTFunction(
            "throwIf",
            std::move(selected_and_oversized),
            make_intrusive<ASTLiteral>(
                "PromQL native raw selector exceeded max_promql_native_rate_samples_per_series in one stored row"));
        conditions.push_back(makeASTFunction("not", std::move(throw_if_oversized)));
    }

    conditions.push_back(std::move(id_is_selected));

    /// For a whole-metric selector over a metric-clustered id layout one more condition is
    /// added: indexHint(<raw id column> >= tuple(hash(metric_name), min) AND <raw id column>
    /// <= tuple(hash(metric_name), max)). `indexHint` keeps it out of the row-level filter, so
    /// its only purpose is to give the primary-key index analysis a continuous key range
    /// instead of the large set (see readImpl).
    insertAtEnd(conditions, std::move(whole_metric_id_range_conditions));

    return makeASTForLogicalAnd(std::move(conditions));
}

ASTPtr makeSelectQueryFromSamplesTable(
    const StorageID & samples_table_id,
    ASTPtr select_query_from_tags_table,
    DateTime64 min_time,
    DateTime64 max_time,
    const DataTypePtr & timestamp_data_type,
    UInt64 bucket_step_seconds,
    bool use_prewhere,
    StorageTimeSeriesSelector::SamplesReadOrder samples_read_order,
    StorageTimeSeriesSelector::SamplesReadMode samples_read_mode,
    std::optional<UInt64> max_raw_samples_per_row,
    ASTs whole_metric_id_range_conditions)
{
    auto select_query = make_intrusive<ASTSelectQuery>();

    /// SELECT id, timeSeriesSliceSortedArray(samples, min_time, max_time) AS time_series
    /// or, for a native consumer which slices the nested columns itself:
    /// SELECT id, samples
    ///
    /// A row of the samples table contains the samples of one series within one bucket sorted by timestamp,
    /// `timeSeriesSliceSortedArray` cuts out the samples with timestamps in [min_time, max_time].
    ///
    /// The `id` column is read as is, without a cast to the data type declared by this storage.
    /// A cast aliased in the SELECT list (e.g. `toUInt64(id) AS id`) would shadow the raw column,
    /// and the filter conditions below would wrap the primary key column, degrading the index
    /// analysis and their ordering.
    /// The casts to the declared types are applied by an outer SELECT instead (see `makeSelectQuery`).
    {
        auto select_list_exp = make_intrusive<ASTExpressionList>();
        auto & select_list = select_list_exp->children;

        select_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));

        if (samples_read_order == StorageTimeSeriesSelector::SamplesReadOrder::IdBucket)
            select_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Bucket));

        if (samples_read_mode == StorageTimeSeriesSelector::SamplesReadMode::Raw)
            select_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Samples));
        else
        {
            select_list.push_back(makeASTFunction(
                "timeSeriesSliceSortedArray",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Samples),
                timeSeriesTimestampToAST(min_time, timestamp_data_type),
                timeSeriesTimestampToAST(max_time, timestamp_data_type)));
            select_list.back()->setAlias(TimeSeriesColumnNames::TimeSeries);
        }

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

    if (samples_read_order == StorageTimeSeriesSelector::SamplesReadOrder::IdBucket)
        select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, makeOrderByIDAndBucket());

    /// PREWHERE (bucket >= <min_bucket>) AND (bucket <= <max_bucket>) AND (max_time >= min_time) AND (min_time <= max_time)
    ///          AND (id IN <select_query_from_tags_table>)
    ///
    /// or WHERE with the same conditions if the samples storage doesn't support PREWHERE.
    ///
    /// where <select_query_from_tags_table> is roughly:
    ///   SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name, ...) FROM tags_table WHERE <matchers>
    {
        /// Keep the cheap row-selection conditions in an explicit PREWHERE. If they are left in WHERE,
        /// predicate pushdown combines them with the outer `notEmpty(time_series)` filter and moves the
        /// whole expression to PREWHERE, so the large `samples` column is read before these conditions
        /// can discard rows.
        auto filter = makeFilterForSamplesTable(
            select_query_from_tags_table,
            min_time,
            max_time,
            timestamp_data_type,
            bucket_step_seconds,
            max_raw_samples_per_row,
            std::move(whole_metric_id_range_conditions));
        select_query->setExpression(
            use_prewhere ? ASTSelectQuery::Expression::PREWHERE : ASTSelectQuery::Expression::WHERE, std::move(filter));
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
/// SELECT _CAST(id, 'UInt64') AS id, [_CAST(bucket, 'DateTime64(3)') AS bucket,]
///        _CAST(time_series, 'Array(Tuple(DateTime64(3), Float64))') AS time_series
/// FROM (select_query_from_samples_table)
///
/// The inner query reads the samples table columns as is (see makeSelectQueryFromSamplesTable()),
/// so its result types are the physical column types, which can differ from the expected ones
/// (e.g. a samples table can store timestamps with a different timezone, and the tuple elements
/// of the `samples` column have names). Casting in an outer
/// SELECT keeps the filter conditions of the inner query on the bare primary key columns, and
/// the casts run only for the rows which passed the filter. The internal `_CAST` is used here
/// because it returns exactly the specified type (`CAST` and conversion functions like
/// `toDateTime64` keep the timezone of the casted expression), and it is free when the type
/// already matches.
ASTPtr makeSelectQuery(
    ASTPtr select_query_from_samples_table,
    const DataTypePtr & id_data_type,
    const DataTypePtr & timestamp_data_type,
    const DataTypePtr & scalar_data_type,
    bool include_bucket,
    StorageTimeSeriesSelector::SamplesReadMode samples_read_mode)
{
    auto select_query = make_intrusive<ASTSelectQuery>();

    /// SELECT _CAST(id, 'UInt64') AS id, [_CAST(bucket, 'DateTime64(3)') AS bucket,]
    ///        _CAST(time_series, 'Array(Tuple(DateTime64(3), Float64))') AS time_series
    {
        auto select_list_exp = make_intrusive<ASTExpressionList>();
        auto & select_list = select_list_exp->children;

        select_list.push_back(makeASTFunction(
            "_CAST", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID), make_intrusive<ASTLiteral>(id_data_type->getName())));
        select_list.back()->setAlias(TimeSeriesColumnNames::ID);

        if (include_bucket)
        {
            select_list.push_back(makeASTFunction(
                "_CAST",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Bucket),
                make_intrusive<ASTLiteral>(timestamp_data_type->getName())));
            select_list.back()->setAlias(TimeSeriesColumnNames::Bucket);
        }

        DataTypePtr time_series_data_type
            = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{timestamp_data_type, scalar_data_type}));

        const char * samples_column_name = samples_read_mode == StorageTimeSeriesSelector::SamplesReadMode::Raw
            ? TimeSeriesColumnNames::Samples
            : TimeSeriesColumnNames::TimeSeries;
        select_list.push_back(makeASTFunction(
            "_CAST", make_intrusive<ASTIdentifier>(samples_column_name), make_intrusive<ASTLiteral>(time_series_data_type->getName())));
        select_list.back()->setAlias(samples_column_name);

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

    if (samples_read_mode == StorageTimeSeriesSelector::SamplesReadMode::Sliced)
    {
        /// WHERE notEmpty(time_series)
        /// The conditions of the inner query select the rows by `min_time` and `max_time`, so a row can match them
        /// without having samples in the requested interval; such rows are filtered out here.
        select_query->setExpression(
            ASTSelectQuery::Expression::WHERE,
            makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries)));
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

/// Checks whether the selector can carry a primary-key range on the samples table's `id` column covering the whole metric,
/// and makes the range condition if it can:
/// `indexHint(tuple(hash(metric_name), min_S) <= id AND id <= tuple(hash(metric_name), max_S))`.
///
/// When the selector's matchers select ALL (time-eligible) series of the metric - the dominant shape of dashboard and
/// recording-rule queries - the large `id IN <set>` condition adds nothing to primary-key index analysis over the range
/// of the metric (see TimeSeriesIDRange), while costing a generic exclusion search over the set (hundreds of milliseconds per part
/// per query for tens of thousands of series, single-threaded). The range conditions returned here select the same granules
/// through the cheap continuous-range path.
///
/// The decision is advisory with respect to correctness: the returned range is a SUPERSET of the resolved id set
/// (verified over the existing series by the probe below and guaranteed for future inserts by the id generator),
/// and the `id IN <set>` condition is kept in the WHERE for exact row-level filtering. Any rows a hash collision could add
/// to the range are still rejected row-by-row.
///
/// Returns nullptr (= emit today's SQL) unless ALL of the following hold:
/// 1. The matchers select exactly one metric (`metric_id_ranges` has one range).
/// 2. The samples table physically stores `id` with exactly the type the range is made for.
/// 3. A probe query on the tags table (and the time ranges table if any) finds NO time-eligible series of the metric that either
///    fails the matchers other than the one on `__name__` (the selector does not select the whole metric) or has an id outside
///    the range (rows written before an `ALTER ... MODIFY SETTING id_generator`).
ASTPtr tryMakeWholeMetricIDRangeCondition(
    const std::optional<TimeSeriesIDRanges> & metric_id_ranges,
    const PrometheusQueryTree::MatcherList & matchers,
    const std::unordered_map<String, String> & column_name_by_tag_name,
    const StorageID & samples_table_id,
    const ColumnsDescription & samples_table_columns,
    const StorageID & tags_table_id,
    const StorageID & time_ranges_table_id,
    const TableTimeRange & time_range_to_filter_ids,
    const DataTypePtr & table_id_type,
    const DataTypePtr & table_timestamp_type,
    const ContextPtr & context,
    const LoggerPtr & log)
{
    /// 1. One metric: the range conditions and the probe below are made for the series of one metric.
    if (!metric_id_ranges || (metric_id_ranges->size() != 1))
        return {};
    const TimeSeriesIDRange & metric_id_range = metric_id_ranges->front();

    /// 2. The samples table stores `id` physically with exactly this type: the range conditions
    /// compare the raw column (bypassing the identity-cast alias of the SELECT list).
    auto samples_table_id_column = samples_table_columns.tryGetPhysical(TimeSeriesColumnNames::ID);
    if (!samples_table_id_column || (samples_table_id_column->type->getName() != table_id_type->getName()))
        return {};

    /// 3. The probe: find one time-eligible series of the metric that contradicts the range emission,
    /// i.e. fails the remaining matchers or does not hash into the range.
    ///
    ///     SELECT 1 FROM (
    ///         SELECT id FROM tags_table [LEFT JOIN <time ranges> USING (id)]
    ///         WHERE <__name__ matcher> AND <time range filter>
    ///           AND (NOT (<other matchers>) OR tupleElement(id, 1) != <first_component>))
    ///     LIMIT 1
    ///
    /// The time range filter is the same as in the tags subquery (see applyTimeRangeFilterToSelectIDsQuery); it keeps the series
    /// with ids outside the range, so they are found whatever their time ranges are. The merge join streams the tables,
    /// so the reading stops at the first row found.
    ///
    /// One such series means the id set is not the whole metric's primary-key range: fall back.
    /// No such series means every series the tags subquery can select lies in the range. The probe result cannot be raced
    /// into incorrectness: series inserted after the probe get their ids from the current (canonical) generator, so they stay
    /// inside the range, and the `id IN <set>` condition keeps doing the exact row-level filtering either way.
    {
        PrometheusQueryTree::MatcherList name_matcher_only;
        ASTs other_matcher_conditions;
        for (const auto & matcher : matchers)
        {
            /// The range exists, so there is exactly one matcher selecting the metric by name: an equality or a regex of one literal.
            if ((matcher.label_name == TimeSeriesTagNames::MetricName)
                && ((matcher.matcher_type == PrometheusQueryTree::MatcherType::EQ) || (matcher.matcher_type == PrometheusQueryTree::MatcherType::RE)))
                name_matcher_only.push_back(matcher);
            else
                other_matcher_conditions.push_back(matcherToAST(matcher, column_name_by_tag_name));
        }

        ASTPtr counterexample = makeIDOutOfRangeCondition(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID), metric_id_range);
        if (!other_matcher_conditions.empty())
        {
            counterexample = makeASTForLogicalOr(
                {makeASTFunction("not", makeASTForLogicalAnd(std::move(other_matcher_conditions))), std::move(counterexample)});
        }

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
        applyTimeRangeFilterToSelectIDsQuery(*probe_select_ids, time_ranges_table_id, time_range_to_filter_ids, metric_id_ranges, table_timestamp_type);

        /// SELECT 1 FROM (<probe_select_ids>) LIMIT 1
        auto probe_select = make_intrusive<ASTSelectQuery>();
        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            select_list_exp->children.push_back(make_intrusive<ASTLiteral>(UInt64{1}));
            probe_select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list_exp));
            probe_select->setExpression(
                ASTSelectQuery::Expression::TABLES,
                makeTablesInSelectQuery(make_intrusive<ASTSubquery>(wrapIntoSelectWithUnionQuery(std::move(probe_select_ids)))));
            probe_select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, make_intrusive<ASTLiteral>(UInt64{1}));
        }

        ASTPtr probe_query = wrapIntoSelectWithUnionQuery(std::move(probe_select));

        LOG_DEBUG(log, "Probing whether selector matches the whole metric {}: {}", quoteString(metric_id_range.metric_name), probe_query->formatForLogging());

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

    /// The range conditions on the raw `id` column of the samples table, qualified so that they resolve to the table column
    /// and not to the same-named alias of the SELECT list. Wrapped in `indexHint` so the range reaches index analysis but is not
    /// evaluated per row: the `id IN <set>` condition the caller keeps is the exact filter, and on this path every id of that set
    /// is inside the range (that is what the probe establishes).
    ASTPtr qualified_id = make_intrusive<ASTIdentifier>(std::vector<String>{samples_table_id.database_name, samples_table_id.table_name, TimeSeriesColumnNames::ID});
    return makeASTFunction("indexHint", makeIDInRangeCondition(qualified_id, metric_id_range));
}

/// Preserve the native planner's exact-name union even when a metric name needs regex escaping.
/// The generic literal-alternative parser deliberately declines those regexes.
std::optional<TimeSeriesIDRanges> tryGetSelectorMetricIDRanges(
    const PrometheusQueryTree::MatcherList & matchers,
    const Names & exact_metric_names,
    const ASTPtr & id_generator,
    const DataTypePtr & table_id_type)
{
    if (exact_metric_names.empty())
        return tryGetTimeSeriesIDRanges(matchers, id_generator, table_id_type);

    const PrometheusQueryTree::Matcher * name_matcher = nullptr;
    for (const auto & matcher : matchers)
    {
        if (matcher.label_name == TimeSeriesTagNames::MetricName)
        {
            if (name_matcher)
                return {};
            name_matcher = &matcher;
        }
    }
    if (!name_matcher || name_matcher->matcher_type != PrometheusQueryTree::MatcherType::RE || exact_metric_names.size() < 2)
        return {};

    String canonical_union = "(?:";
    std::unordered_set<String> unique_metric_names;
    for (size_t i = 0; i != exact_metric_names.size(); ++i)
    {
        const auto & metric_name = exact_metric_names[i];
        if (metric_name.empty() || !unique_metric_names.emplace(metric_name).second)
            return {};
        if (i)
            canonical_union += '|';
        canonical_union += RE2::QuoteMeta(metric_name);
    }
    canonical_union += ')';
    if (name_matcher->label_value != canonical_union)
        return {};

    TimeSeriesIDRanges ranges;
    for (const auto & metric_name : exact_metric_names)
    {
        auto metric_matchers = matchers;
        for (auto & matcher : metric_matchers)
        {
            if (matcher.label_name == TimeSeriesTagNames::MetricName)
            {
                matcher.matcher_type = PrometheusQueryTree::MatcherType::EQ;
                matcher.label_value = metric_name;
            }
        }
        auto metric_ranges = tryGetTimeSeriesIDRanges(metric_matchers, id_generator, table_id_type);
        if (!metric_ranges || metric_ranges->size() != 1)
            return {};
        ranges.push_back(std::move(metric_ranges->front()));
    }
    return ranges;
}

/// A multi-metric hint is admitted only if each constituent metric passes the upstream probe.
/// Exact row filtering and tag collection still use the unchanged selector subquery.
ASTs tryMakeWholeMetricIDRangeConditions(
    const std::optional<TimeSeriesIDRanges> & metric_id_ranges,
    const PrometheusQueryTree::MatcherList & matchers,
    const Names & exact_metric_names,
    const std::unordered_map<String, String> & column_name_by_tag_name,
    const StorageID & samples_table_id,
    const ColumnsDescription & samples_table_columns,
    const StorageID & tags_table_id,
    const StorageID & time_ranges_table_id,
    const TableTimeRange & time_range_to_filter_ids,
    const DataTypePtr & table_id_type,
    const DataTypePtr & table_timestamp_type,
    const ContextPtr & context,
    const LoggerPtr & log)
{
    if (!metric_id_ranges)
        return {};

    if (exact_metric_names.empty())
    {
        auto condition = tryMakeWholeMetricIDRangeCondition(
            metric_id_ranges, matchers, column_name_by_tag_name, samples_table_id, samples_table_columns,
            tags_table_id, time_ranges_table_id, time_range_to_filter_ids, table_id_type, table_timestamp_type, context, log);
        return condition ? ASTs{std::move(condition)} : ASTs{};
    }

    if (metric_id_ranges->size() != exact_metric_names.size())
        return {};

    for (size_t i = 0; i != exact_metric_names.size(); ++i)
    {
        const auto & range = metric_id_ranges->at(i);
        if (range.metric_name != exact_metric_names[i])
            return {};

        auto metric_matchers = matchers;
        for (auto & matcher : metric_matchers)
        {
            if (matcher.label_name == TimeSeriesTagNames::MetricName)
            {
                matcher.matcher_type = PrometheusQueryTree::MatcherType::EQ;
                matcher.label_value = range.metric_name;
            }
        }

        const std::optional<TimeSeriesIDRanges> metric_range{TimeSeriesIDRanges{range}};
        if (!tryMakeWholeMetricIDRangeCondition(
                metric_range, metric_matchers, column_name_by_tag_name, samples_table_id, samples_table_columns,
                tags_table_id, time_ranges_table_id, time_range_to_filter_ids, table_id_type, table_timestamp_type, context, log))
            return {};
    }

    ASTPtr qualified_id = make_intrusive<ASTIdentifier>(
        std::vector<String>{samples_table_id.database_name, samples_table_id.table_name, TimeSeriesColumnNames::ID});
    return {makeASTFunction("indexHint", makeIDInRangesCondition(qualified_id, *metric_id_ranges))};
}

}


void StorageTimeSeriesSelector::applySettingsForSelectIDsQuery(Context & context)
{
    /// The time ranges table is joined by a merge join over both tables read in the order of `id` (see addTimeRangesTableJoin):
    /// a hash join would put the whole time ranges table of the metric into a hash table first,
    /// and reading out of order would sort both tables before the first row of the result.
    context.setSetting("join_algorithm", String{"full_sorting_merge"});
    context.setSetting("optimize_read_in_order", true);
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
    ASTPtr select_query;
    if (time_range_to_filter_ids.containsNoTimestamps())
    {
        select_query = makeSelectNoIDsQuery(table_id_type);
    }
    else
    {
        auto tags_table_metadata = time_series_storage.getTargetTable(ViewTarget::Tags, context)->getInMemoryMetadataPtr(context, false);
        const auto metric_id_ranges = tryGetTimeSeriesIDRanges(
            matchers, getIDGenerator(time_series_settings, tags_table_metadata->getColumns(), table_id_type), table_id_type);
        select_query = makeSelectQueryFromTagsTable(
            tags_table_id, matchers, makeColumnNameByTagNameMap(time_series_settings), time_ranges_table_id, time_range_to_filter_ids,
            metric_id_ranges, table_timestamp_type);
    }

    /// Alias the returned expression (`timeSeriesStoreTags(...)`, which returns `id`) so callers can reference the column by a fixed name.
    const auto & select_with_union = typeid_cast<const ASTSelectWithUnionQuery &>(*select_query);
    auto & select = typeid_cast<ASTSelectQuery &>(*select_with_union.list_of_selects->children.at(0));
    select.select()->children.at(0)->setAlias("series_id");

    return select_query;
}


bool StorageTimeSeriesSelector::hasSamplesIdBucketOrder(const StorageMetadataPtr & samples_table_metadata)
{
    if (!samples_table_metadata)
        return false;

    const auto & sorting_key = samples_table_metadata->getSortingKey();
    if (!sorting_key.expression_list_ast || sorting_key.expression_list_ast->children.size() < 2)
        return false;

    if (sorting_key.column_names.size() < 2 || sorting_key.column_names[0] != TimeSeriesColumnNames::ID
        || sorting_key.column_names[1] != TimeSeriesColumnNames::Bucket)
        return false;

    if (!sorting_key.reverse_flags.empty()
        && (sorting_key.reverse_flags.size() != sorting_key.expression_list_ast->children.size() || sorting_key.reverse_flags[0]
            || sorting_key.reverse_flags[1]))
        return false;

    if (!sorting_key.expression_list_ast->children[0] || !sorting_key.expression_list_ast->children[1])
        return false;

    const auto * id = sorting_key.expression_list_ast->children[0]->as<ASTIdentifier>();
    const auto * bucket = sorting_key.expression_list_ast->children[1]->as<ASTIdentifier>();
    return id && id->isShort() && id->name() == TimeSeriesColumnNames::ID && bucket && bucket->isShort()
        && bucket->name() == TimeSeriesColumnNames::Bucket;
}


bool StorageTimeSeriesSelector::canReadSamplesInOrder(const StoragePtr & samples_table, const StorageMetadataPtr & samples_table_metadata)
{
    if (!samples_table)
        return false;

    const auto merge_tree = castStorage<MergeTreeData>(samples_table, DeferredTable::Skip);
    /// Keep proxy-backed targets ineligible until their metadata identity can be validated.
    return merge_tree && merge_tree.get() == samples_table.get() && hasSamplesIdBucketOrder(samples_table_metadata);
}


bool StorageTimeSeriesSelector::buildQueryPlan(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    size_t /* num_streams */,
    SamplesReadOrder samples_read_order,
    bool enable_whole_metric_id_range_optimization,
    SamplesReadMode samples_read_mode,
    const Names & exact_metric_names_for_whole_metric_id_range)
{
    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(config.time_series_storage_id, context));
    checkTimeSeriesVersionSupportedByPromQL(*time_series_storage);
    auto time_series_settings = time_series_storage->getStorageSettings();
    const bool has_bucketed_samples = time_series_storage->getVersion() >= TimeSeriesVersion::MIN_WITH_BUCKETED_SAMPLES;
    if (has_bucketed_samples)
        time_series_settings->checkBucketedSamplesSettings();

    const auto table_time_range
        = makeTableTimeRange(config.table_timestamp_type, config.min_time, config.max_time, config.time_scale);
    if (table_time_range.containsNoTimestamps())
    {
        auto header = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));
        query_plan.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(std::move(header)))));
        return true;
    }
    chassert(table_time_range.min_time && table_time_range.max_time);
    const DateTime64 min_time = *table_time_range.min_time;
    const DateTime64 max_time = *table_time_range.max_time;

    const auto & matchers = typeid_cast<const PrometheusQueryTree::InstantSelector &>(*config.selector.getRoot()).matchers;

    /// Prefer the recent samples table when the whole range fits in its TTL window: it's a much smaller copy of the recent samples.
    auto samples_table_kind = ViewTarget::Samples;
    UInt64 bucket_step_seconds = (*time_series_settings)[TimeSeriesSetting::samples_bucket_step_seconds];
    const auto recent_samples_ttl_seconds = (*time_series_settings)[TimeSeriesSetting::recent_samples_ttl_seconds].value;
    if (recent_samples_ttl_seconds && context->getSettingsRef()[Setting::time_series_prefer_recent_samples_table])
    {
        const UInt32 table_timestamp_scale = tryGetDecimalScale(*config.table_timestamp_type).value_or(0);
        if (isWithinRecentSamplesTTL(recent_samples_ttl_seconds, std::time(nullptr), min_time, table_timestamp_scale)
            && time_series_storage->tryGetTargetTable(ViewTarget::RecentSamples, context))
        {
            samples_table_kind = ViewTarget::RecentSamples;
            bucket_step_seconds = (*time_series_settings)[TimeSeriesSetting::recent_samples_bucket_step_seconds];
            LOG_DEBUG(
                log,
                "Selector {} time range [{}, {}] fits in the recent samples TTL window: reading from the recent samples table",
                quoteString(config.selector.toString()),
                min_time.value,
                max_time.value);
        }
    }

    auto samples_table = time_series_storage->getTargetTable(samples_table_kind, context);
    auto samples_table_id = samples_table->getStorageID();
    auto tags_table_id = time_series_storage->getTargetTableID(ViewTarget::Tags, context);

    auto column_name_by_tag_name = makeColumnNameByTagNameMap(*time_series_settings);

    const StorageID time_ranges_table_id = time_series_storage->hasTarget(ViewTarget::TimeRanges)
        ? time_series_storage->getTargetTableID(ViewTarget::TimeRanges, context)
        : StorageID::createEmpty();
    const auto time_range_to_filter_ids = getTimeRangeToFilterIDs(table_time_range, time_ranges_table_id, *time_series_settings);

    StorageMetadataPtr samples_metadata_before_planning;
    if (has_bucketed_samples)
    {
        /// Capture the underlying metadata identity before building a plan from its virtual snapshot.
        /// MergeTree copies that snapshot in debug builds, so the two virtual pointers cannot be compared.
        const auto metadata = samples_table->IStorage::getInMemoryMetadataPtr(context, /* bypass_metadata_cache = */ true);
        samples_metadata_before_planning = metadata;
    }
    auto samples_table_metadata = samples_table->getInMemoryMetadataPtr(context, /* bypass_metadata_cache = */ has_bucketed_samples);
    auto tags_table_metadata = time_series_storage->getTargetTable(ViewTarget::Tags, context)->getInMemoryMetadataPtr(context, false);

    /// Native ordered/raw fragments consume the bucketed physical layout. Keep the SQL selector
    /// for historical row-layout tables and let callers retain their existing SQL plan for modes
    /// which require buckets.
    if (!has_bucketed_samples
        && (samples_read_order != SamplesReadOrder::Unordered || samples_read_mode != SamplesReadMode::Sliced))
        return false;

    if (samples_read_order == SamplesReadOrder::IdBucket && !canReadSamplesInOrder(samples_table, samples_table_metadata))
        return false;

    bool samples_table_can_use_prewhere = samples_table->supportsPrewhere() && samples_table->canMoveConditionsToPrewhere();
    if (samples_table_can_use_prewhere)
    {
        if (const auto supported_prewhere_columns = samples_table->supportedPrewhereColumns())
        {
            samples_table_can_use_prewhere = supported_prewhere_columns->contains(TimeSeriesColumnNames::ID)
                && supported_prewhere_columns->contains(TimeSeriesColumnNames::Bucket)
                && supported_prewhere_columns->contains(TimeSeriesColumnNames::MinTime)
                && supported_prewhere_columns->contains(TimeSeriesColumnNames::MaxTime);
        }
    }

    std::optional<UInt64> max_raw_samples_per_row;
    if (samples_read_mode == SamplesReadMode::Raw)
    {
        /// Reading `samples.size0` in PREWHERE is the admission boundary which prevents the raw
        /// nested sample elements from being materialized before their limit is checked. If the
        /// target storage cannot prove that contract, decline this native fragment before it is
        /// installed and let the caller retain the SQL plan.
        if (!samples_table_can_use_prewhere || !samples_table->supportedPrewhereColumnsIncludeSubcolumns())
            return false;

        if (const auto supported_prewhere_columns = samples_table->supportedPrewhereColumns();
            supported_prewhere_columns && !supported_prewhere_columns->contains(TimeSeriesColumnNames::Samples))
            return false;

        max_raw_samples_per_row = context->getSettingsRef()[Setting::max_promql_native_rate_samples_per_series];
        if (!*max_raw_samples_per_row)
            return false;
    }

    auto modified_context = Context::createCopy(context);
    applySettingsForSelectIDsQuery(*modified_context);
    ContextPtr interpreter_context = modified_context;

    if (max_block_size)
        modified_context->setSetting("max_block_size", UInt64{max_block_size});

    /// The samples table is read in parallel when there is enough data:
    /// the threshold in bytes is lowered because a selector usually reads a small part of the table.
    if (!context->getSettingsRef().isChanged("merge_tree_min_bytes_for_concurrent_read"))
        modified_context->setSetting("merge_tree_min_bytes_for_concurrent_read", UInt64{4 * 1024 * 1024});

    if (!context->getSettingsRef().isChanged("merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem"))
        modified_context->setSetting("merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem", UInt64{4 * 1024 * 1024});

    const auto metric_id_ranges = tryGetSelectorMetricIDRanges(
        matchers, exact_metric_names_for_whole_metric_id_range,
        getIDGenerator(*time_series_settings, tags_table_metadata->getColumns(), config.table_id_type), config.table_id_type);

    ASTPtr select_query_from_tags_table = makeSelectQueryFromTagsTable(
        tags_table_id, matchers, column_name_by_tag_name, time_ranges_table_id, time_range_to_filter_ids,
        metric_id_ranges, config.table_timestamp_type);

    ASTs whole_metric_id_range_conditions;
    if (enable_whole_metric_id_range_optimization)
    {
        whole_metric_id_range_conditions = tryMakeWholeMetricIDRangeConditions(
            metric_id_ranges,
            matchers,
            exact_metric_names_for_whole_metric_id_range,
            column_name_by_tag_name,
            samples_table_id,
            samples_table_metadata->getColumns(),
            tags_table_id,
            time_ranges_table_id,
            time_range_to_filter_ids,
            config.table_id_type,
            config.table_timestamp_type,
            interpreter_context,
            log);
    }

    if (!whole_metric_id_range_conditions.empty())
    {
        /// The `id IN <tags subquery>` condition stays in the row-level filter for exact filtering
        /// (and its subquery keeps collecting the tags of the matched series), but its set must
        /// not enter primary-key index analysis: `KeyCondition` runs a generic exclusion search
        /// with the whole set, which costs hundreds of milliseconds per part for tens of
        /// thousands of series, single-threaded, while the whole-metric range conditions select
        /// the same granules through the cheap continuous-range path. Setting
        /// `use_index_for_in_with_subqueries_max_values = 1` makes the set unusable for index
        /// analysis without affecting the row-level filter.
        modified_context->setSetting("use_index_for_in_with_subqueries_max_values", UInt64{1});
        LOG_DEBUG(
            log,
            "Selector {} matches the whole metric: adding a primary-key range on id and excluding the id set from index analysis",
            quoteString(config.selector.toString()));
    }

    if (!has_bucketed_samples)
    {
        ASTPtr select_query_from_samples_table = makeSelectQueryFromRowSamplesTable(
            samples_table_id,
            std::move(select_query_from_tags_table),
            min_time,
            max_time,
            config.table_timestamp_type,
            std::move(whole_metric_id_range_conditions));
        ASTPtr select_query = makeSelectQueryForRowSamples(
            std::move(select_query_from_samples_table), config.table_id_type, config.table_timestamp_type, config.table_value_type);

        LOG_DEBUG(log, "Building row-layout SQL for selector: {}", config.selector.toString());
        LOG_DEBUG(log, "Will execute query:\n{}", select_query->formatForLogging());

        auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);
        InterpreterSelectQueryAnalyzer interpreter(select_query, interpreter_context, options, column_names);
        interpreter.addStorageLimits(*query_info.storage_limits);
        query_plan = std::move(interpreter).extractQueryPlan();
        return true;
    }

    /// The threshold in rows is disabled because a row of a bucketed table contains many samples,
    /// and otherwise the default threshold in rows (163840) would dominate the threshold in bytes.
    if (!context->getSettingsRef().isChanged("merge_tree_min_rows_for_concurrent_read"))
        modified_context->setSetting("merge_tree_min_rows_for_concurrent_read", UInt64{0});

    ASTPtr select_query_from_samples_table = makeSelectQueryFromSamplesTable(
        samples_table_id,
        select_query_from_tags_table,
        min_time,
        max_time,
        config.table_timestamp_type,
        bucket_step_seconds,
        samples_table_can_use_prewhere,
        samples_read_order,
        samples_read_mode,
        max_raw_samples_per_row,
        std::move(whole_metric_id_range_conditions));

    ASTPtr select_query = makeSelectQuery(
        std::move(select_query_from_samples_table),
        config.table_id_type,
        config.table_timestamp_type,
        config.table_value_type,
        samples_read_order == SamplesReadOrder::IdBucket,
        samples_read_mode);

    LOG_DEBUG(log, "Building SQL for selector: {}", config.selector.toString());
    LOG_DEBUG(log, "Will execute query:\n{}", select_query->formatForLogging());

    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);

    std::optional<StorageTimeSeries::LockedTargetTable> locked_samples_table;
    if (has_bucketed_samples)
    {
        locked_samples_table.emplace(time_series_storage->lockAndValidateBucketedSamplesTarget(
            samples_table_kind, samples_table, samples_table_id, interpreter_context));
        if (locked_samples_table->metadata.get() != samples_metadata_before_planning.get())
            throw Exception(ErrorCodes::UNFINISHED, "The {} target table of TimeSeries table {} changed while building a selector plan; retry",
                            samples_table_kind, time_series_storage->getStorageID().getNameForLogs());
    }
    if (locked_samples_table)
        FailPointInjection::pauseFailPoint(FailPoints::time_series_selector_pause_after_target_metadata);

    InterpreterSelectQueryAnalyzer interpreter(select_query, interpreter_context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    query_plan = std::move(interpreter).extractQueryPlan();
    if (locked_samples_table)
        time_series_storage->revalidateBucketedSamplesTarget(
            samples_table_kind, *locked_samples_table, samples_table_id, interpreter_context);
    return true;
}


void StorageTimeSeriesSelector::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    buildQueryPlan(
        query_plan,
        column_names,
        storage_snapshot,
        query_info,
        context,
        processed_stage,
        max_block_size,
        num_streams,
        SamplesReadOrder::Unordered);
}

}
