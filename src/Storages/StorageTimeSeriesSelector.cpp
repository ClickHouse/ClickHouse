#include <Storages/StorageTimeSeriesSelector.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Core/DecimalFunctions.h>
#include <Core/Joins.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
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
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/StoreLookedUpTagsStep.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesHistogramsColumns.h>
#include <Storages/TimeSeries/TimeSeriesIDGenerator.h>
#include <Storages/TimeSeries/TimeSeriesIDRange.h>
#include <Storages/TimeSeries/TimeSeriesSampleKinds.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/tryGetLiteralAlternatives.h>
#include <Storages/TimeSeries/checkTimeSeriesVersion.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <base/insertAtEnd.h>

#include <fmt/format.h>

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
    extern const SettingsUInt64 time_series_id_range_check_min_series;
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

/// Holds info about table time filtering.
/// `table_id` holds the name of a table with min/max_time columns if series should be filtered.
struct SeriesTimeFilter
{
    StorageID table_id = StorageID::createEmpty();
    TableTimeRange range;

    explicit operator bool() const { return static_cast<bool>(table_id); }
};

/// Builds `SeriesTimeFilter` struct based on the query settings and table time range.
/// Result's `range` always holds `table_time_range`.
/// Result's `table_id` is either `tags_table_id` or `time_ranges_table_id` or empty if series should not be filtered.
SeriesTimeFilter makeSeriesTimeFilter(
    const StorageID & tags_table_id,
    const TableTimeRange & table_time_range,
    const StorageID & time_ranges_table_id,
    const TimeSeriesSettings & settings)
{
    SeriesTimeFilter result;

    result.range = table_time_range;

    if (table_time_range.containsNoTimestamps())
        return result;

    if (time_ranges_table_id)
    {
        result.table_id = time_ranges_table_id;
    }
    else if (settings.hasMinTimeAndMaxTimeInTagsTable() && settings[TimeSeriesSetting::filter_by_min_time_and_max_time])
    {
        result.table_id = tags_table_id;
    }

    return result;
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
    config.with_histograms = timeSeriesVersionSupportsHistograms(time_series_storage->getVersion());
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
    constexpr const char * TIME_RANGES_JOIN_SUBQUERY_ALIAS = "time_ranges_subquery";
    constexpr const char * TIME_RANGES_JOIN_SUBQUERY_MATCHED_COLUMN = "matched";

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
    boost::intrusive_ptr<ASTSelectWithUnionQuery> wrapIntoSelectWithUnionQuery(ASTPtr select_query)
    {
        auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
        select_with_union_query->union_mode = SelectUnionMode::UNION_DEFAULT;
        auto list_of_selects = make_intrusive<ASTExpressionList>();
        list_of_selects->children.push_back(std::move(select_query));
        select_with_union_query->children.push_back(std::move(list_of_selects));
        select_with_union_query->list_of_selects = select_with_union_query->children.back();
        return select_with_union_query;
    }

    /// Makes the FROM clause of a select query reading a table (`table` is an ASTTableIdentifier) or a subquery (`table` is an ASTSubquery)
    /// or a table function (`table` is an ASTFunction).
    ASTPtr makeTablesInSelectQuery(ASTPtr table)
    {
        auto table_exp = make_intrusive<ASTTableExpression>();
        if (table->as<ASTSubquery>())
            table_exp->subquery = table;
        else if (table->as<ASTFunction>())
            table_exp->table_function = table;
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

    /// if(time_ranges_subquery.matched, _CAST(time_ranges_subquery.sample_kinds, 'UInt8'), ALL)
    ASTPtr makeSampleKindsOrAllExpression()
    {
        return makeASTFunction(
            "if",
            make_intrusive<ASTIdentifier>(std::vector<String>{TIME_RANGES_JOIN_SUBQUERY_ALIAS, TIME_RANGES_JOIN_SUBQUERY_MATCHED_COLUMN}),
            makeASTFunction(
                "_CAST",
                make_intrusive<ASTIdentifier>(std::vector<String>{TIME_RANGES_JOIN_SUBQUERY_ALIAS, TimeSeriesColumnNames::SampleKinds}),
                make_intrusive<ASTLiteral>(TimeSeriesSampleKinds::getDataType(/* aggregated = */ false)->getName())),
            make_intrusive<ASTLiteral>(UInt64{TimeSeriesSampleKinds::All}));
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
        const std::optional<TimeSeriesIDRanges> & metric_id_ranges,
        bool with_sample_kinds)
    {
        ASTPtr id = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID);

        /// SELECT id[, sample_kinds], 1 AS matched FROM
        ///     time_ranges_table WHERE <time_range_conditions> [AND <id is in one of metric_id_ranges>]
        auto subquery_select = make_intrusive<ASTSelectQuery>();
        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            select_list_exp->children.push_back(id->clone());
            if (with_sample_kinds)
            {
                select_list_exp->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::SampleKinds));
            }
            select_list_exp->children.push_back(make_intrusive<ASTLiteral>(UInt64{1}));
            select_list_exp->children.back()->setAlias(TIME_RANGES_JOIN_SUBQUERY_MATCHED_COLUMN);
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
            table_exp->subquery->setAlias(TIME_RANGES_JOIN_SUBQUERY_ALIAS);
            table_exp->children.push_back(table_exp->subquery);

            auto element = make_intrusive<ASTTablesInSelectQueryElement>();
            element->table_expression = table_exp;
            element->table_join = table_join;
            element->children.push_back(element->table_expression);
            element->children.push_back(element->table_join);

            select_query.tables()->children.push_back(std::move(element));
        }

        /// WHERE ... AND (time_ranges_subquery.matched [OR <id is outside all of metric_id_ranges>])
        {
            ASTPtr condition = make_intrusive<ASTIdentifier>(
                std::vector<String>{TIME_RANGES_JOIN_SUBQUERY_ALIAS, TIME_RANGES_JOIN_SUBQUERY_MATCHED_COLUMN});
            if (metric_id_ranges)
                condition = makeASTForLogicalOr({std::move(condition), makeIDOutOfRangesCondition(id, *metric_id_ranges)});
            if (select_query.where())
                condition = makeASTForLogicalAnd({select_query.where(), std::move(condition)});
            select_query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(condition));
        }
    }

    /// Makes the WHERE clause of a query selecting identifiers from the tags table.
    /// The conditions on the time range are added only if the tags table stores it itself (tables of earlier versions),
    /// otherwise the time range filter is applied by `addTimeRangesTableJoin`.
    /// The bounds of `time_range` must have the scale of `table_timestamp_type`.
    ASTPtr makeWhereFilterForTagsTable(
        const PrometheusQueryTree::MatcherList & matchers,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        const TableTimeRange & time_range,
        const DataTypePtr & table_timestamp_type)
    {
        ASTs asts;
        for (const auto & matcher : matchers)
            asts.push_back(matcherToAST(matcher, column_name_by_tag_name));

        if (asts.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Instant selector without matchers is not allowed");

        insertAtEnd(asts, makeTimeRangeConditions(time_range, table_timestamp_type));

        return makeASTForLogicalAnd(std::move(asts));
    }

    /// Makes `SELECT <none> FROM null('id <type>')`.
    boost::intrusive_ptr<ASTSelectQuery> makeSelectNoIDsQuery(const DataTypePtr & table_id_type, const char * column_name)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        auto column_desc = make_intrusive<ASTLiteral>(fmt::format("{} {}", column_name, table_id_type->getName()));
        select_query->setExpression(
            ASTSelectQuery::Expression::TABLES,
            makeTablesInSelectQuery(makeASTFunction("null", std::move(column_desc))));

        return select_query;
    }

    /// Makes the expressions for the names and the values of the tags of a series, in the form of the arguments of
    /// `timeSeriesStoreTags` after `id`: `tags, '__name__', metric_name, tag_name1, tag_column1, ...`.
    ASTs makeTagNamesAndValuesArguments(const std::unordered_map<String, String> & column_name_by_tag_name)
    {
        ASTs args;
        args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));
        args.push_back(make_intrusive<ASTLiteral>(TimeSeriesTagNames::MetricName));
        args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

        for (const auto & [tag_name, column_name] : column_name_by_tag_name)
        {
            args.push_back(make_intrusive<ASTLiteral>(tag_name));
            args.push_back(make_intrusive<ASTIdentifier>(column_name));
        }

        return args;
    }

    /// Makes `timeSeriesStoreTags(id, tags, '__name__', metric_name, tag_name1, tag_column1, ...)`.
    ASTPtr makeStoreTagsFunction(const std::unordered_map<String, String> & column_name_by_tag_name)
    {
        ASTs args{make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID)};
        insertAtEnd(args, makeTagNamesAndValuesArguments(column_name_by_tag_name));
        return makeASTFunction("timeSeriesStoreTags", std::move(args));
    }

    struct TagsTableSource
    {
        /// `SELECT <none> FROM tags_table [<join time ranges>] WHERE <matchers and time conditions>`
        boost::intrusive_ptr<ASTSelectQuery> query;
        /// If `true` the SELECT list may use `makeSampleKindsOrAllExpression`.
        bool has_sample_kinds = false;
    };

    /// Makes `FROM tags_table [<join time ranges>] WHERE <matchers and time conditions>`.
    TagsTableSource makeTagsTableSource(
        const StorageID & tags_table_id,
        const PrometheusQueryTree::MatcherList & matchers,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        const SeriesTimeFilter & time_filter,
        const std::optional<TimeSeriesIDRanges> & metric_id_ranges,
        const DataTypePtr & table_timestamp_type,
        bool with_sample_kinds)
    {
        TagsTableSource result;
        result.query = make_intrusive<ASTSelectQuery>();

        /// FROM tags_table_id
        result.query->setExpression(
            ASTSelectQuery::Expression::TABLES,
            makeTablesInSelectQuery(make_intrusive<ASTTableIdentifier>(tags_table_id)));

        /// WHERE <filter>
        const bool filter_by_time_in_tags_table = time_filter && time_filter.table_id == tags_table_id;
        {
            const auto & time_range = filter_by_time_in_tags_table
                ? time_filter.range
                : TableTimeRange::ALL_TIMESTAMPS;
            auto where_filter = makeWhereFilterForTagsTable(matchers, column_name_by_tag_name, time_range, table_timestamp_type);
            result.query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_filter));
        }

        /// If there is a time ranges table: LEFT JOIN it and keep the time series with a matching time range (see `addTimeRangesTableJoin`).
        if (time_filter && !filter_by_time_in_tags_table)
        {
            result.has_sample_kinds = with_sample_kinds;
            if (!with_sample_kinds && time_filter.range.containsAllTimestamps())
                return result;

            addTimeRangesTableJoin(
                *result.query,
                time_filter.table_id,
                makeTimeRangeConditions(time_filter.range, table_timestamp_type),
                metric_id_ranges,
                with_sample_kinds);
        }

        return result;
    }

    ASTPtr addProjectionToSelectQuery(boost::intrusive_ptr<ASTSelectQuery> && query, ASTs && select_list)
    {
        auto select_list_exp = make_intrusive<ASTExpressionList>();
        select_list_exp->children = std::move(select_list);
        query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);

        return wrapIntoSelectWithUnionQuery(std::move(query));
    }

    /// The bounds of `time_range_to_filter_samples` must have the scale of `table_timestamp_type`.
    ASTPtr makeWhereFilterForDataTable(
        const String & ids_tmp_table_name,
        const TableTimeRange & time_range_to_filter_samples,
        const DataTypePtr & table_timestamp_type,
        ASTs id_range_conditions)
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

        /// id IN ids_tmp_table
        conditions.push_back(makeASTFunction("in", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID), make_intrusive<ASTIdentifier>(ids_tmp_table_name)));

        /// The range of the selected ids: indexHint(<raw id column> >= <min id> AND <raw id column> <= <max id>).
        /// `indexHint` keeps it out of the row-level filter, so its only purpose is to give the primary-key index analysis
        /// a continuous key range, together with the id set or instead of it (see readImpl).
        for (auto & condition : id_range_conditions)
            conditions.push_back(std::move(condition));

        return makeASTForLogicalAnd(std::move(conditions));
    }

    /// Makes the projection of a select query from the samples table (or the recent samples table):
    /// SELECT id, timestamp, value [, _CAST([], '<histogram type>') AS histogram]
    /// The empty `histogram` is added for tables with histograms, so the samples rows match the rows of the histograms table.
    ASTs makeSamplesProjection(bool with_histograms)
    {
        ASTs projection;
        projection.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        projection.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
        projection.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value));

        if (with_histograms)
        {
            projection.push_back(makeASTFunction(
                "_CAST",
                make_intrusive<ASTLiteral>(Array{}),
                make_intrusive<ASTLiteral>(TimeSeriesHistogramsColumns::getHistogramColumnType()->getName())));
            projection.back()->setAlias(TimeSeriesColumnNames::Histogram);
        }

        return projection;
    }

    /// Makes the projection of a select query from the histograms table:
    /// SELECT id, timestamp, _CAST(0, '<scalar type>') AS value, _CAST([tuple(is_float, ...)], '<histogram type>') AS histogram
    /// `value` is a dummy for histogram rows. `histogram` holds the row as stored; the cast gives the tuple elements their names.
    ASTs makeHistogramsProjection(const DataTypePtr & scalar_data_type)
    {
        ASTs projection;
        projection.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        projection.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));

        projection.push_back(makeASTFunction(
            "_CAST", make_intrusive<ASTLiteral>(UInt64{0}), make_intrusive<ASTLiteral>(scalar_data_type->getName())));
        projection.back()->setAlias(TimeSeriesColumnNames::Value);

        ASTs payload_columns;
        for (auto column : TimeSeriesHistogramsColumns::getAll())
            payload_columns.push_back(make_intrusive<ASTIdentifier>(String{TimeSeriesHistogramsColumns::getName(column)}));
        projection.push_back(makeASTFunction(
            "_CAST",
            makeASTFunction("array", makeASTFunction("tuple", std::move(payload_columns))),
            make_intrusive<ASTLiteral>(TimeSeriesHistogramsColumns::getHistogramColumnType()->getName())));
        projection.back()->setAlias(TimeSeriesColumnNames::Histogram);

        return projection;
    }

    /// Makes a select query from a data table: SELECT <projection> FROM <data table> WHERE <filter>.
    ///
    /// The projection must read `id` and `timestamp` as is, without casts to the data types declared by this storage.
    /// A cast aliased in the SELECT list (e.g. `toDateTime64(timestamp, 3) AS timestamp`) would shadow the raw column,
    /// and the WHERE conditions below would wrap the primary key columns, degrading the index analysis and the ordering
    /// of the PREWHERE conditions. The casts to the declared types are applied by an outer SELECT instead
    /// (see `makeSelectQuery`).
    ASTPtr makeSelectQueryFromDataTable(const StorageID & data_table_id,
                                        ASTs projection,
                                        const String & ids_tmp_table_name,
                                        const TableTimeRange & time_range_to_filter_samples,
                                        const DataTypePtr & table_timestamp_type,
                                        ASTs id_range_conditions)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        /// SELECT <projection>
        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            select_list_exp->children = std::move(projection);
            select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
        }

        /// FROM samples_table_id
        auto tables = make_intrusive<ASTTablesInSelectQuery>();

        {
            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(data_table_id);
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table->table_expression = table_exp;
            table->children.push_back(table->table_expression);
            tables->children.push_back(table);

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        /// WHERE (timestamp >= min_time) AND (timestamp <= max_time) AND (id IN ids_tmp_table)
        {
            auto where_filter = makeWhereFilterForDataTable(
                ids_tmp_table_name, time_range_to_filter_samples, table_timestamp_type, std::move(id_range_conditions));
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_filter));
        }

        return select_query;
    }

    /// Wraps select queries into ASTSelectWithUnionQuery: `<select> UNION ALL <select> ...`, or just `<select>` if there is one.
    /// A query inside ASTSubquery must be ASTSelectWithUnionQuery.
    ASTPtr makeSelectWithUnionQuery(ASTs selects)
    {
        auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
        if (selects.size() > 1)
        {
            select_with_union_query->union_mode = SelectUnionMode::UNION_ALL;
            select_with_union_query->is_normalized = true;
        }
        else
        {
            select_with_union_query->union_mode = SelectUnionMode::UNION_DEFAULT;
        }
        auto list_of_selects = make_intrusive<ASTExpressionList>();
        list_of_selects->children = std::move(selects);
        select_with_union_query->children.push_back(std::move(list_of_selects));
        select_with_union_query->list_of_selects = select_with_union_query->children.back();
        return select_with_union_query;
    }

    /// Makes the final select query by wrapping the select query from the data tables into an outer
    /// SELECT which casts the columns to the data types expected by this storage:
    ///
    /// SELECT _CAST(id, 'UInt64') AS id, _CAST(timestamp, 'DateTime64(3)') AS timestamp, _CAST(value, 'Float64') AS value
    /// FROM (select_query_from_data_tables)
    ///
    /// The inner query reads the data table columns as is (see makeSelectQueryFromDataTable()),
    /// so its result types are the physical column types, which can differ from the expected ones
    /// (e.g. a samples table can store `timestamp` with a different timezone). Casting in an outer
    /// SELECT keeps the WHERE conditions of the inner query on the bare primary key columns, and
    /// the casts run only for the rows which passed the filter. The internal `_CAST` is used here
    /// because it returns exactly the specified type (`CAST` and conversion functions like
    /// `toDateTime64` keep the timezone of the casted expression), and it is free when the type
    /// already matches.
    ASTPtr makeSelectQuery(ASTPtr select_query_from_data_tables, const NamesAndTypesList & columns)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        /// SELECT _CAST(id, 'UInt64') AS id, _CAST(timestamp, 'DateTime64(3)') AS timestamp, _CAST(value, 'Float64') AS value
        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            auto & select_list = select_list_exp->children;

            for (const auto & [name, type] : columns)
            {
                select_list.push_back(makeASTFunction("_CAST", make_intrusive<ASTIdentifier>(name), make_intrusive<ASTLiteral>(type->getName())));
                select_list.back()->setAlias(name);
            }

            select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
        }

        /// FROM (select_query_from_data_tables)
        {
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->subquery = make_intrusive<ASTSubquery>(std::move(select_query_from_data_tables));
            table_exp->children.push_back(table_exp->subquery);

            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            table->table_expression = table_exp;
            table->children.push_back(table->table_expression);

            auto tables = make_intrusive<ASTTablesInSelectQuery>();
            tables->children.push_back(table);

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        return makeSelectWithUnionQuery({std::move(select_query)});
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

    /// The minimum and the maximum of the selected ids.
    struct IDRange
    {
        Field min;
        Field max;
    };

    /// The ids of the series selected by the tags lookup: the name under which the temporary table with the single column `id`
    /// holding them is registered in the context (see `lookUpTags`), their number, their type (the type of the tags table's
    /// `id` without `LowCardinality`), the kinds of their samples (see TimeSeriesSampleKinds) and, if the lookup tracked it,
    /// their range.
    struct LookedUpIDs
    {
        String registered_table_name;
        size_t count = 0;
        DataTypePtr type;
        UInt8 sample_kinds = TimeSeriesSampleKinds::All;
        std::optional<IDRange> range;
    };

    /// Updates `min` and `max`, columns of one row or null, with the rows of `column`. Tuples are compared lexicographically.
    void updateMinMax(const IColumn & column, ColumnPtr & min, ColumnPtr & max)
    {
        size_t min_row = 0;
        size_t max_row = 0;
        for (size_t row = 1; row < column.size(); ++row)
        {
            if (column.compareAt(row, min_row, column, /* nan_direction_hint = */ 1) < 0)
                min_row = row;
            if (column.compareAt(row, max_row, column, /* nan_direction_hint = */ 1) > 0)
                max_row = row;
        }

        const auto copy_row = [&](size_t row)
        {
            auto res = column.cloneEmpty();
            res->insertFrom(column, row);
            return ColumnPtr{std::move(res)};
        };

        if (!min || (column.compareAt(min_row, 0, *min, /* nan_direction_hint = */ 1) < 0))
            min = copy_row(min_row);
        if (!max || (column.compareAt(max_row, 0, *max, /* nan_direction_hint = */ 1) > 0))
            max = copy_row(max_row);
    }

    /// Makes `_CAST(<value>, '<type>')`. The cast keeps the type of the constant exact after the query is formatted and
    /// parsed again (e.g. for the replicas): a `UUID` inside a tuple literal would be parsed as a `String`.
    ASTPtr makeTypedLiteral(const Field & value, const DataTypePtr & type)
    {
        return makeASTFunction("_CAST", make_intrusive<ASTLiteral>(value), make_intrusive<ASTLiteral>(type->getName()));
    }

    /// Makes `<id> >= <min id> AND <id> <= <max id>`. The lookup must have tracked the range of the ids.
    ASTPtr makeIDRangeCondition(const ASTPtr & id, const LookedUpIDs & ids)
    {
        chassert(ids.range);
        return makeASTForLogicalAnd(
            {makeASTFunction("greaterOrEquals", id->clone(), makeTypedLiteral(ids.range->min, ids.type)),
             makeASTFunction("lessOrEquals", id->clone(), makeTypedLiteral(ids.range->max, ids.type))});
    }

    /// Makes the condition on the raw `id` column of a data table restricting it to the range of the selected ids.
    ASTPtr makeIDRangeConditionForDataTable(const LookedUpIDs & ids, const StorageID & data_table_id)
    {
        /// Qualified so that it resolves to the table column and not to the same-named alias of the SELECT list.
        const ASTPtr id = make_intrusive<ASTIdentifier>(
            std::vector<String>{data_table_id.database_name, data_table_id.table_name, TimeSeriesColumnNames::ID});

        /// Wrapped in `indexHint` so the range reaches index analysis but is not evaluated per row: the `id IN <set>`
        /// condition is the exact filter, and every id of the set is inside the range.
        return makeASTFunction("indexHint", makeIDRangeCondition(id, ids));
    }

    /// Whether every data table read by the selector (the samples table, and the histograms table if any) stores `id`
    /// physically with exactly the type of the tags table's `id`: the range conditions compare the raw column (bypassing
    /// the identity-cast alias of the SELECT list). The range is used for all of them or none, because the id set is
    /// excluded from index analysis for the whole query.
    bool dataTablesStoreIDType(const std::vector<const ColumnsDescription *> & data_tables_columns, const DataTypePtr & table_id_type)
    {
        for (const auto * data_table_columns : data_tables_columns)
        {
            const auto data_table_id_column = data_table_columns->tryGetPhysical(TimeSeriesColumnNames::ID);
            if (!data_table_id_column || (data_table_id_column->type->getName() != table_id_type->getName()))
                return false;
        }
        return true;
    }

    /// Checks if `metric_id_range` contains ids of a single metric.
    /// I.e. if [ids.min, ids.max] id range contains no ids not from `ids_tmp_table_name` aka ids selected by `lookupTags`.
    /// Time ranges table is queried if available, otherwise tags table is queried.
    bool idRangeHoldsOnlySelectedSeries(
        const TimeSeriesIDRange & metric_id_range,
        bool name_matcher_only,
        const LookedUpIDs & ids,
        const String & ids_tmp_table_name,
        const DataTypePtr & table_timestamp_type,
        const StorageID & tags_table_id,
        const SeriesTimeFilter & time_filter,
        const ContextPtr & context,
        const LoggerPtr & log)
    {
        chassert(ids.range);

        const auto id_first_component = evaluateConstantExpression(metric_id_range.first_component, context).first;
        /// The first component of every selected id.
        if ((ids.range->min.safeGet<Tuple>().at(0) != id_first_component)
            || (ids.range->max.safeGet<Tuple>().at(0) != id_first_component))
            return false;

        /// Without other matchers the lookup selected every series of the metric with samples in the time range.
        if (name_matcher_only)
            return true;

        /// The check costs a query on the time ranges/tags table, while the exclusion search with a small set is cheap.
        if (ids.count < context->getSettingsRef()[Setting::time_series_id_range_check_min_series])
            return false;

        /// The check finds one series of the metric with samples in the time range which has its id in the range but
        /// wasn't selected:
        ///
        ///     SELECT 1 FROM [time_ranges_table | tags_table]
        ///     WHERE [metric_name = <m>
        ///       AND] id >= <min> AND id <= <max>
        ///       [AND max_time >= <min_time> AND min_time <= <max_time>]
        ///       AND id NOT IN _time_series_selector_ids_<hash>
        ///     LIMIT 1
        ///
        /// A series inserted after the check gets its id from the generator whose first component depends
        /// on a metric name alone: a series of another metric can't get into the range, and one of this metric
        /// would only make the read include its granules.
        /// The `id IN <set>` condition keeps doing the exact row-level filtering either way.

        auto select_query = make_intrusive<ASTSelectQuery>();

        /// SELECT 1
        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            select_list_exp->children.push_back(make_intrusive<ASTLiteral>(UInt64{1}));
            select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
        }

        const bool query_tags_table = !time_filter || time_filter.table_id == tags_table_id;
        /// FROM [time_ranges_table|tags_table]
        {
            const auto & source_table = query_tags_table ? tags_table_id : time_filter.table_id;

            select_query->setExpression(
                ASTSelectQuery::Expression::TABLES,
                makeTablesInSelectQuery(make_intrusive<ASTTableIdentifier>(source_table)));
        }

        ASTs conditions;
        conditions.reserve(2 + (query_tags_table ? 1 : 0) + (time_filter ? 2 : 0));

        /// metric_name = <m>
        if (query_tags_table)
            conditions.emplace_back(makeASTFunction(
                "equals",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName),
                make_intrusive<ASTLiteral>(metric_id_range.metric_name)));
        /// id >= <min> AND id <= <max>
        conditions.emplace_back(makeIDRangeCondition(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID), ids));
        /// max_time >= <min_time> AND min_time <= <max_time>
        if (time_filter)
            insertAtEnd(conditions, makeTimeRangeConditions(time_filter.range, table_timestamp_type));
        /// id NOT IN _time_series_selector_ids_<hash>
        conditions.emplace_back(
            makeASTFunction(
                "notIn",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID),
                make_intrusive<ASTIdentifier>(ids_tmp_table_name)));

        select_query->setExpression(ASTSelectQuery::Expression::WHERE, makeASTForLogicalAnd(std::move(conditions)));
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, make_intrusive<ASTLiteral>(UInt64{1}));

        LOG_DEBUG(log, "Checking whether the id range of {} series of metric {} holds only them: {}",
                  ids.count, quoteString(metric_id_range.metric_name), select_query->formatForLogging());

        InterpreterSelectQueryAnalyzer interpreter(select_query, context, SelectQueryOptions{});
        auto io = interpreter.execute();
        PullingAsyncPipelineExecutor executor(io.pipeline);
        Block block;
        while (executor.pull(block))
        {
            if (block.rows() > 0)
                return false;
        }
        return true;
    }

    /// Adds the step storing the tags on top of the plan of the lookup query. The step is added after the interpreter built the plan,
    /// i.e. above any read from remote replicas, so the tags are stored on the initiator, unlike `timeSeriesStoreTags` written in SQL.
    void addLookUpTagsStep(
        QueryPlan & query_plan,
        const size_t tags_start_index,
        const std::shared_ptr<ContextTimeSeriesTagsCollector> & tags_collector)
    {
        const auto & header = query_plan.getCurrentHeader();
        try
        {
            TimeSeriesTagsFunctionHelpers::checkArgumentTypeForID(StoreLookedUpTagsStep::FUNCTION_NAME, header->getColumnsWithTypeAndName(), 0);
            TimeSeriesTagsFunctionHelpers::checkArgumentTypesForTagNamesAndValues(
                StoreLookedUpTagsStep::FUNCTION_NAME, header->getColumnsWithTypeAndName(), tags_start_index);
        }
        catch (Exception & e)
        {
            e.addMessage(" while building the tags look up query. It's a bug, please report it.");

            throw;
        }

        query_plan.addStep(std::make_unique<StoreLookedUpTagsStep>(header, tags_start_index, tags_collector));
    }

    /// Selects the series matching the matchers from the tags table, stores their tags into the tags collector of the query
    /// context and stores selected ids in a temporary table registered in `context`. Tracks the range of the ids if
    /// `track_range` is set. Reads the kinds of samples of the series if `read_sample_kinds` is set, otherwise any kind may be present.
    LookedUpIDs lookUpTags(
        const StorageID & tags_table_id,
        const DataTypePtr & tags_table_id_type,
        const PrometheusQueryTree::MatcherList & matchers,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        const SeriesTimeFilter & time_filter,
        const std::optional<TimeSeriesIDRanges> & metric_id_ranges,
        const DataTypePtr & table_timestamp_type,
        const StorageLimitsList & storage_limits,
        const bool track_range,
        bool read_sample_kinds,
        const ContextMutablePtr & context,
        const LoggerPtr & log)
    {
        auto tags_source = makeTagsTableSource(
            tags_table_id,
            matchers,
            column_name_by_tag_name,
            time_filter,
            metric_id_ranges,
            table_timestamp_type,
            read_sample_kinds);

        /// SELECT _CAST(id, '<id type without LowCardinality>') AS id,
        ///     [<sample_kinds_or_all>,]
        ///     tags, '__name__', metric_name, 'tag_name1', tag_column1, ...
        /// FROM <...> WHERE <...>
        ASTs select_list;
        read_sample_kinds = tags_source.has_sample_kinds;
        auto tag_names_and_values = makeTagNamesAndValuesArguments(column_name_by_tag_name);
        select_list.reserve(1 + tag_names_and_values.size() + (read_sample_kinds ? 1 : 0));
        select_list.push_back(makeASTFunction(
            "_CAST",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID),
            make_intrusive<ASTLiteral>(recursiveRemoveLowCardinality(tags_table_id_type)->getName())));
        select_list.back()->setAlias(TimeSeriesColumnNames::ID);

        const auto sample_kinds_index = select_list.size();
        if (read_sample_kinds)
        {
            select_list.push_back(makeSampleKindsOrAllExpression());
        }

        const size_t tags_start_index = select_list.size();
        insertAtEnd(select_list, std::move(tag_names_and_values));

        const auto select_query = addProjectionToSelectQuery(std::move(tags_source.query), std::move(select_list));

        /// `TemporaryTableHolder` keeps a weak pointer to its context and needs it to drop the table. The holder lives as long
        /// as `context`, which the query plan keeps, so it gets the query context, which outlives the plan.
        const auto query_context = context->getQueryContext();

        LOG_DEBUG(log, "Looking up the tags: {}", select_query->formatForLogging());
        InterpreterSelectQueryAnalyzer interpreter(select_query, context, SelectQueryOptions{});
        interpreter.addStorageLimits(storage_limits);
        addLookUpTagsStep(interpreter.getQueryPlan(), tags_start_index, query_context->getTimeSeriesTagsCollector());
        auto io = interpreter.execute();
        io.pipeline.setProcessListElement(context->getProcessListElement());

        /// The step storing the tags leaves the id and the kinds of samples.
        const auto header = io.pipeline.getHeader();
        const auto & header_columns = header.getColumnsWithTypeAndName();
        const auto & id_column = header_columns.front();
        auto ids_tmp_table = std::make_shared<TemporaryTableHolder>(
            query_context,
            ColumnsDescription{NamesAndTypesList{{TimeSeriesColumnNames::ID, id_column.type}}},
            ConstraintsDescription{},
            /* query = */ nullptr,
            /* create_for_global_subquery = */ true);

        const auto table = ids_tmp_table->getTable();
        const auto table_metadata = table->getInMemoryMetadataPtr(context, false);
        QueryPipeline insert_pipeline(table->write(/* query = */ nullptr, table_metadata, context, /* async_insert = */ false));
        PushingPipelineExecutor insert_executor(insert_pipeline);
        insert_executor.start();

        /// The table is registered under a name depending only on the lookup query, so a selector gets the same data query plan
        /// in every query: the cache of hash table sizes, which join reordering uses as row estimates, is keyed by the plan,
        /// including the name in `id IN <temp_table>`. The name of the table itself can't be used: it's random, because the table
        /// lives in the database of temporary tables shared by all queries.
        const auto lookup_hash = select_query->getTreeHash(/* ignore_aliases = */ true);
        LookedUpIDs res{
            .registered_table_name = fmt::format("_time_series_selector_ids_{}_{}", lookup_hash.low64, lookup_hash.high64),
            .count = 0,
            .type = id_column.type,
            .sample_kinds = read_sample_kinds ? TimeSeriesSampleKinds::None : TimeSeriesSampleKinds::All,
            .range = {}};
        ColumnPtr min_id;
        ColumnPtr max_id;

        PullingAsyncPipelineExecutor executor(io.pipeline);
        Block block;
        while (executor.pull(block))
        {
            if (!block.rows())
                continue;

            const auto & columns = block.getColumnsWithTypeAndName();
            const auto & ids = columns.front();

            if (track_range)
                updateMinMax(*ids.column->convertToFullColumnIfConst(), min_id, max_id);
            res.count += block.rows();

            if (read_sample_kinds)
            {
                const auto sample_kinds_column = columns[sample_kinds_index].column->convertToFullIfWrapped();

                const auto & sample_kinds = assert_cast<const ColumnUInt8 &>(*sample_kinds_column).getData();
                for (const UInt8 kinds : sample_kinds)
                    res.sample_kinds |= kinds;
            }

            insert_executor.push(Block{{ids.column, ids.type, TimeSeriesColumnNames::ID}});
        }
        insert_executor.finish();

        context->addExternalTable(res.registered_table_name, std::move(ids_tmp_table));

        if (track_range && res.count)
            res.range = IDRange{.min = (*min_id)[0], .max = (*max_id)[0]};

        return res;
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
    const auto series_time_filter = makeSeriesTimeFilter(
        tags_table_id,
        makeTableTimeRange(table_timestamp_type, min_time, max_time, time_scale),
        time_ranges_table_id,
        time_series_settings);
    boost::intrusive_ptr<ASTSelectQuery> select_query;
    ASTs select_list;
    if (series_time_filter.range.containsNoTimestamps())
    {
        select_query = makeSelectNoIDsQuery(table_id_type, TimeSeriesColumnNames::ID);
        select_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
    }
    else
    {
        auto tags_table_metadata = time_series_storage.getTargetTable(ViewTarget::Tags, context)->getInMemoryMetadataPtr(context, false);
        const auto metric_id_ranges = tryGetTimeSeriesIDRanges(
            matchers, getIDGenerator(time_series_settings, tags_table_metadata->getColumns(), table_id_type), table_id_type);
        const auto column_name_by_tag_name = makeColumnNameByTagNameMap(time_series_settings);
        auto tags_source = makeTagsTableSource(
            tags_table_id,
            matchers,
            column_name_by_tag_name,
            series_time_filter,
            metric_id_ranges,
            table_timestamp_type,
            /* with_sample_kinds = */ false);
        select_query = std::move(tags_source.query);

        select_list.emplace_back(makeStoreTagsFunction(column_name_by_tag_name));
    }

    chassert(select_list.size() == 1);
    /// Alias the returned expression so callers can reference the column by a fixed name.
    select_list.back()->setAlias("series_id");

    return addProjectionToSelectQuery(std::move(select_query), std::move(select_list));
}


void StorageTimeSeriesSelector::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
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

    /// Histograms are always read from the histograms table: there is no recent tier for them.
    std::optional<StorageID> histograms_table_id;
    if (config.with_histograms)
        histograms_table_id = time_series_storage->getTargetTableID(ViewTarget::Histograms, context);

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
    const auto time_filter = makeSeriesTimeFilter(tags_table_id, table_time_range, time_ranges_table_id, *time_series_settings);

    auto modified_context = Context::createCopy(context);
    ContextPtr interpreter_context = modified_context;
    applySettingsForSelectIDsQuery(*modified_context);

    if (!context->getSettingsRef().isChanged("merge_tree_min_bytes_for_concurrent_read"))
        modified_context->setSetting("merge_tree_min_bytes_for_concurrent_read", UInt64{4 * 1024 * 1024});

    if (!context->getSettingsRef().isChanged("merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem"))
        modified_context->setSetting("merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem", UInt64{4 * 1024 * 1024});

    auto tags_table_metadata = time_series_storage->getTargetTable(ViewTarget::Tags, context)->getInMemoryMetadataPtr(context, false);
    auto samples_table_metadata = time_series_storage->getTargetTable(samples_table_kind, context)->getInMemoryMetadataPtr(context, false);

    std::vector<const ColumnsDescription *> data_tables_columns{&samples_table_metadata->getColumns()};
    StorageMetadataHandle histograms_table_metadata;
    if (histograms_table_id)
    {
        histograms_table_metadata = time_series_storage->getTargetTable(ViewTarget::Histograms, context)->getInMemoryMetadataPtr(context, false);
        data_tables_columns.push_back(&histograms_table_metadata->getColumns());
    }

    const auto metric_id_ranges = tryGetTimeSeriesIDRanges(
        matchers, getIDGenerator(*time_series_settings, tags_table_metadata->getColumns(), config.table_id_type), config.table_id_type);

    const bool has_id_range_candidate = metric_id_ranges.has_value()
        && metric_id_ranges->size() == 1
        && dataTablesStoreIDType(data_tables_columns, config.table_id_type);

    const auto ids = lookUpTags(
        tags_table_id,
        tags_table_metadata->getColumns().getPhysical(TimeSeriesColumnNames::ID).type,
        matchers,
        column_name_by_tag_name,
        time_filter,
        metric_id_ranges,
        config.table_timestamp_type,
        *query_info.storage_limits,
        /* track_range = */ has_id_range_candidate,
        /* read_sample_kinds = */ histograms_table_id && TimeSeriesSampleKinds::isStored(*time_series_settings),
        modified_context,
        log);

    /// Only the data tables with the kinds of samples of the selected series are read.
    const bool read_samples_table = (ids.sample_kinds & TimeSeriesSampleKinds::Float) != 0;
    const bool read_histograms_table = histograms_table_id && ((ids.sample_kinds & TimeSeriesSampleKinds::Histogram) != 0);

    LOG_DEBUG(log, "Collected the tags of {} series for selector {}, reading the samples table: {}, reading the histograms table: {}",
              ids.count, quoteString(config.selector.toString()), read_samples_table, read_histograms_table);

    if ((ids.count == 0) || (!read_samples_table && !read_histograms_table))
    {
        /// No series match the selector, or they have no samples. Nothing is read - an uninitialized query plan reads from
        /// an empty source.
        return;
    }

    /// The data tables are filtered by `id IN <ids_tmp_table>`, and replicas reading them receive the table as an external one.
    const String & ids_tmp_table_name = ids.registered_table_name;

    /// The conditions on the data tables are ordered by `makeWhereFilterForDataTable`. Estimating their selectivity from
    /// statistics converts the whole id set into ranges, which costs more than the rest of the planning.
    if (!context->getSettingsRef().isChanged("use_statistics"))
        modified_context->setSetting("use_statistics", false);

    /// Checks whether the range [min, max] of the selected ids holds the samples of no other series:
    /// then it selects the same granules of the data tables as the id set, whose generic exclusion search
    /// in primary-key index analysis is expensive for a large set, and the range replaces the set in that analysis.
    /// Requires:
    /// 1. The metric's first component in the minimum and the maximum id,
    ///    hence in every selected id (ids written before an ALTER ... MODIFY SETTING id_generator may differ).
    /// 2. No series with samples in the time range and an id in the range outside the selected set:
    ///    true without other matchers, otherwise checked by a query on the time ranges table (or the tags table
    ///    when it stores the time ranges).
    const bool use_id_range = has_id_range_candidate
        && idRangeHoldsOnlySelectedSeries(
            metric_id_ranges->front(),
            /* name_matcher_only = */ matchers.size() == 1,
            ids,
            ids_tmp_table_name,
            config.table_timestamp_type,
            tags_table_id,
            time_filter,
            modified_context,
            log);

    if (use_id_range)
    {
        /// The `id IN <ids_tmp_table>` condition stays in the WHERE for exact row-level filtering, but its set must not
        /// enter primary-key index analysis: `KeyCondition` runs a generic exclusion search with the whole set, which is
        /// expensive for a large set, while the range selects the same granules through the cheap continuous-range path.
        /// Setting `use_index_for_in_with_subqueries_max_values = 1` makes the set unusable for index analysis without
        /// affecting the row-level filter.
        modified_context->setSetting("use_index_for_in_with_subqueries_max_values", UInt64{1});
        LOG_DEBUG(log, "The id range of selector {} holds only the selected series: excluding the id set from index analysis",
                  quoteString(config.selector.toString()));
    }

    const auto make_id_range_conditions = [&](const StorageID & data_table_id)
    {
        return use_id_range ? ASTs{makeIDRangeConditionForDataTable(ids, data_table_id)} : ASTs{};
    };

    /// The casts of the final query produce exactly the columns of this storage.
    const auto columns = storage_snapshot->metadata->getColumns().getAllPhysical();

    /// The float samples and the histogram samples are read by a UNION ALL of two selects if both are present. With one kind
    /// the column of the other kind is a constant: an empty `histogram` for the float samples, a zero `value` for the histograms.
    ASTs selects;
    selects.reserve((read_samples_table ? 1 : 0) + (read_histograms_table ? 1 : 0));
    if (read_samples_table)
    {
        selects.push_back(makeSelectQueryFromDataTable(
            samples_table_id,
            makeSamplesProjection(/* with_histograms = */ histograms_table_id.has_value()),
            ids_tmp_table_name,
            time_range_to_filter_samples,
            config.table_timestamp_type,
            make_id_range_conditions(samples_table_id)));
    }
    if (read_histograms_table)
    {
        selects.push_back(makeSelectQueryFromDataTable(
            *histograms_table_id,
            makeHistogramsProjection(config.table_value_type),
            ids_tmp_table_name,
            time_range_to_filter_samples,
            config.table_timestamp_type,
            make_id_range_conditions(*histograms_table_id)));
    }
    const ASTPtr select_query = makeSelectQuery(makeSelectWithUnionQuery(std::move(selects)), columns);

    LOG_DEBUG(log, "Building SQL for selector: {}", config.selector.toString());
    LOG_DEBUG(log, "Will execute query:\n{}", select_query->formatForLogging());

    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);

    InterpreterSelectQueryAnalyzer interpreter(select_query, interpreter_context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    query_plan = std::move(interpreter).extractQueryPlan();

    /// The plan keeps the temporary table with the ids.
    query_plan.addInterpreterContext(modified_context);
}

}
