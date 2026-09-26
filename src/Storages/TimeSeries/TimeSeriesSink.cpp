#include <Storages/TimeSeries/TimeSeriesSink.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/addMissingDefaults.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesActiveSeriesCache.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesIDGenerator.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <base/EnumReflection.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <chrono>
#include <ranges>


namespace DB
{

namespace Setting
{
extern const SettingsString insert_deduplication_token;
}

namespace TimeSeriesSetting
{
extern const TimeSeriesSettingsASTFunction id_generator;
extern const TimeSeriesSettingsBool store_min_time_and_max_time;
extern const TimeSeriesSettingsMap tags_to_columns;
}

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TIME_SERIES_TAGS;
extern const int INCORRECT_DATA;
}


namespace
{
/// Fills tag columns for the "tags" table by iterating over the columns metric_name and tags.
void fillTagsColumns(
    const PaddedPODArray<UInt8> & filter,
    const IColumn & metric_name_column,
    const ColumnArray::Offsets & tags_offsets,
    const IColumn & tags_keys,
    const IColumn & tags_values,
    IColumn & out_tags_names,
    IColumn & out_tags_values,
    IColumn & out_tags_offsets,
    std::unordered_map<std::string_view, IColumn *> & columns_by_tag_name)
{
    std::vector<std::pair<std::string_view, std::string_view>> sorted_tags;

    for (size_t i = 0; i < filter.size(); ++i)
    {
        if (!filter[i])
            continue;

        sorted_tags.clear();
        size_t tags_start = (i == 0) ? 0 : tags_offsets[i - 1];
        size_t tags_end = tags_offsets[i];
        sorted_tags.reserve(tags_end - tags_start + 1);
        for (size_t j = tags_start; j < tags_end; ++j)
            sorted_tags.emplace_back(tags_keys.getDataAt(j), tags_values.getDataAt(j));
        std::string_view metric_name_sv = metric_name_column.getDataAt(i);
        if (!metric_name_sv.empty())
            sorted_tags.emplace_back(TimeSeriesTagNames::MetricName, metric_name_sv);

        TimeSeriesSink::sortTagsAndRemoveDuplicates(sorted_tags);

        TimeSeriesSink::insertSortedTagsToColumns(sorted_tags, out_tags_names, out_tags_values, out_tags_offsets, columns_by_tag_name);
    }
}

/// Returns the minimum and maximum values in a range in a column.
std::pair<Field, Field> findMinMax(const IColumn & column, size_t start, size_t end)
{
    chassert(start < end);
    Field min_value;
    column.get(start, min_value);
    Field max_value = min_value;
    for (size_t j = start + 1; j < end; ++j)
    {
        Field value;
        column.get(j, value);
        if (value < min_value)
            min_value = value;
        if (value > max_value)
            max_value = value;
    }
    return {min_value, max_value};
}

/// Fills columns min_time and max_time for the "tags" table.
void fillMinMaxTimeColumns(
    const PaddedPODArray<UInt8> & filter,
    const ColumnArray::Offsets & ts_offsets,
    const IColumn & ts_timestamps,
    IColumn & out_min_time_column,
    IColumn & out_max_time_column)
{
    for (size_t i = 0; i < filter.size(); ++i)
    {
        if (!filter[i])
            continue;

        size_t ts_start = (i == 0) ? 0 : ts_offsets[i - 1];
        size_t ts_end = ts_offsets[i];

        if (ts_start == ts_end)
        {
            out_min_time_column.insertDefault();
            out_max_time_column.insertDefault();
            continue;
        }

        auto [min_time, max_time] = findMinMax(ts_timestamps, ts_start, ts_end);
        out_min_time_column.insert(min_time);
        out_max_time_column.insert(max_time);
    }
}

/// Fills columns id, timestamp, value for the "samples" table.
void fillSamplesColumns(
    const PaddedPODArray<UInt8> & filter,
    const IColumn & id_column,
    const IColumn & ts_timestamps,
    const IColumn & ts_values,
    const ColumnArray::Offsets & ts_offsets,
    IColumn & out_id_column,
    IColumn & out_timestamp_column,
    IColumn & out_value_column)
{
    size_t id_index = 0;
    for (size_t i = 0; i < filter.size(); ++i)
    {
        size_t ts_start = (i == 0) ? 0 : ts_offsets[i - 1];
        size_t ts_end = ts_offsets[i];
        size_t num_samples = ts_end - ts_start;

        if (!filter[i])
        {
            if (num_samples > 0)
            {
                /// We can't store time series without metric name and tags.
                throw Exception(ErrorCodes::INCORRECT_DATA, "Got {} samples without a metric name or tags", num_samples);
            }
            continue;
        }

        if (num_samples > 0)
        {
            out_id_column.insertManyFrom(id_column, id_index, num_samples);
            out_timestamp_column.insertRangeFrom(ts_timestamps, ts_start, num_samples);
            out_value_column.insertRangeFrom(ts_values, ts_start, num_samples);
        }

        ++id_index;
    }
}

/// Fills the columns of the "metric families" table: the name of a metric family, type, unit, help.
void fillMetricFamiliesColumns(
    const IColumn & metric_family_column,
    const IColumn & type_column,
    const IColumn & unit_column,
    const IColumn & help_column,
    IColumn & out_metric_family_column,
    IColumn & out_type_column,
    IColumn & out_unit_column,
    IColumn & out_help_column)
{
    for (size_t i = 0; i < metric_family_column.size(); ++i)
    {
        if (metric_family_column.getDataAt(i).empty())
        {
            if (!type_column.getDataAt(i).empty())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Got non-empty type without a metric family");
            if (!unit_column.getDataAt(i).empty())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Got non-empty unit without a metric family");
            if (!help_column.getDataAt(i).empty())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Got non-empty help without a metric family");
            continue;
        }

        out_metric_family_column.insertFrom(metric_family_column, i);
        out_type_column.insertFrom(type_column, i);
        out_unit_column.insertFrom(unit_column, i);
        out_help_column.insertFrom(help_column, i);
    }
}

/// Fills `filter` with 1 for rows having a non-empty metric name or tag, returning count.
/// Leaves `filter` empty if there are no such rows.
size_t
buildNonEmptyTagsFilter(const IColumn & metric_name_column, const ColumnArray::Offsets & tags_offsets, PaddedPODArray<UInt8> & filter)
{
    filter.clear();
    size_t count = 0;

    size_t num_rows = metric_name_column.size();
    chassert(tags_offsets.size() == num_rows);
    if (!num_rows)
        return 0;

    auto set_filter = [&](size_t i)
    {
        if (filter.empty())
            filter.resize_fill(num_rows);
        if (!filter[i])
        {
            filter[i] = 1;
            ++count;
        }
    };

    for (size_t i = 0; i != num_rows; ++i)
        if (!metric_name_column.getDataAt(i).empty())
            set_filter(i);

    if (tags_offsets.back() != 0)
    {
        for (size_t i = 0; i != num_rows; ++i)
        {
            size_t start = (i == 0) ? 0 : tags_offsets[i - 1];
            if (tags_offsets[i] > start)
                set_filter(i);
        }
    }

    return count;
}

/// Returns the total number of samples in the outer column with samples across all rows.
size_t getTotalSamples(const ColumnArray::Offsets & ts_offsets)
{
    return ts_offsets.empty() ? 0 : ts_offsets.back();
}

/// Returns true if the column has at least one non-empty string value.
bool hasNonEmptyValue(const IColumn & column)
{
    for (size_t i = 0; i < column.size(); ++i)
        if (!column.getDataAt(i).empty())
            return true;
    return false;
}

}


void TimeSeriesSink::sortTagsAndRemoveDuplicates(std::vector<std::pair<std::string_view, std::string_view>> & tags)
{
    std::sort(tags.begin(), tags.end());
    tags.erase(std::unique(tags.begin(), tags.end()), tags.end());

    /// After lexicographic sort, if there is a tag with an empty name it should be first.
    if (!tags.empty() && tags.front().first.empty())
        throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS, "Tag name must not be empty");

    std::erase_if(tags, [](const auto & x) { return x.second.empty(); });

    auto adjacent
        = std::adjacent_find(tags.begin(), tags.end(), [](const auto & left, const auto & right) { return left.first == right.first; });
    if (adjacent != tags.end())
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TIME_SERIES_TAGS,
            "Found two tags with the same name {} but different values {} and {}",
            adjacent->first,
            adjacent->second,
            std::next(adjacent)->second);
    }

    auto it = std::lower_bound(
        tags.begin(), tags.end(), TimeSeriesTagNames::MetricName, [](const auto & tag, const char * name) { return tag.first < name; });
    if (it == tags.end() || it->first != TimeSeriesTagNames::MetricName)
        throw Exception(
            ErrorCodes::ILLEGAL_TIME_SERIES_TAGS,
            "Metric name is missing: the `metric_name` column is empty and there is no `{}` tag with a non-empty value",
            TimeSeriesTagNames::MetricName);
}


void TimeSeriesSink::insertSortedTagsToColumns(
    const std::vector<std::pair<std::string_view, std::string_view>> & sorted_tags,
    IColumn & out_tags_names,
    IColumn & out_tags_values,
    IColumn & out_tags_offsets,
    std::unordered_map<std::string_view, IColumn *> & columns_by_tag_name)
{
    for (const auto & [tag_name, tag_value] : sorted_tags)
    {
        auto it = columns_by_tag_name.find(tag_name);
        if (it != columns_by_tag_name.end())
            it->second->insertData(tag_value.data(), tag_value.size());

        /// The "tags" column gets all the tags, including the metric name and the tags
        /// which are also stored in dedicated columns.
        out_tags_names.insertData(tag_name.data(), tag_name.size());
        out_tags_values.insertData(tag_value.data(), tag_value.size());
    }

    out_tags_offsets.insert(out_tags_names.size());

    /// For named-tag columns that had no matching tag in this row, insert the default value.
    size_t expected_num_rows = out_tags_offsets.size();

    for (IColumn * column : std::views::values(columns_by_tag_name))
    {
        if (column->size() < expected_num_rows)
            column->insertDefault();
    }
}


void TimeSeriesSink::TargetPipeline::push(Block block) const
{
    converting_actions->execute(block);
    executor->push(std::move(block));
}

TimeSeriesSink::TargetPipeline::~TargetPipeline()
{
    /// Cancel uncompleted executor on cancellation without exception
    /// so ~PushingPipelineExecutor finished-or-unwinding invariant holds.
    if (executor)
    {
        try
        {
            executor->cancel();
        }
        catch (...)
        {
            tryLogCurrentException("TimeSeriesSink");
        }
    }
}


ColumnPtr TimeSeriesSink::calculateId(const Block & tags_block) const
{
    Block block = tags_block;
    calculate_id_actions->execute(block);
    convert_id_actions->execute(block);
    return block.getByName(TimeSeriesColumnNames::ID).column;
}


std::unique_ptr<TimeSeriesSink::TargetPipeline> TimeSeriesSink::createTargetPipeline(ViewTarget::Kind kind, const Block & header)
{
    auto pipeline = std::make_unique<TargetPipeline>();

    const auto & target_table_id = time_series_storage.getTargetTableID(kind, getContext());

    auto insert_query = make_intrusive<ASTInsertQuery>();
    insert_query->table_id = target_table_id;

    auto columns_ast = make_intrusive<ASTExpressionList>();
    for (const auto & name : header.getNames())
        columns_ast->children.emplace_back(make_intrusive<ASTIdentifier>(name));
    insert_query->columns = columns_ast;

    pipeline->context = Context::createCopy(getContext());
    pipeline->context->setCurrentQueryId(fmt::format("{}:{}", getContext()->getCurrentQueryId(), kind));

    /// Reopening a target pipeline restarts its internal block counter. Give each source block a
    /// distinct user token so a deduplicating target does not discard later blocks as retries.
    /// Source block numbers, rather than cache misses, keep these tokens stable when an insert is retried.
    if (kind == ViewTarget::Tags || kind == ViewTarget::TagsMinMax)
    {
        const auto & token = getContext()->getSettingsRef()[Setting::insert_deduplication_token].value;
        if (!token.empty())
            pipeline->context->setSetting("insert_deduplication_token", fmt::format("{}:{}:{}", token, kind, input_block_number));
    }

    InterpreterInsertQuery interpreter(
        insert_query,
        pipeline->context,
        /* allow_materialized= */ true,
        /* no_squash= */ false,
        /* no_destination= */ false,
        async_insert);

    pipeline->io = interpreter.execute();
    pipeline->executor = std::make_unique<PushingPipelineExecutor>(pipeline->io.pipeline);
    pipeline->executor->start();

    /// Precompute converting actions from our source block types to the pipeline's expected types.
    const Block & target_header = pipeline->executor->getHeader();
    auto converting_dag = ActionsDAG::makeConvertingActions(
        header.getColumnsWithTypeAndName(),
        target_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        pipeline->context);
    pipeline->converting_actions
        = std::make_shared<ExpressionActions>(std::move(converting_dag), ExpressionActionsSettings(pipeline->context));

    return pipeline;
}


TimeSeriesSink::TimeSeriesSink(
    StorageTimeSeries & time_series_storage_, const Block & header_, const Names & insert_columns_, ContextPtr context_, bool async_insert_)
    : SinkToStorage(std::make_shared<const Block>(header_))
    , WithContext(context_)
    , time_series_storage(time_series_storage_)
    , time_series_settings(time_series_storage_.getStorageSettings())
    , log(getLogger("TimeSeriesSink"))
    , async_insert(async_insert_)
{
    /// Determine which target tables need pipelines based on the columns mentioned in the INSERT query.
    /// If insert_columns is empty (e.g. INSERT INTO mytable VALUES ...), all columns are being inserted.
    auto is_insert_column = [&](const String & name)
    { return (insert_columns_.empty() || std::find(insert_columns_.begin(), insert_columns_.end(), name) != insert_columns_.end()); };

    insert_tags_and_samples = is_insert_column(TimeSeriesColumnNames::MetricName) || is_insert_column(TimeSeriesColumnNames::Tags)
        || is_insert_column(TimeSeriesColumnNames::getOuterSamples(time_series_storage.getVersion()));

    insert_metric_families = is_insert_column(TimeSeriesColumnNames::MetricFamily) || is_insert_column(TimeSeriesColumnNames::Type)
        || is_insert_column(TimeSeriesColumnNames::Unit) || is_insert_column(TimeSeriesColumnNames::Help);

    if (insert_tags_and_samples)
        initTagsAndSamplesPipelines();

    if (insert_metric_families)
        initMetricFamiliesPipeline();
}


void TimeSeriesSink::consume(Chunk & chunk)
{
    if (!chunk.getNumRows())
        return;

    Block block = getHeader().cloneWithColumns(chunk.getColumns());

    if (insert_tags_and_samples)
        consumeTagsAndSamples(block);

    if (insert_metric_families)
        consumeMetricFamilies(block);

    ++input_block_number;
}


void TimeSeriesSink::initTagsAndSamplesPipelines()
{
    /// Use matching data types for tags/samples headers as consumeTagsAndSamples uses.
    /// Pipeline converting actions handle any final target table type differences.
    auto tags_target = time_series_storage.getTargetTable(ViewTarget::Tags, getContext());
    auto tags_target_metadata = tags_target->getInMemoryMetadataPtr(getContext(), false);
    const auto & settings = *time_series_settings;

    /// Resolve the expression for generating `id`.
    const auto & tags_id_col = tags_target_metadata->columns.get(TimeSeriesColumnNames::ID);
    id_type = tags_id_col.type;
    ASTPtr id_generator = settings[TimeSeriesSetting::id_generator].value;
    if (!id_generator)
        id_generator = tags_id_col.default_desc.expression;
    if (!id_generator)
        id_generator = TimeSeriesIDGenerator::getDefault(id_type, time_series_storage.getStorageID());
    id_generator_uses_all_tags = TimeSeriesIDGenerator::usesAllTags(id_generator);

    /// Build the tags header WITHOUT the "id" column (matches what consume() produces before ID calculation).
    auto metric_name_type = tags_target_metadata->columns.get(TimeSeriesColumnNames::MetricName).type;
    tags_header_before_id.insert(ColumnWithTypeAndName{metric_name_type, TimeSeriesColumnNames::MetricName});

    const Map & tags_to_columns = settings[TimeSeriesSetting::tags_to_columns];
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        auto column_type = tags_target_metadata->columns.get(column_name).type;
        tags_header_before_id.insert(ColumnWithTypeAndName{column_type, column_name});
    }

    auto tags_map_type
        = typeid_cast<std::shared_ptr<const DataTypeMap>>(tags_target_metadata->columns.get(TimeSeriesColumnNames::Tags).type);
    if (!tags_map_type)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` must have a Map type", TimeSeriesColumnNames::Tags);
    tags_header_before_id.insert(ColumnWithTypeAndName{tags_map_type, TimeSeriesColumnNames::Tags});

    if (id_generator_uses_all_tags)
    {
        /// The `all_tags` column always contains the same data as the `tags` column.
        tags_header_before_id.insert(ColumnWithTypeAndName{tags_map_type, TimeSeriesColumnNames::AllTags});
    }

    /// Derive sample types from input chunk because fillSamplesColumns uses insertRangeFrom.
    /// Any remaining differences are handled by converting actions in samples_pipeline.
    const auto * samples_column_name = TimeSeriesColumnNames::getOuterSamples(time_series_storage.getVersion());
    auto [timestamp_type, value_type] = splitTimeSeriesType(getHeader().getByName(samples_column_name).type);

    /// Since version MIN_WITH_SEPARATE_TAGS_MIN_MAX the time range of a time series is stored in its own
    /// target table, which keeps the row of a time series in the "tags" table immutable.
    store_min_max_in_separate_table
        = settings[TimeSeriesSetting::store_min_time_and_max_time] && time_series_storage.hasTarget(ViewTarget::TagsMinMax);

    if (settings[TimeSeriesSetting::store_min_time_and_max_time] && !store_min_max_in_separate_table)
    {
        /// Use Nullable(timestamp_type) matching findMinMax return type for min_max_time.
        /// Any remaining differences are handled by converting actions in tags_pipeline.
        auto min_max_time_type = makeNullable(timestamp_type);
        tags_header_before_id.insert(ColumnWithTypeAndName{min_max_time_type, TimeSeriesColumnNames::MinTime});
        tags_header_before_id.insert(ColumnWithTypeAndName{min_max_time_type, TimeSeriesColumnNames::MaxTime});
    }

    /// Precompute ExpressionActions for calculating the "id" column.
    ColumnDescription id_column_description{TimeSeriesColumnNames::ID, id_type};
    id_column_description.default_desc.kind = ColumnDefaultKind::Default;
    id_column_description.default_desc.expression = id_generator;

    /// A single-column header containing just the "id" column, used to build the ExpressionActions for ID calculation.
    Block id_header;
    id_header.insert(ColumnWithTypeAndName{id_type, TimeSeriesColumnNames::ID});

    /// Evaluates the id_generator expression (e.g. reinterpretAsUUID(sipHash128(tags)))
    /// to compute the "id" column from tags columns.
    auto calculate_id_dag = addMissingDefaults(
        tags_header_before_id, id_header.getNamesAndTypesList(), ColumnsDescription{id_column_description}, getContext());
    auto calculate_id_result_columns = calculate_id_dag.getResultColumns();
    calculate_id_actions = std::make_shared<ExpressionActions>(std::move(calculate_id_dag));

    /// Converts the computed "id" column to the configured id_type.
    auto convert_id_dag = ActionsDAG::makeConvertingActions(
        calculate_id_result_columns, id_header.getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Position, getContext());
    convert_id_actions
        = std::make_shared<ExpressionActions>(std::move(convert_id_dag), ExpressionActionsSettings(getContext(), CompileExpressions::yes));

    /// Build the full tags source header WITH the "id" column (what we push to the pipeline).
    Block tags_header;
    tags_header.insert(ColumnWithTypeAndName{id_type, TimeSeriesColumnNames::ID});
    for (const auto & column : tags_header_before_id)
    {
        /// The "all_tags" column is not a stored column, we may have used it only to calculate "id",
        /// so we don't insert it to the "tags" table.
        if (column.name != TimeSeriesColumnNames::AllTags)
            tags_header.insert(column);
    }

    tags_pipeline = createTargetPipeline(ViewTarget::Tags, tags_header);

    /// Build source header for the min/max time block.
    if (store_min_max_in_separate_table)
    {
        auto min_max_time_type = makeNullable(timestamp_type);
        Block tags_min_max_header;
        tags_min_max_header.insert(ColumnWithTypeAndName{id_type, TimeSeriesColumnNames::ID});
        tags_min_max_header.insert(ColumnWithTypeAndName{metric_name_type, TimeSeriesColumnNames::MetricName});
        tags_min_max_header.insert(ColumnWithTypeAndName{min_max_time_type, TimeSeriesColumnNames::MinTime});
        tags_min_max_header.insert(ColumnWithTypeAndName{min_max_time_type, TimeSeriesColumnNames::MaxTime});
        tags_min_max_pipeline = createTargetPipeline(ViewTarget::TagsMinMax, tags_min_max_header);
    }

    /// Build source header for samples block.
    Block samples_header;
    samples_header.insert(ColumnWithTypeAndName{id_type, TimeSeriesColumnNames::ID});
    samples_header.insert(ColumnWithTypeAndName{timestamp_type, TimeSeriesColumnNames::Timestamp});
    samples_header.insert(ColumnWithTypeAndName{value_type, TimeSeriesColumnNames::Value});
    samples_pipeline = createTargetPipeline(ViewTarget::Samples, samples_header);

    /// The recent samples table (if any) receives a copy of every samples block.
    if (time_series_storage.hasTarget(ViewTarget::RecentSamples))
        recent_samples_pipeline = createTargetPipeline(ViewTarget::RecentSamples, samples_header);
}


void TimeSeriesSink::consumeTagsAndSamples(const Block & block)
{
    /// Step 1. Extract columns from the input block.
    const auto & metric_name_col = block.getByName(TimeSeriesColumnNames::MetricName);
    const auto & tags_col = block.getByName(TimeSeriesColumnNames::Tags);
    const auto & time_series_col = block.getByName(TimeSeriesColumnNames::getOuterSamples(time_series_storage.getVersion()));

    const auto * tags_map_column = typeid_cast<const ColumnMap *>(tags_col.column.get());
    if (!tags_map_column)
        throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS, "Expected ColumnMap for tags column, got {}", tags_col.column->getName());
    const auto & tags_map_nested = tags_map_column->getNestedColumn();
    const ColumnArray::Offsets & tags_offsets = tags_map_nested.getOffsets();

    const auto * ts_arrays = typeid_cast<const ColumnArray *>(time_series_col.column.get());
    if (!ts_arrays)
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Expected ColumnArray for the column `{}`, got {}",
            time_series_col.name,
            time_series_col.column->getName());
    const auto * ts_tuples = typeid_cast<const ColumnTuple *>(&ts_arrays->getData());
    if (!ts_tuples)
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Expected ColumnTuple for the data of the column `{}`, got {}",
            time_series_col.name,
            ts_arrays->getData().getName());
    if (ts_tuples->tupleSize() != 2)
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Expected ColumnTuple with 2 elements for the data of the column `{}`, got {}",
            time_series_col.name,
            ts_tuples->tupleSize());
    const ColumnArray::Offsets & ts_offsets = ts_arrays->getOffsets();
    size_t total_samples = getTotalSamples(ts_offsets);

    PaddedPODArray<UInt8> filter;
    size_t num_time_series = buildNonEmptyTagsFilter(*metric_name_col.column, tags_offsets, filter);

    if (!num_time_series)
    {
        /// All rows have empty metric names and no tags - samples must also be empty.
        if (total_samples)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Got {} samples without a metric name or tags", total_samples);

        /// Nothing to insert.
        return;
    }

    const auto & tags_tuple = assert_cast<const ColumnTuple &>(tags_map_nested.getData());
    chassert(tags_tuple.tupleSize() == 2);
    const IColumn & tags_keys = tags_tuple.getColumn(0);
    const IColumn & tags_values = tags_tuple.getColumn(1);

    const IColumn & ts_timestamps = ts_tuples->getColumn(0);
    const IColumn & ts_values = ts_tuples->getColumn(1);

    /// Step 2. Build columns for the tags block.
    std::vector<std::tuple<String, MutableColumnPtr, DataTypePtr>> columns_by_tag_name_holder;
    std::unordered_map<std::string_view, IColumn *> columns_by_tag_name;

    auto add_tag_column = [&](std::string_view tag_name, const String & column_name)
    {
        auto type = tags_header_before_id.getByName(column_name).type;
        auto column = type->createColumn();
        column->reserve(num_time_series);
        columns_by_tag_name[tag_name] = column.get();
        columns_by_tag_name_holder.emplace_back(column_name, std::move(column), std::move(type));
    };

    add_tag_column(TimeSeriesTagNames::MetricName, TimeSeriesColumnNames::MetricName);

    const auto & settings = *time_series_settings;
    const Map & tags_to_columns = settings[TimeSeriesSetting::tags_to_columns];
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
        add_tag_column(tuple.at(0).safeGet<String>(), tuple.at(1).safeGet<String>());
    }

    auto tags_map_type = typeid_cast<std::shared_ptr<const DataTypeMap>>(tags_header_before_id.getByName(TimeSeriesColumnNames::Tags).type);
    auto new_tags_names = tags_map_type->getKeyType()->createColumn();
    new_tags_names->reserve(num_time_series);
    auto new_tags_values = tags_map_type->getValueType()->createColumn();
    new_tags_values->reserve(num_time_series);
    auto new_tags_offsets = ColumnArray::ColumnOffsets::create();
    new_tags_offsets->reserve(num_time_series);

    fillTagsColumns(
        filter,
        *metric_name_col.column,
        tags_offsets,
        tags_keys,
        tags_values,
        *new_tags_names,
        *new_tags_values,
        *new_tags_offsets,
        columns_by_tag_name);

    auto [timestamp_type, value_type] = splitTimeSeriesType(time_series_col.type);

    /// Optionally fill min_time and max_time columns if enabled in settings.
    /// They go either to the tags block or to the min/max time block, so they are kept immutable here.
    ColumnPtr min_time_column;
    ColumnPtr max_time_column;
    DataTypePtr min_max_time_type;
    if (settings[TimeSeriesSetting::store_min_time_and_max_time])
    {
        min_max_time_type = makeNullable(timestamp_type);
        auto new_min_time_column = min_max_time_type->createColumn();
        auto new_max_time_column = min_max_time_type->createColumn();
        new_min_time_column->reserve(num_time_series);
        new_max_time_column->reserve(num_time_series);
        fillMinMaxTimeColumns(filter, ts_offsets, ts_timestamps, *new_min_time_column, *new_max_time_column);
        min_time_column = std::move(new_min_time_column);
        max_time_column = std::move(new_max_time_column);
    }

    /// Step 3. Assemble the tags block.
    Block tags_block;
    for (auto & [column_name, column, type] : columns_by_tag_name_holder)
        tags_block.insert(ColumnWithTypeAndName{std::move(column), type, column_name});

    Columns new_tags_tuples_columns;
    new_tags_tuples_columns.push_back(std::move(new_tags_names));
    new_tags_tuples_columns.push_back(std::move(new_tags_values));
    ColumnPtr new_tags_column
        = ColumnMap::create(ColumnArray::create(ColumnTuple::create(std::move(new_tags_tuples_columns)), std::move(new_tags_offsets)));
    tags_block.insert(ColumnWithTypeAndName{new_tags_column, tags_map_type, TimeSeriesColumnNames::Tags});

    if (id_generator_uses_all_tags)
    {
        /// The `all_tags` column contains the same data as the `tags` column.
        tags_block.insert(ColumnWithTypeAndName{new_tags_column, tags_map_type, TimeSeriesColumnNames::AllTags});
    }

    if (min_time_column && !store_min_max_in_separate_table)
    {
        tags_block.insert(ColumnWithTypeAndName{min_time_column, min_max_time_type, TimeSeriesColumnNames::MinTime});
        tags_block.insert(ColumnWithTypeAndName{max_time_column, min_max_time_type, TimeSeriesColumnNames::MaxTime});
    }

    /// Calculate IDs using precomputed ExpressionActions.
    auto id_column = calculateId(tags_block);
    tags_block.insert(0, ColumnWithTypeAndName{id_column, id_type, TimeSeriesColumnNames::ID});

    if (tags_block.has(TimeSeriesColumnNames::AllTags))
        tags_block.erase(TimeSeriesColumnNames::AllTags);

    /// Step 3a. Assemble the min/max time block, one row per time series like the tags block.
    /// It takes `metric_name` before step 4 filters the tags block, which replaces that column.
    Block tags_min_max_block;
    if (store_min_max_in_separate_table)
    {
        tags_min_max_block.insert(ColumnWithTypeAndName{id_column, id_type, TimeSeriesColumnNames::ID});
        tags_min_max_block.insert(tags_block.getByName(TimeSeriesColumnNames::MetricName));
        tags_min_max_block.insert(ColumnWithTypeAndName{min_time_column, min_max_time_type, TimeSeriesColumnNames::MinTime});
        tags_min_max_block.insert(ColumnWithTypeAndName{max_time_column, min_max_time_type, TimeSeriesColumnNames::MaxTime});
    }

    /// Step 4. Push the tags block.
    /// Deduplicate against active series cache and sink-local pending set to skip redundant tag inserts.
    auto active_series_cache = time_series_storage.getActiveSeriesCache();
    if (active_series_cache)
    {
        UInt32 now = static_cast<UInt32>(
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count());

        IColumn::Filter tags_filter;
        size_t tags_to_write = 0;

        active_series_cache->checkBulk(id_column, now, tags_filter, tags_to_write);

        if (tags_to_write > 0)
        {
            size_t num_ids = id_column->size();
            for (size_t i = 0; i < num_ids; ++i)
            {
                if (!tags_filter[i])
                    continue;

                UInt128 id = TimeSeriesActiveSeriesCache::extractId(*id_column, i);
                if (pending_cached_set.has(id))
                {
                    tags_filter[i] = 0;
                    --tags_to_write;
                }
                else
                {
                    pending_cached_set.insert(id);
                    pending_cached_ids.push_back(id);
                }
            }
        }

        if (tags_to_write > 0)
        {
            if (tags_to_write < num_time_series)
            {
                for (size_t col_idx = 0; col_idx < tags_block.columns(); ++col_idx)
                {
                    auto & col = tags_block.getByPosition(col_idx);
                    col.column = col.column->filter(tags_filter, tags_to_write);
                }
            }

            if (!tags_pipeline)
                tags_pipeline = createTargetPipeline(ViewTarget::Tags, tags_block.cloneEmpty());
            tags_pipeline->push(std::move(tags_block));
        }

        /// A push is not a commit barrier: squashing and the target sink can both retain rows until
        /// `finish`. Commit the tags before samples can advance, including when the cache became
        /// enabled after a previous block. A later cache miss creates a new pipeline, while a hit
        /// writes no duplicate rows merely to flush the previous block.
        if (tags_pipeline)
        {
            tags_pipeline->executor->finish();
            tags_pipeline.reset();
        }
    }
    else
    {
        /// Tags are pushed first so that if the samples insert fails,
        /// we don't end up with sample rows referencing IDs that were never written to the tags table.
        if (!tags_pipeline)
            tags_pipeline = createTargetPipeline(ViewTarget::Tags, tags_block.cloneEmpty());
        tags_pipeline->push(std::move(tags_block));
    }

    /// Step 4a. Push the min/max time block. It is pushed for every block, including one whose tags rows
    /// were all skipped above, because the time range of a time series changes with every block.
    if (store_min_max_in_separate_table)
    {
        if (!tags_min_max_pipeline)
            tags_min_max_pipeline = createTargetPipeline(ViewTarget::TagsMinMax, tags_min_max_block.cloneEmpty());
        tags_min_max_pipeline->push(std::move(tags_min_max_block));
        if (active_series_cache)
        {
            /// Cached tags also need committed bounds before their samples become visible to a
            /// time-bounded selector; the bounds pipeline may squash fewer rows than the samples one.
            tags_min_max_pipeline->executor->finish();
            tags_min_max_pipeline.reset();
        }
    }

    /// Step 5. Assemble and push the samples block.
    if (total_samples)
    {
        /// Build columns for the samples block.
        auto samples_id_column = id_type->createColumn();
        samples_id_column->reserve(total_samples);

        auto timestamp_column = timestamp_type->createColumn();
        timestamp_column->reserve(total_samples);

        auto value_column = value_type->createColumn();
        value_column->reserve(total_samples);

        fillSamplesColumns(filter, *id_column, ts_timestamps, ts_values, ts_offsets, *samples_id_column, *timestamp_column, *value_column);

        /// Assemble the block and push it to the "samples" table.
        Block samples_block;
        samples_block.insert(ColumnWithTypeAndName{std::move(samples_id_column), id_type, TimeSeriesColumnNames::ID});
        samples_block.insert(ColumnWithTypeAndName{std::move(timestamp_column), timestamp_type, TimeSeriesColumnNames::Timestamp});
        samples_block.insert(ColumnWithTypeAndName{std::move(value_column), value_type, TimeSeriesColumnNames::Value});

        /// Samples table is written before recent samples to avoid exposing lost data if insert fails.
        /// Block copy is cheap as it only copies column pointers.
        samples_pipeline->push(samples_block);

        if (recent_samples_pipeline)
            recent_samples_pipeline->push(std::move(samples_block));
    }
}


void TimeSeriesSink::initMetricFamiliesPipeline()
{
    /// Use matching data types for metric_families_header as consumeMetricFamilies uses.
    /// Pipeline converting actions handle any final target table differences.

    const Block & header = getHeader();

    Block metric_families_header;
    metric_families_header.insert(ColumnWithTypeAndName{
        header.getByName(TimeSeriesColumnNames::MetricFamily).type, TimeSeriesColumnNames::getInnerMetricFamily(time_series_storage.getVersion())});

    metric_families_header.insert(ColumnWithTypeAndName{header.getByName(TimeSeriesColumnNames::Type).type, TimeSeriesColumnNames::Type});

    metric_families_header.insert(ColumnWithTypeAndName{header.getByName(TimeSeriesColumnNames::Unit).type, TimeSeriesColumnNames::Unit});

    metric_families_header.insert(ColumnWithTypeAndName{header.getByName(TimeSeriesColumnNames::Help).type, TimeSeriesColumnNames::Help});

    metric_families_pipeline = createTargetPipeline(ViewTarget::MetricFamilies, metric_families_header);
}


void TimeSeriesSink::consumeMetricFamilies(const Block & block)
{
    /// Step 1. Extract columns from the input block.
    const auto & metric_family_col = block.getByName(TimeSeriesColumnNames::MetricFamily);
    const auto & type_col = block.getByName(TimeSeriesColumnNames::Type);
    const auto & unit_col = block.getByName(TimeSeriesColumnNames::Unit);
    const auto & help_col = block.getByName(TimeSeriesColumnNames::Help);

    if (!hasNonEmptyValue(*metric_family_col.column))
    {
        /// All metric_family values are empty - type/unit/help must also be empty.
        if (hasNonEmptyValue(*type_col.column))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Got non-empty type without a metric family");

        if (hasNonEmptyValue(*unit_col.column))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Got non-empty unit values without a metric family");

        if (hasNonEmptyValue(*help_col.column))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Got non-empty help values without a metric family");

        /// Nothing to insert.
        return;
    }

    /// Step 2. Build columns for the metric families block, skipping rows with empty metric_family.
    auto new_metric_family_column = metric_family_col.type->createColumn();
    auto new_type_column = type_col.type->createColumn();
    auto new_unit_column = unit_col.type->createColumn();
    auto new_help_column = help_col.type->createColumn();

    fillMetricFamiliesColumns(
        *metric_family_col.column,
        *type_col.column,
        *unit_col.column,
        *help_col.column,
        *new_metric_family_column,
        *new_type_column,
        *new_unit_column,
        *new_help_column);

    /// We've already checked that at least one non-empty `metric_family` is present.
    chassert(!new_metric_family_column->empty());

    /// Step 3. Assemble the block and push it to the "metric families" table.
    Block metric_families_block;
    metric_families_block.insert(ColumnWithTypeAndName{
        std::move(new_metric_family_column), metric_family_col.type, TimeSeriesColumnNames::getInnerMetricFamily(time_series_storage.getVersion())});
    metric_families_block.insert(ColumnWithTypeAndName{std::move(new_type_column), type_col.type, TimeSeriesColumnNames::Type});
    metric_families_block.insert(ColumnWithTypeAndName{std::move(new_unit_column), unit_col.type, TimeSeriesColumnNames::Unit});
    metric_families_block.insert(ColumnWithTypeAndName{std::move(new_help_column), help_col.type, TimeSeriesColumnNames::Help});

    metric_families_pipeline->push(std::move(metric_families_block));
}


void TimeSeriesSink::onFinish()
{
    if (tags_pipeline)
        tags_pipeline->executor->finish();
    if (tags_min_max_pipeline)
        tags_min_max_pipeline->executor->finish();
    if (samples_pipeline)
        samples_pipeline->executor->finish();
    if (recent_samples_pipeline)
        recent_samples_pipeline->executor->finish();
    if (metric_families_pipeline)
        metric_families_pipeline->executor->finish();

    if (!pending_cached_ids.empty())
    {
        if (auto cache = time_series_storage.getActiveSeriesCache())
            cache->commit(pending_cached_ids);
    }
}

}
