#include <Storages/TimeSeries/TimeSeriesSink.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
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
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <base/EnumReflection.h>

#include <algorithm>
#include <ranges>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsDataType id_type;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsBool use_all_tags_column_to_generate_id;
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
        IColumn & out_metric_name_column,
        IColumn & out_tags_names,
        IColumn & out_tags_values,
        IColumn & out_tags_offsets,
        std::unordered_map<std::string_view, IColumn *> & columns_by_tag_name,
        IColumn * all_tags_names,
        IColumn * all_tags_values,
        IColumn * all_tags_offsets)
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

            TimeSeriesSink::insertSortedTagsToColumns(
                sorted_tags,
                out_metric_name_column,
                out_tags_names, out_tags_values, out_tags_offsets,
                columns_by_tag_name,
                all_tags_names, all_tags_values, all_tags_offsets);
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

    /// Fills columns metric_family_name, type, unit, help for the "metrics" table.
    void fillMetricsColumns(
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

    /// Fills `filter` with 1 for rows that have either a non-empty metric name or at least one tag.
    /// Returns the number of such rows.
    /// The function returns 0 and leaves `filter` empty if there are no such rows.
    size_t buildNonEmptyTagsFilter(
        const IColumn & metric_name_column,
        const ColumnArray::Offsets & tags_offsets,
        PaddedPODArray<UInt8> & filter)
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

    /// Returns the total number of samples in the column `time_series` across all rows.
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

    /// Splits a time series type Array(Tuple(timestamp, value)) into its timestamp and scalar component types.
    std::pair<DataTypePtr, DataTypePtr> splitTimeSeriesType(const DataTypePtr & time_series_type)
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(time_series_type.get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected DataTypeArray for the time_series column type, got {}", time_series_type->getName());
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get());
        if (!tuple_type)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected DataTypeTuple as the element type of the time_series column, got {}", array_type->getNestedType()->getName());
        return {tuple_type->getElement(0), tuple_type->getElement(1)};
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

    auto adjacent = std::adjacent_find(tags.begin(), tags.end(),
        [](const auto & left, const auto & right) { return left.first == right.first; });
    if (adjacent != tags.end())
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TIME_SERIES_TAGS,
            "Found two tags with the same name {} but different values {} and {}",
            adjacent->first, adjacent->second, std::next(adjacent)->second);
    }

    auto it = std::lower_bound(tags.begin(), tags.end(), TimeSeriesTagNames::MetricName,
        [](const auto & tag, const char * name) { return tag.first < name; });
    if (it == tags.end() || it->first != TimeSeriesTagNames::MetricName)
        throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS,
            "Metric name is missing: the `metric_name` column is empty and there is no `{}` tag with a non-empty value",
            TimeSeriesTagNames::MetricName);
}


void TimeSeriesSink::insertSortedTagsToColumns(
    const std::vector<std::pair<std::string_view, std::string_view>> & sorted_tags,
    IColumn & out_metric_name_column,
    IColumn & out_tags_names,
    IColumn & out_tags_values,
    IColumn & out_tags_offsets,
    std::unordered_map<std::string_view, IColumn *> & columns_by_tag_name,
    IColumn * all_tags_names,
    IColumn * all_tags_values,
    IColumn * all_tags_offsets)
{
    for (const auto & [tag_name, tag_value] : sorted_tags)
    {
        if (tag_name == TimeSeriesTagNames::MetricName)
        {
            out_metric_name_column.insertData(tag_value.data(), tag_value.size());
        }
        else
        {
            if (all_tags_names)
            {
                all_tags_names->insertData(tag_name.data(), tag_name.size());
                all_tags_values->insertData(tag_value.data(), tag_value.size());
            }

            auto it = columns_by_tag_name.find(tag_name);
            if (it != columns_by_tag_name.end())
            {
                it->second->insertData(tag_value.data(), tag_value.size());
            }
            else
            {
                out_tags_names.insertData(tag_name.data(), tag_name.size());
                out_tags_values.insertData(tag_value.data(), tag_value.size());
            }
        }
    }

    out_tags_offsets.insert(out_tags_names.size());
    if (all_tags_names)
        all_tags_offsets->insert(all_tags_names->size());

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

TimeSeriesSink::TargetPipeline::~TargetPipeline() = default;


ColumnPtr TimeSeriesSink::calculateId(const Block & tags_block) const
{
    Block block = tags_block;
    calculate_id_actions->execute(block);
    convert_id_actions->execute(block);
    return block.getByName(TimeSeriesColumnNames::ID).column;
}


std::unique_ptr<TimeSeriesSink::TargetPipeline> TimeSeriesSink::createTargetPipeline(
    ViewTarget::Kind kind, const Block & header)
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
    pipeline->converting_actions = std::make_shared<ExpressionActions>(
        std::move(converting_dag), ExpressionActionsSettings(pipeline->context));

    return pipeline;
}


TimeSeriesSink::TimeSeriesSink(
    StorageTimeSeries & time_series_storage_,
    const Block & header_,
    const Names & insert_columns_,
    ContextPtr context_,
    bool async_insert_)
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
    {
        return (insert_columns_.empty() || std::find(insert_columns_.begin(), insert_columns_.end(), name) != insert_columns_.end());
    };

    insert_tags_and_samples = is_insert_column(TimeSeriesColumnNames::MetricName)
        || is_insert_column(TimeSeriesColumnNames::Tags)
        || is_insert_column(TimeSeriesColumnNames::TimeSeries);

    insert_metrics = is_insert_column(TimeSeriesColumnNames::MetricFamily)
        || is_insert_column(TimeSeriesColumnNames::Type)
        || is_insert_column(TimeSeriesColumnNames::Unit)
        || is_insert_column(TimeSeriesColumnNames::Help);

    if (insert_tags_and_samples)
        initTagsAndSamplesPipelines();

    if (insert_metrics)
        initMetricsPipeline();
}


void TimeSeriesSink::consume(Chunk & chunk)
{
    if (!chunk.getNumRows())
        return;

    Block block = getHeader().cloneWithColumns(chunk.getColumns());

    if (insert_tags_and_samples)
        consumeTagsAndSamples(block);

    if (insert_metrics)
        consumeMetrics(block);
}


void TimeSeriesSink::initTagsAndSamplesPipelines()
{
    /// It's important to use here for `tags_header` and `samples_header`
    /// the same data types as function consumeTagsAndSamples() uses to push blocks.
    /// There is a conversion step in all the target pipelines, so we don't have to always
    /// match the data types of the columns in the "tags" or "samples" tables.

    /// Build the tags header WITHOUT the "id" column (matches what consume() produces before ID calculation).
    Block tags_header_before_id;

    const auto & settings = *time_series_settings;
    tags_header_before_id.insert(ColumnWithTypeAndName{
        std::make_shared<DataTypeString>(), TimeSeriesColumnNames::MetricName});

    const Map & tags_to_columns = settings[TimeSeriesSetting::tags_to_columns];
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        tags_header_before_id.insert(ColumnWithTypeAndName{std::make_shared<DataTypeString>(), column_name});
    }

    tags_header_before_id.insert(
        ColumnWithTypeAndName{
            std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()),
            TimeSeriesColumnNames::Tags});

    if (settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
    {
        tags_header_before_id.insert(ColumnWithTypeAndName{
            std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()),
            TimeSeriesColumnNames::AllTags});
    }

    auto [timestamp_type, scalar_type] = splitTimeSeriesType(getHeader().getByName(TimeSeriesColumnNames::TimeSeries).type);

    if (settings[TimeSeriesSetting::store_min_time_and_max_time])
    {
        auto min_max_time_type = makeNullable(timestamp_type);
        tags_header_before_id.insert(ColumnWithTypeAndName{min_max_time_type, TimeSeriesColumnNames::MinTime});
        tags_header_before_id.insert(ColumnWithTypeAndName{min_max_time_type, TimeSeriesColumnNames::MaxTime});
    }

    /// Precompute ExpressionActions for calculating the "id" column.
    DataTypePtr id_type = settings[TimeSeriesSetting::id_type];
    ColumnDescription id_column_description{TimeSeriesColumnNames::ID, id_type};
    id_column_description.default_desc.kind = ColumnDefaultKind::Default;
    id_column_description.default_desc.expression = settings[TimeSeriesSetting::id_generator].value;

    /// A single-column header containing just the "id" column, used to build the ExpressionActions for ID calculation.
    Block id_header;
    id_header.insert(ColumnWithTypeAndName{id_type, TimeSeriesColumnNames::ID});

    /// Evaluates the id_generator expression (e.g. reinterpretAsUUID(sipHash128(metric_name, all_tags)))
    /// to compute the "id" column from tags columns.
    auto calculate_id_dag = addMissingDefaults(
        tags_header_before_id,
        id_header.getNamesAndTypesList(),
        ColumnsDescription{id_column_description},
        getContext());
    auto calculate_id_result_columns = calculate_id_dag.getResultColumns();
    calculate_id_actions = std::make_shared<ExpressionActions>(std::move(calculate_id_dag));

    /// Converts the computed "id" column to the configured id_type.
    auto convert_id_dag = ActionsDAG::makeConvertingActions(
        calculate_id_result_columns,
        id_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Position,
        getContext());
    convert_id_actions = std::make_shared<ExpressionActions>(
        std::move(convert_id_dag),
        ExpressionActionsSettings(getContext(), CompileExpressions::yes));

    /// Build the full tags source header WITH the "id" column (what we push to the pipeline).
    Block tags_header;
    tags_header.insert(ColumnWithTypeAndName{id_type, TimeSeriesColumnNames::ID});
    for (const auto & column : tags_header_before_id)
    {
        /// The "all_tags" column in the "tags" table is either ephemeral or doesn't exist.
        /// We've used the "all_tags" column to calculate the "id" column already,
        /// and now we don't need it to insert to the "tags" table.
        if (column.name != TimeSeriesColumnNames::AllTags)
            tags_header.insert(column);
    }

    tags_pipeline = createTargetPipeline(ViewTarget::Tags, tags_header);

    /// Build source header for samples block.
    Block samples_header;
    samples_header.insert(ColumnWithTypeAndName{id_type, TimeSeriesColumnNames::ID});
    samples_header.insert(ColumnWithTypeAndName{timestamp_type, TimeSeriesColumnNames::Timestamp});
    samples_header.insert(ColumnWithTypeAndName{scalar_type, TimeSeriesColumnNames::Value});
    samples_pipeline = createTargetPipeline(ViewTarget::Samples, samples_header);
}


void TimeSeriesSink::consumeTagsAndSamples(const Block & block)
{
    /// Step 1. Extract columns from the input block.
    const auto & metric_name_col = block.getByName(TimeSeriesColumnNames::MetricName);
    const auto & tags_col = block.getByName(TimeSeriesColumnNames::Tags);
    const auto & time_series_col = block.getByName(TimeSeriesColumnNames::TimeSeries);

    const auto * tags_map_column = typeid_cast<const ColumnMap *>(tags_col.column.get());
    if (!tags_map_column)
        throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS, "Expected ColumnMap for tags column, got {}", tags_col.column->getName());
    const auto & tags_map_nested = tags_map_column->getNestedColumn();
    const ColumnArray::Offsets & tags_offsets = tags_map_nested.getOffsets();

    const auto * ts_arrays = typeid_cast<const ColumnArray *>(time_series_col.column.get());
    if (!ts_arrays)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected ColumnArray for the time_series column, got {}", time_series_col.column->getName());
    const auto * ts_tuples = typeid_cast<const ColumnTuple *>(&ts_arrays->getData());
    if (!ts_tuples)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected ColumnTuple for the time_series column data, got {}", ts_arrays->getData().getName());
    if (ts_tuples->tupleSize() != 2)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected ColumnTuple with 2 elements for the time_series column data, got {}", ts_tuples->tupleSize());
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
    auto new_metric_name_column = ColumnString::create();
    new_metric_name_column->reserve(num_time_series);

    std::vector<std::pair<String, MutableColumnPtr>> columns_by_tag_name_holder;
    std::unordered_map<std::string_view, IColumn *> columns_by_tag_name;

    const auto & settings = *time_series_settings;
    const Map & tags_to_columns = settings[TimeSeriesSetting::tags_to_columns];

    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
        const auto & tag_name = tuple.at(0).safeGet<String>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        auto column = ColumnString::create();
        column->reserve(num_time_series);
        columns_by_tag_name[tag_name] = column.get();
        columns_by_tag_name_holder.emplace_back(column_name, std::move(column));
    }

    auto new_tags_names = ColumnString::create();
    new_tags_names->reserve(num_time_series);
    auto new_tags_values = ColumnString::create();
    new_tags_values->reserve(num_time_series);
    auto new_tags_offsets = ColumnArray::ColumnOffsets::create();
    new_tags_offsets->reserve(num_time_series);

    ColumnString::MutablePtr all_tags_names;
    ColumnString::MutablePtr all_tags_values;
    ColumnArray::ColumnOffsets::MutablePtr all_tags_offsets;

    if (settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
    {
        all_tags_names = ColumnString::create();
        all_tags_names->reserve(num_time_series);
        all_tags_values = ColumnString::create();
        all_tags_values->reserve(num_time_series);
        all_tags_offsets = ColumnArray::ColumnOffsets::create();
        all_tags_offsets->reserve(num_time_series);
    }

    fillTagsColumns(
        filter,
        *metric_name_col.column,
        tags_offsets, tags_keys, tags_values,
        *new_metric_name_column,
        *new_tags_names, *new_tags_values, *new_tags_offsets,
        columns_by_tag_name,
        all_tags_names.get(), all_tags_values.get(), all_tags_offsets.get());

    chassert(!new_metric_name_column->empty());

    /// Get timestamp/value types from the time_series array's inner tuple.
    auto [timestamp_type, scalar_type] = splitTimeSeriesType(time_series_col.type);

    /// Optionally fill min_time and max_time columns if enabled in settings.
    MutableColumnPtr min_time_column;
    MutableColumnPtr max_time_column;
    DataTypePtr min_max_time_type;
    if (settings[TimeSeriesSetting::store_min_time_and_max_time])
    {
        min_max_time_type = makeNullable(timestamp_type);
        min_time_column = min_max_time_type->createColumn();
        max_time_column = min_max_time_type->createColumn();
        min_time_column->reserve(num_time_series);
        max_time_column->reserve(num_time_series);
        fillMinMaxTimeColumns(filter, ts_offsets, ts_timestamps, *min_time_column, *max_time_column);
    }

    /// Step 3. Assemble the tags block.
    Block tags_block;
    tags_block.insert(ColumnWithTypeAndName{std::move(new_metric_name_column), std::make_shared<DataTypeString>(), TimeSeriesColumnNames::MetricName});

    for (auto & [column_name, column] : columns_by_tag_name_holder)
        tags_block.insert(ColumnWithTypeAndName{std::move(column), std::make_shared<DataTypeString>(), column_name});

    Columns new_tags_tuples_columns;
    new_tags_tuples_columns.push_back(std::move(new_tags_names));
    new_tags_tuples_columns.push_back(std::move(new_tags_values));
    auto new_tags_column = ColumnMap::create(
        ColumnArray::create(ColumnTuple::create(std::move(new_tags_tuples_columns)), std::move(new_tags_offsets)));
    tags_block.insert(ColumnWithTypeAndName{std::move(new_tags_column), std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()), TimeSeriesColumnNames::Tags});

    if (all_tags_names)
    {
        Columns all_tags_tuple_columns;
        all_tags_tuple_columns.push_back(std::move(all_tags_names));
        all_tags_tuple_columns.push_back(std::move(all_tags_values));
        auto all_tags_column = ColumnMap::create(
            ColumnArray::create(ColumnTuple::create(std::move(all_tags_tuple_columns)), std::move(all_tags_offsets)));
        tags_block.insert(ColumnWithTypeAndName{std::move(all_tags_column), std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()), TimeSeriesColumnNames::AllTags});
    }

    if (min_time_column)
    {
        tags_block.insert(ColumnWithTypeAndName{std::move(min_time_column), min_max_time_type, TimeSeriesColumnNames::MinTime});
        tags_block.insert(ColumnWithTypeAndName{std::move(max_time_column), min_max_time_type, TimeSeriesColumnNames::MaxTime});
    }

    /// Calculate IDs using precomputed ExpressionActions.
    DataTypePtr id_type = settings[TimeSeriesSetting::id_type];
    auto id_column = calculateId(tags_block);
    tags_block.insert(0, ColumnWithTypeAndName{id_column, id_type, TimeSeriesColumnNames::ID});

    if (tags_block.has(TimeSeriesColumnNames::AllTags))
        tags_block.erase(TimeSeriesColumnNames::AllTags);

    /// Step 4. Push the tags block.

    /// Tags are pushed first so that if the samples insert fails,
    /// we don't end up with sample rows referencing IDs that were never written to the tags table.
    tags_pipeline->push(std::move(tags_block));

    /// Step 5. Assemble and push the samples block.
    if (total_samples)
    {
        /// Build columns for the samples block.
        auto samples_id_column = id_type->createColumn();
        samples_id_column->reserve(total_samples);

        auto timestamp_column = timestamp_type->createColumn();
        timestamp_column->reserve(total_samples);

        auto value_column = scalar_type->createColumn();
        value_column->reserve(total_samples);

        fillSamplesColumns(
            filter,
            *id_column, ts_timestamps, ts_values, ts_offsets,
            *samples_id_column, *timestamp_column, *value_column);

        /// Assemble the block and push it to the "samples" table.
        Block samples_block;
        samples_block.insert(ColumnWithTypeAndName{std::move(samples_id_column), id_type, TimeSeriesColumnNames::ID});
        samples_block.insert(ColumnWithTypeAndName{std::move(timestamp_column), timestamp_type, TimeSeriesColumnNames::Timestamp});
        samples_block.insert(ColumnWithTypeAndName{std::move(value_column), scalar_type, TimeSeriesColumnNames::Value});

        samples_pipeline->push(std::move(samples_block));
    }
}


void TimeSeriesSink::initMetricsPipeline()
{
    /// It's important to use here for `metrics_header`
    /// the same data types as function consumeMetrics() uses to push blocks.
    /// There is a conversion step in the target pipelines, so we don't have to always
    /// match the data types of the columns in the "tags" or "samples" tables.

    const Block & header = getHeader();

    Block metrics_header;
    metrics_header.insert(ColumnWithTypeAndName{
        header.getByName(TimeSeriesColumnNames::MetricFamily).type, TimeSeriesColumnNames::MetricFamilyName});

    metrics_header.insert(ColumnWithTypeAndName{
        header.getByName(TimeSeriesColumnNames::Type).type, TimeSeriesColumnNames::Type});

    metrics_header.insert(ColumnWithTypeAndName{
        header.getByName(TimeSeriesColumnNames::Unit).type, TimeSeriesColumnNames::Unit});

    metrics_header.insert(ColumnWithTypeAndName{
        header.getByName(TimeSeriesColumnNames::Help).type, TimeSeriesColumnNames::Help});

    metrics_pipeline = createTargetPipeline(ViewTarget::Metrics, metrics_header);
}


void TimeSeriesSink::consumeMetrics(const Block & block)
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

    /// Step 2. Build columns for the metrics block, skipping rows with empty metric_family.
    auto new_metric_family_column = metric_family_col.type->createColumn();
    auto new_type_column = type_col.type->createColumn();
    auto new_unit_column = unit_col.type->createColumn();
    auto new_help_column = help_col.type->createColumn();

    fillMetricsColumns(
        *metric_family_col.column,
        *type_col.column, *unit_col.column, *help_col.column,
        *new_metric_family_column,
        *new_type_column, *new_unit_column, *new_help_column);

    /// We've already checked that at least one non-empty `metric_family` is present.
    chassert(!new_metric_family_column->empty());

    /// Step 3. Assemble the block and push it to the "metrics" table.
    Block metrics_block;
    metrics_block.insert(ColumnWithTypeAndName{std::move(new_metric_family_column), metric_family_col.type, TimeSeriesColumnNames::MetricFamilyName});
    metrics_block.insert(ColumnWithTypeAndName{std::move(new_type_column), type_col.type, TimeSeriesColumnNames::Type});
    metrics_block.insert(ColumnWithTypeAndName{std::move(new_unit_column), unit_col.type, TimeSeriesColumnNames::Unit});
    metrics_block.insert(ColumnWithTypeAndName{std::move(new_help_column), help_col.type, TimeSeriesColumnNames::Help});

    metrics_pipeline->push(std::move(metrics_block));
}


void TimeSeriesSink::onFinish()
{
    if (tags_pipeline)
        tags_pipeline->executor->finish();
    if (samples_pipeline)
        samples_pipeline->executor->finish();
    if (metrics_pipeline)
        metrics_pipeline->executor->finish();
}

}
