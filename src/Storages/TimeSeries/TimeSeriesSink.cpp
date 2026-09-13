#include <Storages/TimeSeries/TimeSeriesSink.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/NaNUtils.h>
#include <Common/DateLUTImpl.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Core/DecimalFunctions.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
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
#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <Storages/TimeSeries/TimeSeriesIDGenerator.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <base/EnumReflection.h>

#include <base/sort.h>

#include <algorithm>
#include <optional>
#include <ranges>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsUInt64 recent_samples_bucket_step_seconds;
    extern const TimeSeriesSettingsUInt64 samples_bucket_step_seconds;
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

            TimeSeriesSink::insertSortedTagsToColumns(
                sorted_tags,
                out_tags_names, out_tags_values, out_tags_offsets,
                columns_by_tag_name);
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

    /// Builds a block for the "tags" table from the series of the input block which pass `filter`:
    /// the columns `metric_name`, the columns of the tags from `tags_to_columns`, `tags`, and `min_time`, `max_time` if they are stored.
    /// The returned block has no `id` column yet. `tags_header` contains the types of all these columns; if it contains
    /// the `all_tags` column too then that column is added with the same data as `tags` (an id generator can reference it).
    Block makeTagsBlockWithoutId(
        const PaddedPODArray<UInt8> & filter,
        size_t num_time_series,
        const IColumn & metric_name_column,
        const ColumnMap & tags_map_column,
        const ColumnArray::Offsets & ts_offsets,
        const IColumn & ts_timestamps,
        const Block & tags_header,
        const Map & tags_to_columns)
    {
        const auto & tags_map_nested = tags_map_column.getNestedColumn();
        const auto & tags_tuple = assert_cast<const ColumnTuple &>(tags_map_nested.getData());
        chassert(tags_tuple.tupleSize() == 2);

        /// The columns with dedicated tags: `metric_name` and the columns from `tags_to_columns`.
        std::vector<std::tuple<String, MutableColumnPtr, DataTypePtr>> columns_by_tag_name_holder;
        std::unordered_map<std::string_view, IColumn *> columns_by_tag_name;

        auto add_tag_column = [&](std::string_view tag_name, const String & column_name)
        {
            auto type = tags_header.getByName(column_name).type;
            auto column = type->createColumn();
            column->reserve(num_time_series);
            columns_by_tag_name[tag_name] = column.get();
            columns_by_tag_name_holder.emplace_back(column_name, std::move(column), std::move(type));
        };

        add_tag_column(TimeSeriesTagNames::MetricName, TimeSeriesColumnNames::MetricName);

        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
            add_tag_column(tuple.at(0).safeGet<String>(), tuple.at(1).safeGet<String>());
        }

        auto tags_map_type = typeid_cast<std::shared_ptr<const DataTypeMap>>(tags_header.getByName(TimeSeriesColumnNames::Tags).type);
        auto new_tags_names = tags_map_type->getKeyType()->createColumn();
        new_tags_names->reserve(num_time_series);
        auto new_tags_values = tags_map_type->getValueType()->createColumn();
        new_tags_values->reserve(num_time_series);
        auto new_tags_offsets = ColumnArray::ColumnOffsets::create();
        new_tags_offsets->reserve(num_time_series);

        fillTagsColumns(
            filter,
            metric_name_column,
            tags_map_nested.getOffsets(), tags_tuple.getColumn(0), tags_tuple.getColumn(1),
            *new_tags_names, *new_tags_values, *new_tags_offsets,
            columns_by_tag_name);

        Block tags_block;
        for (auto & [column_name, column, type] : columns_by_tag_name_holder)
            tags_block.insert(ColumnWithTypeAndName{std::move(column), type, column_name});

        Columns new_tags_tuples_columns;
        new_tags_tuples_columns.push_back(std::move(new_tags_names));
        new_tags_tuples_columns.push_back(std::move(new_tags_values));
        ColumnPtr new_tags_column = ColumnMap::create(
            ColumnArray::create(ColumnTuple::create(std::move(new_tags_tuples_columns)), std::move(new_tags_offsets)));
        tags_block.insert(ColumnWithTypeAndName{new_tags_column, tags_map_type, TimeSeriesColumnNames::Tags});

        if (tags_header.has(TimeSeriesColumnNames::AllTags))
            tags_block.insert(ColumnWithTypeAndName{new_tags_column, tags_map_type, TimeSeriesColumnNames::AllTags});

        /// The columns `min_time` and `max_time` are optional (see the `store_min_time_and_max_time` setting).
        if (tags_header.has(TimeSeriesColumnNames::MinTime))
        {
            auto min_max_time_type = tags_header.getByName(TimeSeriesColumnNames::MinTime).type;
            auto min_time_column = min_max_time_type->createColumn();
            auto max_time_column = min_max_time_type->createColumn();
            min_time_column->reserve(num_time_series);
            max_time_column->reserve(num_time_series);
            fillMinMaxTimeColumns(filter, ts_offsets, ts_timestamps, *min_time_column, *max_time_column);
            tags_block.insert(ColumnWithTypeAndName{std::move(min_time_column), min_max_time_type, TimeSeriesColumnNames::MinTime});
            tags_block.insert(ColumnWithTypeAndName{std::move(max_time_column), min_max_time_type, TimeSeriesColumnNames::MaxTime});
        }

        return tags_block;
    }

    /// Sorts the samples of each time series by timestamp and removes the samples with duplicate timestamps
    /// (the sample with the greatest value is kept, a NaN value loses to any other value).
    /// The result is stored as indexes of the samples in the nested columns of the `time_series` column:
    /// `sorted_indices` contains the indexes, and `sorted_offsets[i]` is the end of the range of row `i` in `sorted_indices`.
    template <typename TimestampColumn>
    void sortSamples(
        const PaddedPODArray<UInt8> & filter,
        const ColumnArray::Offsets & ts_offsets,
        const TimestampColumn & ts_timestamps,
        const IColumn & ts_values,
        PaddedPODArray<size_t> & sorted_indices,
        PaddedPODArray<size_t> & sorted_offsets)
    {
        const auto & timestamps = ts_timestamps.getData();

        sorted_indices.clear();
        sorted_indices.reserve(timestamps.size());
        sorted_offsets.clear();
        sorted_offsets.reserve(filter.size());

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
                sorted_offsets.push_back(sorted_indices.size());
                continue;
            }

            size_t row_begin = sorted_indices.size();
            for (size_t j = ts_start; j < ts_end; ++j)
                sorted_indices.push_back(j);

            auto less_by_timestamp = [&](size_t lhs, size_t rhs) { return timestamps[lhs] < timestamps[rhs]; };
            size_t * row_data = sorted_indices.data() + row_begin;

            /// Samples usually arrive in timestamp order.
            if (!std::is_sorted(row_data, row_data + num_samples, less_by_timestamp))
                ::sort(row_data, row_data + num_samples, less_by_timestamp);

            /// Collapse each run of equal timestamps into one sample: the sample with the greatest value,
            /// where a NaN value loses to any other value.
            auto new_sample_wins = [&](size_t kept_index, size_t new_index)
            {
                Float64 kept_value = ts_values.getFloat64(kept_index);
                Float64 new_value = ts_values.getFloat64(new_index);
                if (isNaN(new_value))
                    return false;
                return isNaN(kept_value) || (new_value > kept_value);
            };

            size_t last_unique = 0;
            for (size_t k = 1; k < num_samples; ++k)
            {
                if (timestamps[row_data[k]] == timestamps[row_data[last_unique]])
                {
                    if (new_sample_wins(row_data[last_unique], row_data[k]))
                        row_data[last_unique] = row_data[k];
                }
                else
                {
                    row_data[++last_unique] = row_data[k];
                }
            }
            if (num_samples > 0)
                sorted_indices.resize(row_begin + last_unique + 1);

            sorted_offsets.push_back(sorted_indices.size());
        }
    }

    /// Returns the start of the bucket containing a timestamp: the timestamp rounded down to a multiple of the bucket step.
    /// The timestamp, the step and the result have the scale of the timestamp type.
    Decimal64 getBucket(Decimal64 timestamp, Decimal64 bucket_step)
    {
        /// The rounding is towards negative infinity, which matters for timestamps before 1970 (negative values):
        /// the start of a bucket must not be greater than any timestamp in it.
        return Decimal64(DateLUTImpl::roundDownToMultiple(timestamp.value, bucket_step.value));
    }

    /// Copies the samples `indices[begin, end)` of `ts_timestamps` and `ts_values` to the columns of the `samples` array.
    void insertSamplesByIndices(
        const size_t * indices, size_t begin, size_t end,
        const IColumn & ts_timestamps, const IColumn & ts_values,
        IColumn & out_timestamps, IColumn & out_values)
    {
        /// The indices are increasing, so the samples are copied by ranges:
        /// usually the input is already sorted and a bucket is one range.
        size_t range_start = begin;
        while (range_start < end)
        {
            size_t range_end = range_start + 1;
            while ((range_end < end) && (indices[range_end] == indices[range_end - 1] + 1))
                ++range_end;
            size_t first_index = indices[range_start];
            out_timestamps.insertRangeFrom(ts_timestamps, first_index, range_end - range_start);
            out_values.insertRangeFrom(ts_values, first_index, range_end - range_start);
            range_start = range_end;
        }
    }

    /// Converts a timestamp to `Decimal64` with the scale of the timestamp type.
    Decimal64 toDecimal64(DateTime64 timestamp)
    {
        return timestamp;
    }

    Decimal64 toDecimal64(UInt32 timestamp)
    {
        return Decimal64(timestamp);
    }

    /// Converts a `Decimal64` value with the scale of the timestamp type back
    /// to the value type of the timestamp column.
    template <typename TimestampColumn>
    typename TimestampColumn::ValueType toTimestamp(Decimal64 value)
    {
        using TimestampValue = typename TimestampColumn::ValueType;
        if constexpr (is_decimal<TimestampValue>)
            return TimestampValue(value.value);
        else
            return static_cast<TimestampValue>(value.value);
    }

    /// The type of the `samples` column of a samples block: `Array(Tuple(timestamp <timestamp_type>, value <value_type>))`.
    DataTypePtr makeSamplesArrayDataType(const DataTypePtr & timestamp_type, const DataTypePtr & value_type)
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
            DataTypes{timestamp_type, value_type}, Strings{TimeSeriesColumnNames::Timestamp, TimeSeriesColumnNames::Value}));
    }

    /// Builds a block for a samples table from the sorted samples of the series in the input block (see sortSamples).
    /// A row of the block contains the samples of one series within one bucket of `bucket_step` (with the scale of the timestamp type).
    template <typename TimestampColumn>
    Block makeSamplesBlock(
        const PaddedPODArray<UInt8> & filter,
        const IColumn & id_column,
        const DataTypePtr & id_type,
        const TimestampColumn & ts_timestamps,
        const DataTypePtr & timestamp_type,
        const IColumn & ts_values,
        const DataTypePtr & value_type,
        const PaddedPODArray<size_t> & sorted_indices,
        const PaddedPODArray<size_t> & sorted_offsets,
        Decimal64 bucket_step)
    {
        const auto & timestamps = ts_timestamps.getData();
        size_t total_samples = sorted_indices.size();

        auto out_id = id_type->createColumn();
        auto out_timestamps = ts_timestamps.cloneEmpty();
        auto out_values = ts_values.cloneEmpty();
        auto out_offsets = ColumnArray::ColumnOffsets::create();
        auto out_min_time = ts_timestamps.cloneEmpty();
        auto out_max_time = ts_timestamps.cloneEmpty();

        /// `cloneEmpty` keeps the scale of `DateTime64`.
        auto out_bucket = ts_timestamps.cloneEmpty();
        auto & bucket_data = assert_cast<TimestampColumn &>(*out_bucket).getData();

        out_timestamps->reserve(total_samples);
        out_values->reserve(total_samples);

        /// The filtered-in rows and the rows of `id_column` correspond one-to-one.
        size_t id_index = 0;
        for (size_t i = 0; i < filter.size(); ++i)
        {
            if (!filter[i])
                continue;

            size_t row_begin = (i == 0) ? 0 : sorted_offsets[i - 1];
            size_t row_end = sorted_offsets[i];

            /// The samples are sorted, so the samples of one bucket are adjacent.
            size_t bucket_begin = row_begin;
            while (bucket_begin < row_end)
            {
                Decimal64 bucket = getBucket(toDecimal64(timestamps[sorted_indices[bucket_begin]]), bucket_step);
                size_t bucket_end = bucket_begin + 1;
                while ((bucket_end < row_end) && (getBucket(toDecimal64(timestamps[sorted_indices[bucket_end]]), bucket_step) == bucket))
                    ++bucket_end;

                out_id->insertFrom(id_column, id_index);
                insertSamplesByIndices(sorted_indices.data(), bucket_begin, bucket_end, ts_timestamps, ts_values, *out_timestamps, *out_values);
                out_offsets->getData().push_back(out_timestamps->size());
                bucket_data.push_back(toTimestamp<TimestampColumn>(bucket));
                out_min_time->insertFrom(ts_timestamps, sorted_indices[bucket_begin]);
                out_max_time->insertFrom(ts_timestamps, sorted_indices[bucket_end - 1]);

                bucket_begin = bucket_end;
            }

            ++id_index;
        }

        auto samples_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(out_timestamps), std::move(out_values)}), std::move(out_offsets));

        Block samples_block;
        samples_block.insert(ColumnWithTypeAndName{std::move(out_id), id_type, TimeSeriesColumnNames::ID});
        samples_block.insert(ColumnWithTypeAndName{std::move(samples_column), makeSamplesArrayDataType(timestamp_type, value_type), TimeSeriesColumnNames::Samples});
        samples_block.insert(ColumnWithTypeAndName{std::move(out_bucket), timestamp_type, TimeSeriesColumnNames::Bucket});
        samples_block.insert(ColumnWithTypeAndName{std::move(out_min_time), timestamp_type, TimeSeriesColumnNames::MinTime});
        samples_block.insert(ColumnWithTypeAndName{std::move(out_max_time), timestamp_type, TimeSeriesColumnNames::MaxTime});
        return samples_block;
    }

    /// The blocks to insert into the samples table and into the recent samples table (if there is one).
    struct SamplesBlocks
    {
        Block samples_block;
        std::optional<Block> recent_samples_block;
    };

    /// Builds the blocks for the samples tables from the series in the input block: the samples of each series
    /// are sorted once (see sortSamples), then split into the buckets of each table (see makeSamplesBlock).
    /// `recent_samples_bucket_step` is unset if there is no recent samples table.
    template <typename TimestampColumn>
    SamplesBlocks makeSamplesBlocksForTimestampType(
        const PaddedPODArray<UInt8> & filter,
        const IColumn & id_column,
        const DataTypePtr & id_type,
        const TimestampColumn & ts_timestamps,
        const DataTypePtr & timestamp_type,
        const IColumn & ts_values,
        const DataTypePtr & value_type,
        const ColumnArray::Offsets & ts_offsets,
        Decimal64 samples_bucket_step,
        std::optional<Decimal64> recent_samples_bucket_step)
    {
        PaddedPODArray<size_t> sorted_indices;
        PaddedPODArray<size_t> sorted_offsets;
        sortSamples(filter, ts_offsets, ts_timestamps, ts_values, sorted_indices, sorted_offsets);

        SamplesBlocks res;
        res.samples_block = makeSamplesBlock(
            filter, id_column, id_type, ts_timestamps, timestamp_type, ts_values, value_type, sorted_indices, sorted_offsets, samples_bucket_step);

        if (recent_samples_bucket_step)
        {
            /// The same block is reused if the buckets of the two tables have the same step
            /// (a Block copy is cheap: it only copies column pointers).
            if (*recent_samples_bucket_step == samples_bucket_step)
                res.recent_samples_block = res.samples_block;
            else
                res.recent_samples_block = makeSamplesBlock(
                    filter, id_column, id_type, ts_timestamps, timestamp_type, ts_values, value_type, sorted_indices, sorted_offsets, *recent_samples_bucket_step);
        }

        return res;
    }

    /// Calls makeSamplesBlocksForTimestampType with the type of the column of the timestamps.
    SamplesBlocks makeSamplesBlocks(
        const PaddedPODArray<UInt8> & filter,
        const IColumn & id_column,
        const DataTypePtr & id_type,
        const IColumn & ts_timestamps,
        const DataTypePtr & timestamp_type,
        const IColumn & ts_values,
        const DataTypePtr & value_type,
        const ColumnArray::Offsets & ts_offsets,
        Decimal64 samples_bucket_step,
        std::optional<Decimal64> recent_samples_bucket_step)
    {
        if (const auto * decimal_timestamps = typeid_cast<const ColumnDecimal<DateTime64> *>(&ts_timestamps))
            return makeSamplesBlocksForTimestampType(filter, id_column, id_type, *decimal_timestamps, timestamp_type, ts_values, value_type, ts_offsets, samples_bucket_step, recent_samples_bucket_step);

        if (const auto * uint32_timestamps = typeid_cast<const ColumnUInt32 *>(&ts_timestamps))
            return makeSamplesBlocksForTimestampType(filter, id_column, id_type, *uint32_timestamps, timestamp_type, ts_values, value_type, ts_offsets, samples_bucket_step, recent_samples_bucket_step);

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected column {} of timestamps in the time_series column", ts_timestamps.getName());
    }

    /// Builds a block for the "metrics" table from the columns `metric_family`, `type`, `unit`, `help` of the input block,
    /// skipping the rows with an empty metric family. Returns nothing if there are no rows to insert.
    std::optional<Block> makeMetricsBlock(const Block & block)
    {
        const auto & metric_family_col = block.getByName(TimeSeriesColumnNames::MetricFamily);
        const auto & type_col = block.getByName(TimeSeriesColumnNames::Type);
        const auto & unit_col = block.getByName(TimeSeriesColumnNames::Unit);
        const auto & help_col = block.getByName(TimeSeriesColumnNames::Help);

        auto out_metric_family_column = metric_family_col.type->createColumn();
        auto out_type_column = type_col.type->createColumn();
        auto out_unit_column = unit_col.type->createColumn();
        auto out_help_column = help_col.type->createColumn();

        for (size_t i = 0; i < block.rows(); ++i)
        {
            if (metric_family_col.column->getDataAt(i).empty())
            {
                if (!type_col.column->getDataAt(i).empty())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Got non-empty type without a metric family");
                if (!unit_col.column->getDataAt(i).empty())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Got non-empty unit without a metric family");
                if (!help_col.column->getDataAt(i).empty())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Got non-empty help without a metric family");
                continue;
            }

            out_metric_family_column->insertFrom(*metric_family_col.column, i);
            out_type_column->insertFrom(*type_col.column, i);
            out_unit_column->insertFrom(*unit_col.column, i);
            out_help_column->insertFrom(*help_col.column, i);
        }

        if (out_metric_family_column->empty())
            return {};

        Block metrics_block;
        metrics_block.insert(ColumnWithTypeAndName{std::move(out_metric_family_column), metric_family_col.type, TimeSeriesColumnNames::MetricFamilyName});
        metrics_block.insert(ColumnWithTypeAndName{std::move(out_type_column), type_col.type, TimeSeriesColumnNames::Type});
        metrics_block.insert(ColumnWithTypeAndName{std::move(out_unit_column), unit_col.type, TimeSeriesColumnNames::Unit});
        metrics_block.insert(ColumnWithTypeAndName{std::move(out_help_column), help_col.type, TimeSeriesColumnNames::Help});
        return metrics_block;
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
    /// On cancellation without an exception (e.g. `timeout_overflow_mode='break'`) neither
    /// `onFinish` nor `onException` runs, leaving the executor started but unfinished.
    /// Cancel it so `~PushingPipelineExecutor`'s finished-or-unwinding invariant holds.
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

    auto tags_map_type = typeid_cast<std::shared_ptr<const DataTypeMap>>(tags_target_metadata->columns.get(TimeSeriesColumnNames::Tags).type);
    if (!tags_map_type)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` must have a Map type", TimeSeriesColumnNames::Tags);
    tags_header_before_id.insert(ColumnWithTypeAndName{tags_map_type, TimeSeriesColumnNames::Tags});

    /// The `all_tags` column is not stored, an id generator can reference it for compatibility: it contains the same data as `tags`.
    if (id_generator_uses_all_tags)
        tags_header_before_id.insert(ColumnWithTypeAndName{tags_map_type, TimeSeriesColumnNames::AllTags});

    /// The types of the timestamps and the values are taken from the input header, not from the samples table:
    /// the samples are copied from the input columns as is, the converting actions of the pipelines
    /// convert them to the types of the target tables afterwards.
    std::tie(timestamp_type, value_type) = splitTimeSeriesType(getHeader().getByName(TimeSeriesColumnNames::TimeSeries).type);

    if (settings[TimeSeriesSetting::store_min_time_and_max_time])
    {
        /// The same for `min_time` and `max_time`: they are filled with the timestamps of the input.
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
        /// The `all_tags` column is used only to calculate `id`, it is not inserted into the "tags" table.
        if (column.name != TimeSeriesColumnNames::AllTags)
            tags_header.insert(column);
    }

    tags_pipeline = createTargetPipeline(ViewTarget::Tags, tags_header);

    /// Build source header for samples block: a row contains the samples of one series within one time bucket.
    /// The steps of the buckets are converted from seconds to the scale of the timestamp type.
    Int64 timestamp_scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(tryGetDecimalScale(*timestamp_type).value_or(0));

    Block samples_header;
    samples_header.insert(ColumnWithTypeAndName{id_type, TimeSeriesColumnNames::ID});
    samples_header.insert(ColumnWithTypeAndName{makeSamplesArrayDataType(timestamp_type, value_type), TimeSeriesColumnNames::Samples});
    samples_header.insert(ColumnWithTypeAndName{timestamp_type, TimeSeriesColumnNames::Bucket});
    samples_header.insert(ColumnWithTypeAndName{timestamp_type, TimeSeriesColumnNames::MinTime});
    samples_header.insert(ColumnWithTypeAndName{timestamp_type, TimeSeriesColumnNames::MaxTime});

    samples_pipeline = createTargetPipeline(ViewTarget::Samples, samples_header);
    samples_bucket_step = Decimal64(static_cast<Int64>(settings[TimeSeriesSetting::samples_bucket_step_seconds].value) * timestamp_scale_multiplier);

    /// The recent samples table (if any) receives every sample too, but its buckets can have another step.
    if (time_series_storage.hasTarget(ViewTarget::RecentSamples))
    {
        recent_samples_pipeline = createTargetPipeline(ViewTarget::RecentSamples, samples_header);
        recent_samples_bucket_step = Decimal64(static_cast<Int64>(settings[TimeSeriesSetting::recent_samples_bucket_step_seconds].value) * timestamp_scale_multiplier);
    }
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

    const IColumn & ts_timestamps = ts_tuples->getColumn(0);
    const IColumn & ts_values = ts_tuples->getColumn(1);
    const Map & tags_to_columns = (*time_series_settings)[TimeSeriesSetting::tags_to_columns];

    /// Step 2. Build the tags block and calculate the identifiers.
    Block tags_block = makeTagsBlockWithoutId(
        filter, num_time_series, *metric_name_col.column, *tags_map_column, ts_offsets, ts_timestamps, tags_header_before_id, tags_to_columns);

    auto id_column = calculateId(tags_block);
    tags_block.insert(0, ColumnWithTypeAndName{id_column, id_type, TimeSeriesColumnNames::ID});

    if (tags_block.has(TimeSeriesColumnNames::AllTags))
        tags_block.erase(TimeSeriesColumnNames::AllTags);

    /// Step 3. Push the tags block.

    /// Tags are pushed first so that if the samples insert fails,
    /// we don't end up with sample rows referencing IDs that were never written to the tags table.
    tags_pipeline->push(std::move(tags_block));

    /// Step 4. Assemble and push the samples blocks.
    if (total_samples)
    {
        SamplesBlocks samples_blocks = makeSamplesBlocks(
            filter, *id_column, id_type, ts_timestamps, timestamp_type, ts_values, value_type, ts_offsets, samples_bucket_step, recent_samples_bucket_step);

        /// The samples table is written before the recent samples table: if the insert fails between
        /// the two writes, the sample is then missing from the recent samples table and just stays
        /// invisible until the TTL window slides past it. With the opposite order the sample would be
        /// visible in the TTL window and then disappear, which looks like data loss.
        samples_pipeline->push(std::move(samples_blocks.samples_block));
        if (samples_blocks.recent_samples_block)
            recent_samples_pipeline->push(std::move(*samples_blocks.recent_samples_block));
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
    if (auto metrics_block = makeMetricsBlock(block))
        metrics_pipeline->push(std::move(*metrics_block));
}


void TimeSeriesSink::onFinish()
{
    if (tags_pipeline)
        tags_pipeline->executor->finish();
    if (samples_pipeline)
        samples_pipeline->executor->finish();
    if (recent_samples_pipeline)
        recent_samples_pipeline->executor->finish();
    if (metrics_pipeline)
        metrics_pipeline->executor->finish();
}

}
