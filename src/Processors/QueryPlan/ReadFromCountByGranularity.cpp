#include <Processors/QueryPlan/ReadFromCountByGranularity.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/ISource.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/StorageSnapshot.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/logger_useful.h>

#include <atomic>

namespace DB
{

ReadFromCountByGranularity::ReadFromCountByGranularity(
    SharedHeader output_header_,
    RangesInDataParts parts_with_ranges_,
    ExpressionActionsPtr bucket_expression_,
    Names group_by_key_names_,
    KeyDescription primary_key_,
    AggregateFunctionPtr count_function_,
    StorageSnapshotPtr storage_snapshot_,
    MergeTreeSettingsPtr data_settings_,
    ContextPtr context_,
    size_t num_streams_,
    ExpressionActionsPtr filter_expression_,
    String filter_column_name_,
    bool has_filter_)
    : ISourceStep(std::move(output_header_))
    , parts_with_ranges(std::move(parts_with_ranges_))
    , bucket_expression(std::move(bucket_expression_))
    , group_by_key_names(std::move(group_by_key_names_))
    , primary_key(primary_key_)
    , count_function(std::move(count_function_))
    , storage_snapshot(std::move(storage_snapshot_))
    , data_settings(std::move(data_settings_))
    , context(std::move(context_))
    , num_streams(num_streams_)
    , filter_expression(std::move(filter_expression_))
    , filter_column_name(std::move(filter_column_name_))
    , has_filter(has_filter_)

{
}

/// A task is a slice of mark ranges from a single part.
struct CountTask
{
    DataPartPtr data_part;
    MarkRanges mark_ranges;
    MarkRanges exact_ranges;
};

struct CountByGranularityPool
{
    std::vector<CountTask> tasks;
    std::atomic<size_t> next_task{0};

    const CountTask * getTask()
    {
        size_t idx = next_task.fetch_add(1, std::memory_order_relaxed);
        if (idx >= tasks.size())
            return nullptr;
        return &tasks[idx];
    }
};

using CountByGranularityPoolPtr = std::shared_ptr<CountByGranularityPool>;
using BucketKey = std::vector<Field>;

struct BucketKeyCompare
{
    bool operator()(const BucketKey & a, const BucketKey & b) const
    {
        for (size_t i = 0; i < a.size() && i < b.size(); ++i)
        {
            if (a[i] < b[i]) return true;
            if (b[i] < a[i]) return false;
        }
        return a.size() < b.size();
    }
};

using BucketCounts = std::map<BucketKey, UInt64, BucketKeyCompare>;

static bool bucketColumnsEqual(const Columns & cols, size_t a, size_t b)
{
    for (const auto & col : cols)
        if (0 != col->compareAt(a, b, *col, 0))
            return false;
    return true;
}

static BucketKey getBucketKey(const Columns & cols, size_t row)
{
    BucketKey key;
    key.reserve(cols.size());
    for (const auto & col : cols)
    {
        Field val;
        col->get(row, val);
        key.push_back(std::move(val));
    }
    return key;
}

class CountByGranularitySource : public ISource
{
public:
    CountByGranularitySource(
        SharedHeader header_,
        CountByGranularityPoolPtr pool_,
        ExpressionActionsPtr bucket_expression_,
        Names group_by_key_names_,
        KeyDescription primary_key_,
        AggregateFunctionPtr count_function_,
        StorageSnapshotPtr storage_snapshot_,
        MergeTreeSettingsPtr data_settings_,
        ContextPtr context_,
        ExpressionActionsPtr filter_expression_,
        String filter_column_name_,
        bool has_filter_)
        : ISource(std::move(header_))
        , pool(std::move(pool_))
        , bucket_expression(std::move(bucket_expression_))
        , group_by_key_names(std::move(group_by_key_names_))
        , primary_key(primary_key_)
        , count_function(std::move(count_function_))
        , storage_snapshot(std::move(storage_snapshot_))
        , data_settings(std::move(data_settings_))
        , context(std::move(context_))
        , filter_expression(std::move(filter_expression_))
        , filter_column_name(std::move(filter_column_name_))
        , has_filter(has_filter_)
    {
    }

    String getName() const override { return "CountByGranularitySource"; }

protected:
    Chunk generate() override
    {
        while (const auto * task = pool->getTask())
        {
            auto chunk = processTask(*task);
            if (chunk.hasRows())
                return chunk;
        }
        return {};
    }

private:
    /// Create a MergeTreeReader for the given marks (PK columns only).
    MergeTreeReaderPtr createReaderForMarks(
        const DataPartPtr & data_part, const MarkRanges & marks)
    {
        if (marks.empty())
            return nullptr;

        NamesAndTypesList columns_to_read;
        for (size_t i = 0; i < primary_key.column_names.size(); ++i)
            columns_to_read.push_back({primary_key.column_names[i], primary_key.data_types[i]});

        auto part_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(
            data_part, std::make_shared<AlterConversions>());
        auto reader_settings = MergeTreeReaderSettings::createFromContext(context);

        return createMergeTreeReader(
            part_info, columns_to_read, storage_snapshot, data_settings,
            marks, {},
            context->getUncompressedCache().get(),
            context->getMarkCache().get(),
            nullptr, reader_settings, {}, {});
    }

    /// Read a granule's PK columns and count rows per bucket.
    /// If apply_filter is true, also apply the filter expression
    /// and only count rows that pass.
    void countGranuleByReading(
        IMergeTreeReader & reader, size_t mark, size_t rows_in_granule,
        bool apply_filter, BucketCounts & bucket_counts)
    {
        Columns res(primary_key.column_names.size(), nullptr);
        reader.readRows(mark, mark + 1, false, rows_in_granule, 0, res);

        const auto & bucket_inputs = bucket_expression->getActionsDAG().getInputs();
        Block block;
        for (size_t i = 0; i < primary_key.column_names.size(); ++i)
        {
            if (!res[i])
                return;
            String col_name = (i < bucket_inputs.size()) ? bucket_inputs[i]->result_name : primary_key.column_names[i];
            block.insert({res[i], primary_key.data_types[i], col_name});
        }

        size_t num_rows = block.rows();

        ColumnPtr filter_col;
        if (apply_filter && filter_expression)
        {
            filter_expression->execute(block);
            if (block.has(filter_column_name))
                filter_col = block.getByName(filter_column_name).column;
        }

        bucket_expression->execute(block);

        Columns bucket_cols;
        for (const auto & key_name : group_by_key_names)
            bucket_cols.push_back(block.getByName(key_name).column);

        BucketKey current_key = getBucketKey(bucket_cols, 0);
        size_t run_start = 0;

        auto flush_run = [&](size_t run_end)
        {
            UInt64 count;
            if (filter_col)
            {
                count = 0;
                for (size_t j = run_start; j < run_end; ++j)
                    if (filter_col->getBool(j))
                        ++count;
            }
            else
            {
                count = run_end - run_start;
            }
            if (count > 0)
                bucket_counts[current_key] += count;
        };

        for (size_t i = 1; i < num_rows; ++i)
        {
            BucketKey key = getBucketKey(bucket_cols, i);
            if (key != current_key)
            {
                flush_run(i);
                current_key = std::move(key);
                run_start = i;
            }
        }
        flush_run(num_rows);
    }

    /// Process a set of mark ranges: metadata for same-bucket runs,
    /// read PK column for boundary/last marks.
    void processMarkRanges(
        const DataPartPtr & data_part,
        const MarkRanges & ranges,
        const Columns & bucket_columns,
        const MergeTreeIndexGranularity & granularity,
        size_t part_rows_count,
        size_t marks_without_final,
        bool apply_filter,
        BucketCounts & bucket_counts)
    {
        if (ranges.empty())
            return;

        /// Identify boundary marks that need PK column read.
        MarkRanges boundary_marks;
        for (const auto & range : ranges)
        {
            for (size_t mark = range.begin; mark < range.end; ++mark)
            {
                bool is_last = (mark + 1 >= marks_without_final);
                bool crosses = !is_last && !bucketColumnsEqual(bucket_columns, mark, mark + 1);
                if (is_last || crosses)
                    boundary_marks.push_back(MarkRange(mark, mark + 1));
            }
        }

        /// If apply_filter is true, ALL marks need reading (not just boundaries).
        auto reader = createReaderForMarks(data_part, apply_filter ? ranges : boundary_marks);

        for (const auto & range : ranges)
        {
            size_t mark = range.begin;
            while (mark < range.end)
            {
                bool is_last = (mark + 1 >= marks_without_final);
                bool crosses = !is_last && !bucketColumnsEqual(bucket_columns, mark, mark + 1);

                if (!apply_filter && !is_last && !crosses)
                {
                    /// Same bucket, no filter needed: bulk count via metadata.
                    size_t run_end = mark + 1;
                    while (run_end < range.end
                        && run_end + 1 < marks_without_final
                        && bucketColumnsEqual(bucket_columns, mark, run_end)
                        && bucketColumnsEqual(bucket_columns, run_end, run_end + 1))
                        ++run_end;

                    auto key = getBucketKey(bucket_columns, mark);
                    bucket_counts[key] += granularity.getRowsCountInRange(mark, run_end);
                    mark = run_end;
                }
                else
                {
                    /// Need to read PK column: boundary mark, last mark, or filter required.
                    size_t rows = is_last
                        ? part_rows_count - granularity.getRowsCountInRange(0, mark)
                        : granularity.getRowsCountInRange(mark, mark + 1);

                    if (rows > 0 && reader)
                        countGranuleByReading(*reader, mark, rows, apply_filter, bucket_counts);

                    ++mark;
                }
            }
        }
    }

    Chunk processTask(const CountTask & task)
    {
        const auto & part = *task.data_part;
        const auto & granularity = *part.index_granularity;

        auto index = part.getIndex();
        if (!index || index->empty())
            return {};

        if (granularity.getMarksCount() == 0)
            return {};

        /// Build block with column names matching what bucket_expression expects.
        /// The expression DAG may use qualified names (e.g. "__table1.k") from the analyzer,
        /// while PK index columns use unqualified names ("k").
        const auto & bucket_inputs = bucket_expression->getActionsDAG().getInputs();
        Block index_block;
        for (size_t i = 0; i < bucket_inputs.size() && i < index->size(); ++i)
            index_block.insert({(*index)[i], primary_key.data_types[i], bucket_inputs[i]->result_name});
        bucket_expression->execute(index_block);

        Columns bucket_columns;
        for (const auto & key_name : group_by_key_names)
            bucket_columns.push_back(index_block.getByName(key_name).column);

        size_t marks_without_final = granularity.getMarksCountWithoutFinal();

        BucketCounts bucket_counts;

        if (has_filter)
        {
            /// Exact ranges: all rows match filter, only need bucket split.
            processMarkRanges(task.data_part, task.exact_ranges, bucket_columns,
                granularity, part.rows_count, marks_without_final,
                /*apply_filter=*/false, bucket_counts);

            /// Non-exact marks: need filter.
            MarkRanges non_exact;
            size_t ei = 0;
            for (const auto & range : task.mark_ranges)
            {
                for (size_t mark = range.begin; mark < range.end; ++mark)
                {
                    while (ei < task.exact_ranges.size() && task.exact_ranges[ei].end <= mark)
                        ++ei;
                    bool in_exact = ei < task.exact_ranges.size()
                        && mark >= task.exact_ranges[ei].begin
                        && mark < task.exact_ranges[ei].end;
                    if (!in_exact)
                        non_exact.push_back(MarkRange(mark, mark + 1));
                }
            }
            if (!non_exact.empty())
                processMarkRanges(task.data_part, non_exact, bucket_columns,
                    granularity, part.rows_count, marks_without_final,
                    /*apply_filter=*/true, bucket_counts);
        }
        else
        {
            processMarkRanges(task.data_part, task.mark_ranges, bucket_columns,
                granularity, part.rows_count, marks_without_final,
                /*apply_filter=*/false, bucket_counts);
        }

        if (bucket_counts.empty())
            return {};

        return buildChunk(bucket_counts);
    }

    Chunk buildChunk(const BucketCounts & bucket_counts)
    {
        const auto & header = getPort().getHeader();

        std::vector<MutableColumnPtr> key_cols;
        for (const auto & key_name : group_by_key_names)
            key_cols.push_back(header.getByName(key_name).type->createColumn());

        auto agg_col = ColumnAggregateFunction::create(count_function);

        for (const auto & [bucket_key, count] : bucket_counts)
        {
            for (size_t i = 0; i < bucket_key.size(); ++i)
                key_cols[i]->insert(bucket_key[i]);

            std::vector<char> state(count_function->sizeOfData());
            AggregateDataPtr place = state.data();
            count_function->create(place);
            AggregateFunctionCount::set(place, count);
            agg_col->insertFrom(place);
            count_function->destroy(place);
        }

        size_t num_rows = agg_col->size();
        Columns result_columns;
        ColumnPtr agg_col_ptr = std::move(agg_col);
        for (size_t i = 0; i < header.columns(); ++i)
        {
            bool is_key = false;
            for (size_t k = 0; k < group_by_key_names.size(); ++k)
            {
                if (header.getByPosition(i).name == group_by_key_names[k])
                {
                    result_columns.push_back(std::move(key_cols[k]));
                    is_key = true;
                    break;
                }
            }
            if (!is_key)
                result_columns.push_back(agg_col_ptr);
        }

        return Chunk(std::move(result_columns), num_rows);
    }

    CountByGranularityPoolPtr pool;
    ExpressionActionsPtr bucket_expression;
    Names group_by_key_names;
    KeyDescription primary_key;
    AggregateFunctionPtr count_function;
    StorageSnapshotPtr storage_snapshot;
    MergeTreeSettingsPtr data_settings;
    ContextPtr context;
    ExpressionActionsPtr filter_expression;
    String filter_column_name;
    bool has_filter;
};

/// Intersect two sorted MarkRanges.
static MarkRanges intersectRanges(const MarkRanges & a, const MarkRanges & b)
{
    MarkRanges result;
    size_t bi = 0;
    for (const auto & ar : a)
    {
        while (bi < b.size() && b[bi].end <= ar.begin)
            ++bi;
        for (size_t j = bi; j < b.size() && b[j].begin < ar.end; ++j)
        {
            size_t lo = std::max(ar.begin, b[j].begin);
            size_t hi = std::min(ar.end, b[j].end);
            if (lo < hi)
                result.push_back(MarkRange(lo, hi));
        }
    }
    return result;
}

Pipe ReadFromCountByGranularity::makePipe()
{
    if (parts_with_ranges.empty())
        return Pipe(std::make_shared<NullSource>(getOutputHeader()));

    size_t total_marks = 0;
    for (const auto & part : parts_with_ranges)
        for (const auto & range : part.ranges)
            total_marks += range.end - range.begin;

    size_t actual_streams = std::min(num_streams, std::max<size_t>(total_marks, 1));
    actual_streams = std::max<size_t>(actual_streams, 1);
    size_t marks_per_task = std::max<size_t>((total_marks + actual_streams - 1) / actual_streams, 1);

    auto pool = std::make_shared<CountByGranularityPool>();

    for (const auto & part_with_ranges : parts_with_ranges)
    {
        size_t range_idx = 0;
        size_t offset_in_range = 0;

        while (range_idx < part_with_ranges.ranges.size())
        {
            MarkRanges task_marks;
            size_t task_size = 0;

            while (task_size < marks_per_task && range_idx < part_with_ranges.ranges.size())
            {
                const auto & range = part_with_ranges.ranges[range_idx];
                size_t start = range.begin + offset_in_range;
                size_t available = range.end - start;
                size_t take = std::min(available, marks_per_task - task_size);

                task_marks.push_back(MarkRange(start, start + take));
                task_size += take;
                offset_in_range += take;

                if (offset_in_range >= range.end - range.begin)
                {
                    ++range_idx;
                    offset_in_range = 0;
                }
            }

            if (!task_marks.empty())
            {
                MarkRanges task_exact = has_filter
                    ? intersectRanges(task_marks, part_with_ranges.exact_ranges)
                    : MarkRanges{};

                pool->tasks.push_back(CountTask{
                    .data_part = part_with_ranges.data_part,
                    .mark_ranges = std::move(task_marks),
                    .exact_ranges = std::move(task_exact),
                });
            }
        }
    }

    Pipe pipe;
    for (size_t i = 0; i < std::min(actual_streams, pool->tasks.size()); ++i)
    {
        auto source = std::make_shared<CountByGranularitySource>(
            getOutputHeader(),
            pool,
            bucket_expression,
            group_by_key_names,
            primary_key,
            count_function,
            storage_snapshot,
            data_settings,
            context,
            filter_expression,
            filter_column_name,
            has_filter);

        pipe.addSource(std::move(source));
    }

    return pipe;
}

void ReadFromCountByGranularity::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto pipe = makePipe();

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

}
