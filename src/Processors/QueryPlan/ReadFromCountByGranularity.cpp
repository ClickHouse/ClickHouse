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
#include <Columns/ColumnVector.h>
#include <Common/FieldVisitorHash.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>

#include <absl/container/flat_hash_map.h>

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

struct BucketKeyHash
{
    size_t operator()(const BucketKey & key) const
    {
        SipHash hash;
        for (const auto & field : key)
            applyVisitor(FieldVisitorHash(hash), field);
        return hash.get64();
    }
};

using GenericCounts = absl::flat_hash_map<BucketKey, UInt64, BucketKeyHash>;

template <typename T>
using NumericCounts = absl::flat_hash_map<T, UInt64>;

/// Mapping from bucket expression input name to PK column index.
/// Precomputed once per source to avoid per-granule string matching.
struct InputToPKMapping
{
    struct Entry
    {
        String input_name;
        size_t pk_index;
    };
    /// Entries for bucket expression inputs that need aliasing (qualified name != PK name).
    std::vector<Entry> aliases;
    /// All bucket expression inputs mapped to their PK column index, for building index blocks.
    std::vector<Entry> all;
};

static InputToPKMapping buildInputToPKMapping(
    const ExpressionActionsPtr & bucket_expression,
    const KeyDescription & primary_key)
{
    InputToPKMapping mapping;
    const auto & bucket_inputs = bucket_expression->getActionsDAG().getInputs();
    for (const auto * input_node : bucket_inputs)
    {
        const auto & input_name = input_node->result_name;
        for (size_t pk_i = 0; pk_i < primary_key.column_names.size(); ++pk_i)
        {
            const auto & pk_name = primary_key.column_names[pk_i];
            if (input_name == pk_name || input_name.ends_with("." + pk_name))
            {
                mapping.all.push_back({input_name, pk_i});
                if (input_name != pk_name)
                    mapping.aliases.push_back({input_name, pk_i});
                break;
            }
        }
    }
    return mapping;
}



static bool bucketColumnsEqual(const Columns & cols, size_t a, size_t b)
{
    for (const auto & col : cols)
        if (0 != col->compareAt(a, b, *col, 0))
            return false;
    return true;
}

static BucketKey getGenericBucketKey(const Columns & cols, size_t row)
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
        bool has_filter_,
        InputToPKMapping input_mapping_,
        TypeIndex numeric_type_index_,
        std::vector<size_t> key_col_positions_)
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
        , input_mapping(std::move(input_mapping_))
        , numeric_type_index(numeric_type_index_)
        , key_col_positions(std::move(key_col_positions_))
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

    Block readGranule(IMergeTreeReader & reader, size_t mark, size_t rows_in_granule)
    {
        Columns res(primary_key.column_names.size(), nullptr);
        reader.readRows(mark, mark + 1, false, rows_in_granule, 0, res);

        Block block;
        for (size_t i = 0; i < primary_key.column_names.size(); ++i)
        {
            if (!res[i])
                return {};
            block.insert({res[i], primary_key.data_types[i], primary_key.column_names[i]});
        }
        for (const auto & alias : input_mapping.aliases)
            block.insert({res[alias.pk_index], primary_key.data_types[alias.pk_index], alias.input_name});
        return block;
    }

    Columns executeBucketExpression(Block & block)
    {
        bucket_expression->execute(block);
        Columns bucket_cols;
        bucket_cols.reserve(group_by_key_names.size());
        for (const auto & key_name : group_by_key_names)
            bucket_cols.push_back(block.getByName(key_name).column);
        return bucket_cols;
    }

    template <typename BucketCounts, typename GetKey>
    void countGranuleByReading(
        IMergeTreeReader & reader, size_t mark, size_t rows_in_granule,
        BucketCounts & bucket_counts, GetKey && get_key)
    {
        Block block = readGranule(reader, mark, rows_in_granule);
        if (block.columns() == 0 || block.rows() == 0)
            return;

        Columns bucket_cols = executeBucketExpression(block);
        size_t num_rows = block.rows();
        auto current_key = get_key(bucket_cols, 0);
        size_t run_start = 0;

        for (size_t i = 1; i < num_rows; ++i)
        {
            auto key = get_key(bucket_cols, i);
            if (key != current_key)
            {
                bucket_counts[current_key] += i - run_start;
                current_key = std::move(key);
                run_start = i;
            }
        }
        bucket_counts[current_key] += num_rows - run_start;
    }

    template <typename BucketCounts, typename GetKey>
    void countGranuleWithFilter(
        IMergeTreeReader & reader, size_t mark, size_t rows_in_granule,
        BucketCounts & bucket_counts, GetKey && get_key)
    {
        Block block = readGranule(reader, mark, rows_in_granule);
        if (block.columns() == 0 || block.rows() == 0)
            return;

        filter_expression->execute(block);
        ColumnPtr filter_col = block.getByName(filter_column_name).column;

        Columns bucket_cols = executeBucketExpression(block);
        size_t num_rows = block.rows();
        auto current_key = get_key(bucket_cols, 0);
        size_t run_start = 0;

        auto flush_run = [&](size_t run_end)
        {
            UInt64 count = 0;
            for (size_t j = run_start; j < run_end; ++j)
                if (filter_col->getBool(j))
                    ++count;
            if (count > 0)
                bucket_counts[current_key] += count;
        };

        for (size_t i = 1; i < num_rows; ++i)
        {
            auto key = get_key(bucket_cols, i);
            if (key != current_key)
            {
                flush_run(i);
                current_key = std::move(key);
                run_start = i;
            }
        }
        flush_run(num_rows);
    }

    template <typename BucketCounts, typename GetKey>
    void processMarkRangesNoFilter(
        const DataPartPtr & data_part,
        const MarkRanges & ranges,
        const Columns & bucket_columns,
        const MergeTreeIndexGranularity & granularity,
        size_t part_rows_count,
        size_t marks_without_final,
        BucketCounts & bucket_counts,
        GetKey && get_key)
    {
        if (ranges.empty())
            return;

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

        auto reader = createReaderForMarks(data_part, boundary_marks);

        for (const auto & range : ranges)
        {
            size_t mark = range.begin;
            while (mark < range.end)
            {
                bool is_last = (mark + 1 >= marks_without_final);
                bool crosses = !is_last && !bucketColumnsEqual(bucket_columns, mark, mark + 1);

                if (!is_last && !crosses)
                {
                    size_t run_end = mark + 1;
                    while (run_end < range.end
                        && run_end + 1 < marks_without_final
                        && bucketColumnsEqual(bucket_columns, mark, run_end)
                        && bucketColumnsEqual(bucket_columns, run_end, run_end + 1))
                        ++run_end;

                    auto key = get_key(bucket_columns, mark);
                    bucket_counts[key] += granularity.getRowsCountInRange(mark, run_end);
                    mark = run_end;
                }
                else
                {
                    size_t rows = is_last
                        ? part_rows_count - granularity.getRowsCountInRange(0, mark)
                        : granularity.getRowsCountInRange(mark, mark + 1);

                    if (rows > 0 && reader)
                        countGranuleByReading(*reader, mark, rows, bucket_counts, get_key);

                    ++mark;
                }
            }
        }
    }

    template <typename BucketCounts, typename GetKey>
    void processMarkRangesWithFilter(
        const DataPartPtr & data_part,
        const MarkRanges & ranges,
        const MergeTreeIndexGranularity & granularity,
        size_t part_rows_count,
        size_t marks_without_final,
        BucketCounts & bucket_counts,
        GetKey && get_key)
    {
        if (ranges.empty())
            return;

        auto reader = createReaderForMarks(data_part, ranges);

        for (const auto & range : ranges)
        {
            for (size_t mark = range.begin; mark < range.end; ++mark)
            {
                bool is_last = (mark + 1 >= marks_without_final);
                size_t rows = is_last
                    ? part_rows_count - granularity.getRowsCountInRange(0, mark)
                    : granularity.getRowsCountInRange(mark, mark + 1);

                if (rows > 0 && reader)
                    countGranuleWithFilter(*reader, mark, rows, bucket_counts, get_key);
            }
        }
    }

    MarkRanges computeNonExactMarks(const CountTask & task)
    {
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
        return non_exact;
    }

    template <typename BucketCounts, typename GetKey, typename InsertKey>
    Chunk processTaskImpl(
        const CountTask & task,
        const Columns & bucket_columns,
        size_t marks_without_final,
        GetKey && get_key,
        InsertKey && insert_key)
    {
        const auto & part = *task.data_part;
        const auto & granularity = *part.index_granularity;

        BucketCounts bucket_counts;

        if (has_filter)
        {
            processMarkRangesNoFilter(task.data_part, task.exact_ranges, bucket_columns,
                granularity, part.rows_count, marks_without_final,
                bucket_counts, get_key);

            MarkRanges non_exact = computeNonExactMarks(task);
            if (!non_exact.empty())
                processMarkRangesWithFilter(task.data_part, non_exact,
                    granularity, part.rows_count, marks_without_final,
                    bucket_counts, get_key);
        }
        else
        {
            processMarkRangesNoFilter(task.data_part, task.mark_ranges, bucket_columns,
                granularity, part.rows_count, marks_without_final,
                bucket_counts, get_key);
        }

        if (bucket_counts.empty())
            return {};

        return buildChunk(bucket_counts, insert_key);
    }

    template <typename BucketCounts, typename InsertKey>
    Chunk buildChunk(const BucketCounts & bucket_counts, InsertKey && insert_key)
    {
        const auto & header = getPort().getHeader();

        std::vector<MutableColumnPtr> key_cols;
        key_cols.reserve(group_by_key_names.size());
        for (const auto & key_name : group_by_key_names)
            key_cols.push_back(header.getByName(key_name).type->createColumn());

        auto agg_col = ColumnAggregateFunction::create(count_function);
        agg_col->reserve(bucket_counts.size());

        std::vector<char> state_buf(count_function->sizeOfData());
        AggregateDataPtr place = state_buf.data();

        for (const auto & [bucket_key, count] : bucket_counts)
        {
            insert_key(key_cols, bucket_key);

            count_function->create(place);
            AggregateFunctionCount::set(place, count);
            agg_col->insertFrom(place);
            count_function->destroy(place);
        }

        size_t num_rows = agg_col->size();
        Columns result_columns(header.columns());
        ColumnPtr agg_col_ptr = std::move(agg_col);

        for (size_t k = 0; k < key_col_positions.size(); ++k)
            result_columns[key_col_positions[k]] = std::move(key_cols[k]);

        for (auto & col : result_columns)
            if (!col)
                col = agg_col_ptr;

        return Chunk(std::move(result_columns), num_rows);
    }

    Block buildIndexBlock(const IMergeTreeDataPart & part)
    {
        auto index = part.getIndex();
        if (!index || index->empty())
            return {};

        Block index_block;
        for (const auto & entry : input_mapping.all)
        {
            if (entry.pk_index < index->size() && !index_block.has(entry.input_name))
                index_block.insert({(*index)[entry.pk_index], primary_key.data_types[entry.pk_index], entry.input_name});
        }
        return index_block;
    }

    Chunk processTask(const CountTask & task)
    {
        const auto & part = *task.data_part;
        const auto & granularity = *part.index_granularity;

        if (granularity.getMarksCount() == 0)
            return {};

        Block index_block = buildIndexBlock(part);
        if (index_block.columns() == 0)
            return {};

        bucket_expression->execute(index_block);

        Columns bucket_columns;
        bucket_columns.reserve(group_by_key_names.size());
        for (const auto & key_name : group_by_key_names)
            bucket_columns.push_back(index_block.getByName(key_name).column);

        size_t marks_without_final = granularity.getMarksCountWithoutFinal();

        if (numeric_type_index != TypeIndex::Nothing && group_by_key_names.size() == 1)
            return dispatchNumericType(task, bucket_columns, marks_without_final);

        auto get_key = [](const Columns & cols, size_t row) { return getGenericBucketKey(cols, row); };
        auto insert_key = [](std::vector<MutableColumnPtr> & cols, const BucketKey & key)
        {
            for (size_t i = 0; i < key.size(); ++i)
                cols[i]->insert(key[i]);
        };
        return processTaskImpl<GenericCounts>(task, bucket_columns, marks_without_final, get_key, insert_key);
    }

    template <typename T>
    Chunk processTaskNumeric(
        const CountTask & task,
        const Columns & bucket_columns,
        size_t marks_without_final)
    {
        auto get_key = [](const Columns & cols, size_t row) -> T
        {
            return assert_cast<const ColumnVector<T> &>(*cols[0]).getData()[row];
        };
        auto insert_key = [](std::vector<MutableColumnPtr> & cols, T key)
        {
            assert_cast<ColumnVector<T> &>(*cols[0]).getData().push_back(key);
        };
        return processTaskImpl<NumericCounts<T>>(task, bucket_columns, marks_without_final, get_key, insert_key);
    }

    Chunk dispatchNumericType(
        const CountTask & task,
        const Columns & bucket_columns,
        size_t marks_without_final)
    {
        switch (numeric_type_index)
        {
            case TypeIndex::UInt8: return processTaskNumeric<UInt8>(task, bucket_columns, marks_without_final);
            case TypeIndex::UInt16: return processTaskNumeric<UInt16>(task, bucket_columns, marks_without_final);
            case TypeIndex::UInt32: return processTaskNumeric<UInt32>(task, bucket_columns, marks_without_final);
            case TypeIndex::UInt64: return processTaskNumeric<UInt64>(task, bucket_columns, marks_without_final);
            case TypeIndex::Int8: return processTaskNumeric<Int8>(task, bucket_columns, marks_without_final);
            case TypeIndex::Int16: return processTaskNumeric<Int16>(task, bucket_columns, marks_without_final);
            case TypeIndex::Int32: return processTaskNumeric<Int32>(task, bucket_columns, marks_without_final);
            case TypeIndex::Int64: return processTaskNumeric<Int64>(task, bucket_columns, marks_without_final);
            case TypeIndex::Float32: return processTaskNumeric<Float32>(task, bucket_columns, marks_without_final);
            case TypeIndex::Float64: return processTaskNumeric<Float64>(task, bucket_columns, marks_without_final);
            default: UNREACHABLE();
        }
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
    InputToPKMapping input_mapping;
    TypeIndex numeric_type_index;
    std::vector<size_t> key_col_positions;
};

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

    auto input_mapping = buildInputToPKMapping(bucket_expression, primary_key);

    /// Detect single-numeric fast path: if GROUP BY has exactly one key
    /// and its bucket expression output is a native numeric type, store its
    /// TypeIndex for devirtualized column access. Otherwise TypeIndex::Nothing.
    TypeIndex numeric_type_index = TypeIndex::Nothing;
    {
        Block dummy;
        for (const auto & entry : input_mapping.all)
            dummy.insert({primary_key.data_types[entry.pk_index]->createColumn(), primary_key.data_types[entry.pk_index], entry.input_name});
        bucket_expression->execute(dummy);

        if (group_by_key_names.size() == 1)
        {
            const auto & type = dummy.getByName(group_by_key_names[0]).type;
            if (isNativeNumber(type))
                numeric_type_index = type->getTypeId();
        }
    }

    /// Precompute key column positions in the output header.
    const auto & header = getOutputHeader();
    std::vector<size_t> key_col_positions;
    key_col_positions.reserve(group_by_key_names.size());
    for (const auto & key_name : group_by_key_names)
        key_col_positions.push_back(header->getPositionByName(key_name));

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
            has_filter,
            input_mapping,
            numeric_type_index,
            key_col_positions);

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
