#include <Processors/QueryPlan/ReadFromGPUResidentColumns.h>

#if USE_GPU

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <GPU/GPUAggregation.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Common/JSONBuilder.h>

#include <atomic>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

namespace
{

using ColumnToSum = ReadFromGPUResidentColumns::ColumnToSum;

/// Everything the streams of this step share: the columns to sum, the parts still to be claimed,
/// and what reading one of them needs. Immutable but for `next_part`, which is how the streams
/// divide the parts between themselves.
struct SharedState
{
    SharedState(
        std::vector<ColumnToSum> columns_,
        DataPartsVector parts_,
        const MergeTreeData & data_,
        StorageSnapshotPtr storage_snapshot_,
        GPUColumnCachePtr cache_,
        ContextPtr context_,
        UUID table_uuid_)
        : columns(std::move(columns_))
        , parts(std::move(parts_))
        , data(data_)
        , storage(data_.shared_from_this())
        , storage_snapshot(std::move(storage_snapshot_))
        , cache(std::move(cache_))
        , context(std::move(context_))
        , table_uuid(table_uuid_)
    {
    }

    std::vector<ColumnToSum> columns;
    DataPartsVector parts;

    const MergeTreeData & data;

    /// The same table as `data`, as something a cache entry can hold: an entry outlives the query
    /// and has to keep the table alive along with the part - see `GPUResidentColumn`.
    ConstStoragePtr storage;

    StorageSnapshotPtr storage_snapshot;
    GPUColumnCachePtr cache;
    ContextPtr context;
    UUID table_uuid;

    /// Where the next stream to ask takes its part from. The only thing here that changes.
    std::atomic<size_t> next_part{0};
};

using SharedStatePtr = std::shared_ptr<SharedState>;

/// Emits one chunk per part, holding that part's own sum of each column, claiming parts from the
/// shared queue above - the shape `ReadFromTextIndexCount`'s source has, for the same reason: the
/// per-part work is independent, and how many parts there are is not known to be divisible evenly.
///
/// The reading is done by several of these at once; the device work inside them is not concurrent,
/// because `GPU::DeviceBuffer` makes every call over the boundary under one process-wide lock -
/// see `GPU::lockDevice` for why serializing there costs nothing.
class GPUResidentColumnsSource : public ISource
{
public:
    GPUResidentColumnsSource(SharedHeader header_, SharedStatePtr state_, QueryStatusPtr query_status_)
        : ISource(std::move(header_))
        , state(std::move(state_))
        , query_status(std::move(query_status_))
    {
    }

    String getName() const override { return "GPUResidentColumns"; }

protected:
    Chunk generate() override
    {
        const size_t part_idx = state->next_part.fetch_add(1);
        if (part_idx >= state->parts.size())
            return {};

        checkNotCancelled();

        const DataPartPtr & part = state->parts[part_idx];

        /// Every buffer and every offset below is sized from this one number, and a cache entry is
        /// a whole part's column - so a part of no rows would mean a buffer of no bytes and a
        /// reduction over nothing. The pass leaves such parts out; reaching one here is a mistake
        /// in the pass rather than a query that cannot be answered this way.
        const size_t num_rows = part->rows_count;
        if (num_rows == 0)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Part {} holds no rows and should not have reached the GPU resident columns", part->name);

        /// One entry per column, whether it came from the cache or was just filled - so that the
        /// reduction below is the same call either way.
        std::vector<GPUColumnCache::MappedPtr> entries(state->columns.size());
        std::vector<size_t> missing;

        for (size_t i = 0; i < state->columns.size(); ++i)
        {
            entries[i] = state->cache->getForPart(keyOf(*part, state->columns[i]), part);

            if (!entries[i])
            {
                missing.push_back(i);
                continue;
            }

            /// The entry is this part's, so it holds this part's rows. Checked because the row
            /// count is what the reduction reads the buffer with: a disagreement would be a sum
            /// over the wrong number of values rather than an error.
            if (entries[i]->num_rows != num_rows)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Column {} of part {} is held on the device with {} values, where the part has {} rows",
                    state->columns[i].name,
                    part->name,
                    entries[i]->num_rows,
                    num_rows);
        }

        if (!missing.empty())
            readAndUpload(part, missing, entries);

        MutableColumns result_columns = getPort().getHeader().cloneEmptyColumns();
        if (result_columns.size() != entries.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "A row of {} per-part sums does not fit an output header of {} columns",
                entries.size(),
                result_columns.size());

        for (size_t i = 0; i < entries.size(); ++i)
        {
            const ColumnToSum & column = state->columns[i];

            /// The reduction. On a hit this is the whole of the work this step does for the part,
            /// and it reads nothing but device memory.
            result_columns[i]->insert(entries[i]->buffer.sum(column.element_type, column.sum_type, num_rows));
        }

        return Chunk(std::move(result_columns), 1);
    }

private:
    GPUColumnCacheKey keyOf(const IMergeTreeDataPart & part, const ColumnToSum & column) const
    {
        return GPUColumnCacheKey{.table_uuid = state->table_uuid, .part_name = part.name, .column_name = column.name};
    }

    void checkNotCancelled() const
    {
        if (isCancelled())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
        if (query_status)
            query_status->checkTimeLimit();
    }

    /// Reads the columns `missing` names out of `part` in one pass, copies each block straight into
    /// a device buffer of the part's full size, and puts the buffers in the cache.
    void readAndUpload(const DataPartPtr & part, const std::vector<size_t> & missing, std::vector<GPUColumnCache::MappedPtr> & entries)
    {
        const size_t num_rows = part->rows_count;

        Names columns_to_read;
        columns_to_read.reserve(missing.size());

        for (const size_t i : missing)
        {
            columns_to_read.push_back(state->columns[i].name);

            /// Allocated before anything is read, at the part's full size, so that a block can be
            /// copied to its place the moment it arrives and nothing is ever held in host memory
            /// beyond the block itself.
            entries[i] = std::make_shared<GPUResidentColumn>(state->storage, part, num_rows, state->columns[i].element_size);
        }

        /// The same reader a merge uses, over the whole part and in order. `apply_deleted_mask` is
        /// left on although the pass only accepts parts without a lightweight delete mask: were one
        /// to slip through, the read would return fewer rows than the part claims and the check
        /// below would fail loudly, where reading past the mask would quietly sum deleted rows.
        ///
        /// The alter conversions are empty because the pass refuses any part that needs them - an
        /// on-the-fly mutation, a patch part or a masking policy would all mean the values on disk
        /// are not the values the query must see.
        ///
        /// `MergeTreeSequentialSourceType` only chooses which throttler the read is subject to, and
        /// neither of the two is a query-read throttler. `Merge` is what the other query-time user
        /// of this reader picks, and `merges_throttler` is unset unless a server asks for it.
        Pipe pipe = createMergeTreeSequentialSource(
            MergeTreeSequentialSourceType::Merge,
            state->data,
            state->storage_snapshot,
            RangesInDataPart(part),
            std::make_shared<AlterConversions>(),
            /*merged_part_offsets=*/nullptr,
            columns_to_read,
            /*mark_ranges=*/std::nullopt,
            /*filtered_rows_count=*/nullptr,
            /*apply_deleted_mask=*/true,
            /*read_with_direct_io=*/false,
            /*prefetch=*/false);

        QueryPipeline pipeline(std::move(pipe));

        /// Ties the read to the query it is for: it is cancellable and killable, its rows and bytes
        /// are counted in the query's progress and in `system.query_log`, and they are charged to
        /// the query's own quota bucket rather than to a shared one.
        pipeline.setProcessListElement(state->context->getProcessListElement());
        pipeline.setProgressCallback(state->context->getProgressCallback());
        pipeline.setQuota(state->context->getQuota());
        pipeline.setNormalizedQueryHash(state->context->getNormalizedQueryHash());

        PullingPipelineExecutor executor(pipeline);

        Block block;
        size_t rows_read = 0;

        while (executor.pull(block))
        {
            const size_t block_rows = block.rows();
            if (block_rows == 0)
                continue;

            /// The buffers hold the part's rows and no more. A part that grew between the analysis
            /// and this read is not a thing that can happen - a part is immutable - so this is an
            /// invariant rather than a case.
            if (block_rows > num_rows - rows_read)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Reading part {} produced more than the {} rows it holds",
                    part->name,
                    num_rows);

            for (const size_t i : missing)
            {
                const ColumnToSum & column = state->columns[i];

                /// A column arriving from the reader is a `ColumnVector` of its type already,
                /// unless it is sparse - a representation around such a vector - and the device
                /// needs the values one after another either way. `LowCardinality` is not among
                /// the wrappers to strip, because such a column is not eligible for this path.
                const ColumnPtr values = block.getByName(column.name).column->convertToFullIfWrapped();
                const std::string_view raw = GPU::rawValuesOf(*values, block_rows, column.element_size);

                entries[i]->buffer.copyIn(rows_read * column.element_size, raw.data(), raw.size());
            }

            rows_read += block_rows;

            checkNotCancelled();
        }

        /// The row we are about to emit claims to be this part's whole sum, so a read that ended
        /// early may not be turned into one. Nothing here recovers from that: the only ways to get
        /// here are a part that disagrees with its own row count and a soft limit that stopped the
        /// read, and the pass refuses the queries that can carry such a limit.
        if (rows_read != num_rows)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Reading part {} produced {} rows, where the part holds {}",
                part->name,
                rows_read,
                num_rows);

        const size_t cache_size = state->cache->maxSizeInBytes();

        for (const size_t i : missing)
        {
            /// A column larger than the whole cache is summed and let go. Inserting it would evict
            /// everything else to make room for something that cannot be kept either, which is the
            /// worst of both: the cache would end up holding one column and having thrown away the
            /// ones that were being used.
            if (entries[i]->buffer.size() > cache_size)
                continue;

            /// Not under the device lock - an insertion evicts, an eviction frees device buffers,
            /// and freeing takes that lock.
            state->cache->setForPart(keyOf(*part, state->columns[i]), entries[i]);
        }
    }

    SharedStatePtr state;
    QueryStatusPtr query_status;
};

}

ReadFromGPUResidentColumns::ReadFromGPUResidentColumns(
    SharedHeader output_header_,
    std::vector<ColumnToSum> columns_,
    DataPartsVector parts_,
    const MergeTreeData & data_,
    StorageSnapshotPtr storage_snapshot_,
    GPUColumnCachePtr cache_,
    ContextPtr context_,
    size_t num_streams_)
    : ISourceStep(std::move(output_header_))
    , columns(std::move(columns_))
    , parts(std::move(parts_))
    , data(data_)
    , storage_snapshot(std::move(storage_snapshot_))
    , cache(std::move(cache_))
    , context(std::move(context_))
    , table_uuid(data_.getStorageID().uuid)
    , num_streams(num_streams_)
{
}

void ReadFromGPUResidentColumns::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto state = std::make_shared<SharedState>(columns, std::move(parts), data, storage_snapshot, cache, context, table_uuid);

    /// More streams than parts would be processors that claim nothing and exit, and at least one
    /// is needed to produce the empty result of a table with no parts.
    const size_t streams = std::max<size_t>(1, std::min(num_streams, state->parts.size()));

    Pipes pipes;
    pipes.reserve(streams);

    for (size_t i = 0; i < streams; ++i)
        pipes.emplace_back(std::make_shared<GPUResidentColumnsSource>(getOutputHeader(), state, settings.process_list_element));

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

void ReadFromGPUResidentColumns::describeActions(FormatSettings & format_settings) const
{
    format_settings.out << format_settings.detail_prefix << "Parts: " << parts.size() << "\n";
    format_settings.out << format_settings.detail_prefix << "Columns: ";

    for (size_t i = 0; i < columns.size(); ++i)
        format_settings.out << (i == 0 ? "" : ", ") << columns[i].name;

    format_settings.out << "\n";
}

void ReadFromGPUResidentColumns::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Parts", parts.size());

    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : columns)
        columns_array->add(column.name);

    map.add("Columns", std::move(columns_array));
}

}

#endif
