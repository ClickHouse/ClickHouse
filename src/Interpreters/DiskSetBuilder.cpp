#include <Interpreters/DiskSetBuilder.h>
#include <Interpreters/DiskSetImpl.h>

#include <Columns/ColumnsNumber.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
extern const Event ExternalSetMerge;
}

namespace DB
{

namespace ErrorCodes
{
extern const int QUERY_WAS_CANCELLED;
extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace FailPoints
{
extern const char disk_set_builder_stop_before_finish[];
}

namespace
{

/// With runs of about 16 MiB, the final merge stays narrow even for large sets, as with external `DISTINCT`.
constexpr size_t DEFAULT_BYTES_IN_RUN = DEFAULT_BLOCK_SIZE * 256;

}

size_t DiskSetBuilder::getMinBytesInRun(size_t max_bytes_before_external_set)
{
    /// Smaller runs would make the final merge wide, so only a spill threshold below the default
    /// run size makes runs smaller.
    return std::min(max_bytes_before_external_set, DEFAULT_BYTES_IN_RUN);
}

size_t
DiskSetBuilder::estimateMemoryToWriteRun(size_t max_bytes_before_external_set, size_t max_block_size, size_t key_bytes, size_t buffer_size)
{
    /// The estimate counts the run, an added column of keys with its sort permutation, and three buffers of
    /// temporary files.
    const size_t column_bytes = max_block_size * (key_bytes + sizeof(IColumn::Permutation::value_type));
    return getMinBytesInRun(max_bytes_before_external_set) + column_bytes + 3 * buffer_size;
}

/// Each column of keys is sorted and deduplicated on arrival and merged by the external sorter, which keeps
/// one row per key, in blocks of `max_block_size` rows. The sorter writes a run once it buffers at least
/// `getMinBytesInRun` bytes and tracked query memory exceeds `max_bytes_before_external_set`. Its final
/// merge writes the keys into a `DiskSetImpl`, checks the size limits after each merged block and, in the
/// `break` mode, stops at the block that reaches them.
template <typename Key>
class DiskSetBuilderImpl final : public DiskSetBuilder
{
public:
    DiskSetBuilderImpl(
        TemporaryDataOnDiskScopePtr tmp_data_,
        const SizeLimits & limits_,
        size_t max_bytes_before_external_set,
        size_t max_block_size,
        size_t min_free_disk_space_,
        QueryStatusPtr process_list_element);
    ~DiskSetBuilderImpl() override;

    void add(ColumnPtr keys) override;
    std::unique_ptr<DiskSet> finish() override;
    bool isTruncated() const override { return truncated; }

private:
    class Sink;

    /// Appends the next block of the sorter's output to the set and checks the size limits. The sorter
    /// also deduplicates the keys.
    void appendSortedDistinctKeys(const IColumn & keys);

    TemporaryDataOnDiskScopePtr tmp_data;
    SizeLimits limits;
    size_t min_free_disk_space;
    SharedHeader header;
    SortDescription description;

    /// Becomes true once the sorter finishes, by delivering all keys or by being stopped at the size limits.
    bool sorter_finished = false;

    /// Becomes true when the set reaches its size limits in the `break` overflow mode and keeps no more keys.
    bool truncated = false;

    /// The set is created with its file when the sorter delivers the first keys.
    std::unique_ptr<DiskSetImpl<Key>> set;
    QueryPipeline pipeline;

    /// The first `add` creates the executor. `PushingPipelineExecutor::finish` runs nothing for an executor
    /// that never received a chunk, so a builder without keys skips the sorter and finishes an empty set.
    std::unique_ptr<PushingPipelineExecutor> executor;
};

template <typename Key>
class DiskSetBuilderImpl<Key>::Sink final : public ISink
{
public:
    Sink(SharedHeader header_, DiskSetBuilderImpl & builder_)
        : ISink(std::move(header_))
        , builder(builder_)
    {
    }
    String getName() const override { return "DiskSetSink"; }

    Status prepare() override
    {
        /// Closing the input stops the merge once the set keeps no more keys.
        if (builder.truncated)
            input.close();
        return ISink::prepare();
    }

private:
    void consume(Chunk chunk) override { builder.appendSortedDistinctKeys(*chunk.getColumns().front()); }
    void onFinish() override { builder.sorter_finished = true; }
    DiskSetBuilderImpl & builder;
};

template <typename Key>
DiskSetBuilderImpl<Key>::DiskSetBuilderImpl(
    TemporaryDataOnDiskScopePtr tmp_data_,
    const SizeLimits & limits_,
    size_t max_bytes_before_external_set,
    size_t max_block_size,
    size_t min_free_disk_space_,
    QueryStatusPtr process_list_element)
    : tmp_data(std::move(tmp_data_))
    , limits(limits_)
    , min_free_disk_space(min_free_disk_space_)
    , header(std::make_shared<const Block>(Block{{ColumnVector<Key>::create(), std::make_shared<DataTypeNumber<Key>>(), "key"}}))
{
    chassert(max_bytes_before_external_set);
    description.emplace_back("key", 1, 1);

    /// `add` passes sorted unique chunks, which lets the sorter keep one row per key in its merges.
    auto merge_sort = std::make_shared<MergeSortingTransform>(
        header,
        description,
        max_block_size,
        /*max_block_bytes=*/0,
        /*limit=*/0,
        /*increase_sort_description_compile_attempts=*/false,
        /*max_bytes_before_remerge=*/0,
        /*remerge_lowered_memory_bytes_ratio=*/0,
        getMinBytesInRun(max_bytes_before_external_set),
        max_bytes_before_external_set,
        tmp_data,
        min_free_disk_space,
        /*threshold_tracker=*/nullptr,
        MergeSorter::Mode::MergeUniqueChunks,
        ProfileEvents::ExternalSetMerge);

    auto sink = std::make_shared<Sink>(header, *this);
    connect(merge_sort->getOutputs().front(), sink->getPort());

    auto processors = std::make_shared<Processors>(Processors{merge_sort, sink});
    pipeline = QueryPipeline({}, std::move(processors), &merge_sort->getInputs().front());
    pipeline.setProcessListElement(std::move(process_list_element));
    pipeline.disableReadProgress();
}

template <typename Key>
DiskSetBuilderImpl<Key>::~DiskSetBuilderImpl()
{
    if (executor)
    {
        try
        {
            executor->cancel();
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("DiskSetBuilder"), "Failed to cancel PushingPipelineExecutor");
        }
    }
}

template <typename Key>
void DiskSetBuilderImpl<Key>::add(ColumnPtr keys)
{
    auto block = header->cloneWithColumns({std::move(keys)});

    /// Rows with equal keys are identical, so it does not matter which one the unstable sort
    /// and the subsequent deduplication keep.
    sortBlockAndDeduplicate(block, description, IColumn::PermutationSortStability::Unstable);

    if (!executor)
        executor = std::make_unique<PushingPipelineExecutor>(pipeline);

    executor->push(std::move(block));
}

template <typename Key>
void DiskSetBuilderImpl<Key>::appendSortedDistinctKeys(const IColumn & keys)
{
    const auto & values = assert_cast<const ColumnVector<Key> &>(keys).getData();
    if (values.empty())
        return;

    if (!set)
        set = std::make_unique<DiskSetImpl<Key>>(tmp_data, min_free_disk_space);

    set->add({values.data(), values.size()});

    /// The limits apply to the distinct keys of the set and to the memory of its directory. In the
    /// `break` mode, the set keeps the keys of this block, as the set in memory keeps the keys of the
    /// block that reaches the limits.
    if (!limits.check(set->getTotalRowCount(), set->getTotalByteCount(), "IN-set", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
    {
        LOG_TRACE(getLogger("DiskSetBuilder"), "The set reached its size limits with {} keys, stopping the merge", set->getTotalRowCount());
        truncated = true;
    }
}

template <typename Key>
std::unique_ptr<DiskSet> DiskSetBuilderImpl<Key>::finish()
{
    if (executor)
    {
        /// The failpoint stops the executor as the time limit in the `break` overflow mode does.
        fiu_do_on(FailPoints::disk_set_builder_stop_before_finish, { executor->cancel(); });
        executor->finish();
        executor.reset();

        /// The executor stops without an exception at the time limit in the `break` overflow mode. A set
        /// without all its keys would give wrong results, so the query stops with an exception instead.
        if (!sorter_finished)
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled before the set of IN was written to disk");
    }
    pipeline = {};

    if (!set)
        set = std::make_unique<DiskSetImpl<Key>>();
    set->finishWriting();
    return std::move(set);
}

template <typename Key>
std::unique_ptr<DiskSetBuilder> createDiskSetBuilder(
    TemporaryDataOnDiskScopePtr tmp_data,
    const SizeLimits & limits,
    size_t max_bytes_before_external_set,
    size_t max_block_size,
    size_t min_free_disk_space,
    QueryStatusPtr process_list_element)
{
    return std::make_unique<DiskSetBuilderImpl<Key>>(
        std::move(tmp_data), limits, max_bytes_before_external_set, max_block_size, min_free_disk_space, std::move(process_list_element));
}

template std::unique_ptr<DiskSetBuilder>
createDiskSetBuilder<UInt8>(TemporaryDataOnDiskScopePtr, const SizeLimits &, size_t, size_t, size_t, QueryStatusPtr);
template std::unique_ptr<DiskSetBuilder>
createDiskSetBuilder<UInt16>(TemporaryDataOnDiskScopePtr, const SizeLimits &, size_t, size_t, size_t, QueryStatusPtr);
template std::unique_ptr<DiskSetBuilder>
createDiskSetBuilder<UInt32>(TemporaryDataOnDiskScopePtr, const SizeLimits &, size_t, size_t, size_t, QueryStatusPtr);
template std::unique_ptr<DiskSetBuilder>
createDiskSetBuilder<UInt64>(TemporaryDataOnDiskScopePtr, const SizeLimits &, size_t, size_t, size_t, QueryStatusPtr);
template std::unique_ptr<DiskSetBuilder>
createDiskSetBuilder<UInt128>(TemporaryDataOnDiskScopePtr, const SizeLimits &, size_t, size_t, size_t, QueryStatusPtr);
template std::unique_ptr<DiskSetBuilder>
createDiskSetBuilder<UInt256>(TemporaryDataOnDiskScopePtr, const SizeLimits &, size_t, size_t, size_t, QueryStatusPtr);

}
