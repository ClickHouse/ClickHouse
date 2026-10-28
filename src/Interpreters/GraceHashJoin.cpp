#include <Compression/CompressedWriteBuffer.h>
#include <Core/Settings.h>
#include <Formats/NativeWriter.h>
#include <Formats/formatBlock.h>
#include <Interpreters/Context.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <base/FnTraits.h>
#include <Common/FailPoint.h>
#include <Common/SharedMutex.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include <algorithm>
#include <bit>
#include <limits>
#include <numeric>
#include <optional>
#include <fmt/format.h>


namespace CurrentMetrics
{
    extern const Metric TemporaryFilesForJoin;
}

namespace ProfileEvents
{
    extern const Event ExternalJoinCompressedBytes;
    extern const Event ExternalJoinUncompressedBytes;
    extern const Event ExternalJoinWritePart;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FAULT_INJECTED;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace FailPoints
{
    extern const char grace_hash_join_fail_in_delayed_block_read[];
}

namespace
{
    class AccumulatedBlockReader
    {
    public:
        AccumulatedBlockReader(
            TemporaryBlockStreamReaderHolder reader_, std::mutex & mutex_, size_t result_block_size_ = 0, bool inject_read_failure_ = false)
            : mutex(mutex_)
            , result_block_size(result_block_size_)
            , inject_read_failure(inject_read_failure_)
        {
            if (!reader_)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Reader is nullptr");
            readers.emplace_back(std::move(reader_));
        }

        AccumulatedBlockReader(std::deque<TemporaryBlockStreamReaderHolder> readers_, std::mutex & mutex_)
            : readers(std::move(readers_))
            , mutex(mutex_)
            , result_block_size(0)
            , inject_read_failure(false)
        {
            chassert(!readers.empty());
        }

        Block read()
        {
            std::lock_guard lock(mutex);

            if (eof)
                return {};

            Blocks blocks;
            size_t rows_read = 0;
            // One AccumulatedBlockReader is shared between concurrent DelayedJoinedBlocksWorkerTransform
            // threads. If reader->read() throws (e.g. a mid-read MEMORY_LIMIT_EXCEEDED cancels the
            // underlying ReadBuffer), mark the reader finished before propagating so a sibling worker
            // does not re-enter read() on the now-canceled buffer (which trips chassert(!isCanceled())).
            try
            {
                while (true)
                {
                    // Only the left (delayed) reader arms this, so the failure can only be injected
                    // into the reader that several workers share. Canceling the buffer before
                    // throwing reproduces what ReadBuffer::next does on a real mid-read failure.
                    if (inject_read_failure)
                        fiu_do_on(FailPoints::grace_hash_join_fail_in_delayed_block_read,
                        {
                            readers.front().getHolder()->cancel();
                            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure in delayed block read");
                        });

                    Block block = readers.front()->read();
                    rows_read += block.rows();
                    if (block.empty())
                    {
                        readers.pop_front();
                        if (!readers.empty())
                            continue;
                        eof = true;
                        return concatenateBlocks(blocks);
                    }
                    blocks.push_back(std::move(block));
                    if (rows_read >= result_block_size)
                        return concatenateBlocks(blocks);
                }
            }
            catch (...)
            {
                eof = true;
                throw;
            }
        }

    private:
        std::deque<TemporaryBlockStreamReaderHolder> readers;
        std::mutex & mutex;

        const size_t result_block_size;
        const bool inject_read_failure;
        bool eof = false;
    };

    std::deque<size_t> generateRandomPermutation(size_t from, size_t to)
    {
        size_t size = to - from;
        std::deque<size_t> indices(size);
        std::iota(indices.begin(), indices.end(), from);
        std::shuffle(indices.begin(), indices.end(), thread_local_rng);
        return indices;
    }

    // Try to apply @callback in the order specified in @indices
    // Until it returns true for each index in the @indices.
    void retryForEach(std::deque<size_t> indices, Fn<bool(size_t)> auto callback)
    {
        while (!indices.empty())
        {
            size_t bucket_index = indices.front();
            indices.pop_front();

            if (!callback(bucket_index))
                indices.push_back(bucket_index);
        }
    }
}

class GraceHashJoin::BucketRouting
{
    static constexpr size_t NO_CHILD = std::numeric_limits<size_t>::max();

    struct Node
    {
        size_t bucket_index;
        size_t first_child = NO_CHILD;
        size_t depth;
    };

public:
    explicit BucketRouting(size_t initial_buckets)
        : initial_mask(initial_buckets - 1)
    {
        const size_t initial_depth = std::countr_zero(initial_buckets);
        for (size_t i = 0; i < initial_buckets; ++i)
        {
            nodes.push_back({i, NO_CHILD, initial_depth});
            leaves.push_back(i);
        }
    }

    size_t depth(size_t bucket_index) const { return nodes[leaves[bucket_index]].depth; }

    void split(size_t bucket_index)
    {
        const size_t leaf = leaves[bucket_index];
        const size_t child_depth = nodes[leaf].depth + 1;
        const size_t first_child = nodes.size();
        nodes.push_back({bucket_index, NO_CHILD, child_depth});
        nodes.push_back({leaves.size(), NO_CHILD, child_depth});
        leaves[bucket_index] = first_child;
        leaves.push_back(first_child + 1);
        nodes[leaf].first_child = first_child;
    }

    Blocks scatter(const Names & keys, const Block & block) const
    {
        auto hashes = JoinCommon::computeJoinKeyHashes(keys, block);
        auto selector = JoinCommon::hashToSelector(
            hashes,
            [this](size_t hash)
            {
                size_t node = hash & initial_mask;
                while (nodes[node].first_child != NO_CHILD)
                    node = nodes[node].first_child + ((hash >> nodes[node].depth) & 1);
                return nodes[node].bucket_index;
            });
        return JoinCommon::scatterBlockBySelector(block, leaves.size(), selector);
    }

private:
    const size_t initial_mask;
    std::vector<Node> nodes;
    std::vector<size_t> leaves;
};

class GraceHashJoin::FileBucket : boost::noncopyable
{
    struct RightPartition
    {
        TemporaryBlockStreamHolder file;
        size_t rows;
        size_t bytes;
    };

    enum class State : uint8_t
    {
        WRITING_BLOCKS,
        JOINING_BLOCKS,
        FINISHED,
    };

public:
    using BucketLock = std::unique_lock<std::mutex>;

    explicit FileBucket(
        size_t bucket_index_,
        TemporaryBlockStreamHolder left_file_,
        TemporaryBlockStreamHolder right_file_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        LoggerPtr log_)
        : idx(bucket_index_)
        , left_file(std::move(left_file_))
        , right_file(std::move(right_file_))
        , tmp_data(std::move(tmp_data_))
        , state(State::WRITING_BLOCKS)
        , log(log_)
    {
    }

    void addLeftBlock(const Block & block)
    {
        std::unique_lock<std::mutex> lock(left_file_mutex);
        addBlockImpl(block, left_file, lock);
    }

    bool tryAddLeftBlock(const Block & block)
    {
        std::unique_lock<std::mutex> lock(left_file_mutex, std::try_to_lock);
        return addBlockImpl(block, left_file, lock);
    }

    bool tryAddRightBlock(const Block & block, bool inherited = false)
    {
        std::unique_lock<std::mutex> lock(right_file_mutex, std::try_to_lock);
        if (!lock.owns_lock())
            return false;

        if (inherited)
        {
            /// Ancestors are read before descendants, so this prefix also preserves right-side input order.
            if (!right_prefix_file)
                right_prefix_file.emplace(std::make_shared<const Block>(right_file.getHeader()), tmp_data);
            addBlockImpl(block, *right_prefix_file, lock);
        }
        else
            addBlockImpl(block, right_file, lock);
        right_rows += block.rows();
        right_bytes += block.bytes();
        return true;
    }

    /// Returns the planning estimate (rows, logical bytes) that the new sibling inherits, see `inherited_rows_estimate`.
    std::pair<size_t, size_t> sealRightPartition()
    {
        std::lock_guard lock(right_file_mutex);
        ensureState(State::WRITING_BLOCKS);
        /// Nothing was written since the last seal, so there is nothing to keep apart from the children's rows.
        if (right_rows.load() > 0)
        {
            TemporaryBlockStreamHolder new_file(std::make_shared<const Block>(right_file.getHeader()), tmp_data);
            right_files.reserve(right_files.size() + 1);
            right_file.finishWriting();
            right_files.push_back({std::move(right_file), right_rows.load(), right_bytes.load()});
            right_file = std::move(new_file);
        }
        const std::pair<size_t, size_t> child_estimate{
            (right_rows.load() + inherited_rows_estimate.load()) / 2, (right_bytes.load() + inherited_bytes_estimate.load()) / 2};
        right_rows = 0;
        right_bytes = 0;
        inherited_rows_estimate = child_estimate.first;
        inherited_bytes_estimate = child_estimate.second;
        return child_estimate;
    }

    void setInheritedEstimate(std::pair<size_t, size_t> estimate)
    {
        inherited_rows_estimate = estimate.first;
        inherited_bytes_estimate = estimate.second;
    }

    /// Rows and logical bytes estimated for this leaf: written directly since the last seal plus inherited. For split planning only.
    std::pair<size_t, size_t> plannedRightSize() const
    {
        return {right_rows.load() + inherited_rows_estimate.load(), right_bytes.load() + inherited_bytes_estimate.load()};
    }

    bool empty() const { return is_empty.load(); }

    AccumulatedBlockReader startJoining()
    {
        LOG_TRACE(log, "Joining file bucket {}", idx);
        {
            std::unique_lock<std::mutex> left_lock(left_file_mutex);
            std::unique_lock<std::mutex> right_lock(right_file_mutex);

            state = State::JOINING_BLOCKS;
        }
        std::deque<TemporaryBlockStreamReaderHolder> readers;
        if (right_prefix_file)
            readers.emplace_back(right_prefix_file->getReadStream());
        for (const auto & file : right_files)
        {
            LOG_TRACE(log, "Reading historical right partition of bucket {}: {} rows, {}", idx, file.rows, ReadableSize(file.bytes));
            readers.emplace_back(file.file.getReadStream());
        }
        readers.emplace_back(right_file.getReadStream());
        return AccumulatedBlockReader(std::move(readers), right_file_mutex);
    }

    AccumulatedBlockReader getLeftTableReader()
    {
        ensureState(State::JOINING_BLOCKS);
        return AccumulatedBlockReader(left_file.getReadStream(), left_file_mutex, 0, /*inject_read_failure_=*/true);
    }

    /// Spilled bytes for this bucket. Only called after the join
    /// has finished, when no thread is writing to the files.
    TemporaryDataBuffer::Stat leftSpillStat() const { return left_file.getHolder()->getStat(); }
    TemporaryDataBuffer::Stat rightSpillStat() const
    {
        auto result = right_file.getHolder()->getStat();
        auto add_stat = [&](const TemporaryBlockStreamHolder & file)
        {
            const auto file_stat = file.getHolder()->getStat();
            result.compressed_size += file_stat.compressed_size;
            result.uncompressed_size += file_stat.uncompressed_size;
        };
        if (right_prefix_file)
            add_stat(*right_prefix_file);
        for (const auto & file : right_files)
            add_stat(file.file);
        return result;
    }

    const size_t idx;

private:
    bool addBlockImpl(const Block & block, TemporaryBlockStreamHolder & writer, std::unique_lock<std::mutex> & lock)
    {
        ensureState(State::WRITING_BLOCKS);

        if (!lock.owns_lock())
            return false;

        if (block.rows())
            is_empty = false;

        writer->write(block);
        return true;
    }

    void transition(State expected, State desired)
    {
        State prev = state.exchange(desired);
        if (prev != expected)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid state transition from {} (got {}) to {}", expected, prev, desired);
    }

    void ensureState(State expected) const
    {
        State cur_state = state.load();
        if (cur_state != expected)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid state transition, expected {}, got {}", expected, state.load());
    }

    TemporaryBlockStreamHolder left_file;
    TemporaryBlockStreamHolder right_file;
    TemporaryDataOnDiskScopePtr tmp_data;
    std::vector<RightPartition> right_files;
    std::optional<TemporaryBlockStreamHolder> right_prefix_file;
    std::atomic<size_t> right_rows = 0;
    std::atomic<size_t> right_bytes = 0;
    /// Rows and logical bytes of sealed ancestor files assumed to belong to this leaf: each split gives both children
    /// half of the parent's estimate. Historical rows need not divide evenly; this only plans splits of spilled buckets,
    /// and the hash-table checks when the leaf is loaded remain authoritative. Not part of any other row or byte accounting.
    std::atomic<size_t> inherited_rows_estimate = 0;
    std::atomic<size_t> inherited_bytes_estimate = 0;
    mutable std::mutex left_file_mutex;
    mutable std::mutex right_file_mutex;

    std::atomic_bool is_empty = true;

    std::atomic<State> state;

    LoggerPtr log;
};

namespace
{

template <JoinTableSide table_side>
void flushBlocksToBuckets(Blocks & blocks, const GraceHashJoin::Buckets & buckets, size_t except_index = 0, bool inherited_right = false)
{
    chassert(blocks.size() == buckets.size());
    retryForEach(
        generateRandomPermutation(0, buckets.size()),
        [&](size_t i)
        {
            /// Skip empty and current bucket
            if (!blocks[i].rows() || i == except_index)
                return true;

            bool flushed = false;
            if constexpr (table_side == JoinTableSide::Left)
                flushed = buckets[i]->tryAddLeftBlock(blocks[i]);
            if constexpr (table_side == JoinTableSide::Right)
            {
                if (inherited_right)
                    chassert(i > except_index);
                flushed = buckets[i]->tryAddRightBlock(blocks[i], inherited_right);
            }

            if (flushed)
                blocks[i].clear();

            return flushed;
        });
}
}

GraceHashJoin::GraceHashJoin(
    size_t initial_num_buckets_,
    size_t max_num_buckets_,
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader left_sample_block_,
    SharedHeader right_sample_block_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    bool any_take_last_row_,
    size_t external_join_threshold_)
    : log{getLogger("GraceHashJoin")}
    , table_join{std::move(table_join_)}
    , left_sample_block{left_sample_block_}
    , right_sample_block{right_sample_block_}
    , any_take_last_row{any_take_last_row_}
    , initial_num_buckets(initial_num_buckets_)
    , max_num_buckets(max_num_buckets_)
    , external_join_threshold(external_join_threshold_)
    , left_key_names(table_join->getOnlyClause().key_names_left)
    , right_key_names(table_join->getOnlyClause().key_names_right)
    , tmp_data(tmp_data_->childScope({
            .current_metric = CurrentMetrics::TemporaryFilesForJoin,
            .bytes_compressed = ProfileEvents::ExternalJoinCompressedBytes,
            .bytes_uncompressed = ProfileEvents::ExternalJoinUncompressedBytes,
            .num_files = ProfileEvents::ExternalJoinWritePart,
            .spilled_to_disk_operator = "join",
        }, table_join->temporaryFilesBufferSize(), table_join->temporaryFilesCodec()))
    , hash_join(makeInMemoryJoin("grace0"))
    , hash_join_sample_block(hash_join->savedBlockSample())
{
    /// Nothing is inserted yet. `HashJoin` allocates its maps in its constructor (`dataMapInit`), so this is the
    /// initial buffer that every unreserved bucket starts with. Its arena allocates lazily and grows with the data.
    empty_build_bytes = hash_join->getTotalByteCount();

    if (!GraceHashJoin::isSupported(table_join))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "GraceHashJoin is not supported for this join type");

    /// In legacy mode the size limits still drive spilling, so there is nothing to require here.
    if (external_join_threshold == 0 && !table_join->legacyJoinSizeLimitsTriggerSpilling())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "join_algorithm = 'grace_hash' is external from the first block and needs a spill threshold, but neither "
            "max_bytes_before_external_join nor max_bytes_ratio_before_external_join resolved to a non-zero value. Set "
            "max_bytes_before_external_join, or set max_bytes_ratio_before_external_join on a server that has memory limits "
            "configured (the ratio is ignored without them). Leaving join_algorithm at its default lets ClickHouse "
            "pick an algorithm that fits the settings you have");
}

void GraceHashJoin::initBuckets()
{
    if (!buckets.empty())
        return;

    size_t initial_rounded_num_buckets = roundUpToPowerOfTwoOrZero(
        std::clamp<size_t>(initial_num_buckets, 1, max_num_buckets));
    initial_rounded_num_buckets = std::min(initial_rounded_num_buckets, std::bit_floor(max_num_buckets));

    addBuckets(initial_rounded_num_buckets);
    bucket_routing = std::make_shared<BucketRouting>(initial_rounded_num_buckets);

    if (buckets.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No buckets created");

    LOG_TRACE(log, "Initialize {} bucket{}", buckets.size(), buckets.size() > 1 ? "s" : "");

    current_bucket = buckets.front().get();
    current_bucket->startJoining();
}

bool GraceHashJoin::isSupported(const std::shared_ptr<TableJoin> & table_join)
{
    bool is_asof = (table_join->strictness() == JoinStrictness::Asof);
    auto kind = table_join->kind();
    return !is_asof && (isInner(kind) || isLeft(kind) || isRight(kind) || isFull(kind)) && table_join->oneDisjunct();
}

GraceHashJoin::~GraceHashJoin() = default;

bool GraceHashJoin::addBlockToJoin(const Block & block, size_t /*num_rows*/, JoinBuildContext context)
{
    if (current_bucket == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "GraceHashJoin is not initialized");

    /// Another thread already latched the stop below, so the bucket must stay exactly as it was then:
    /// a repartition here would move half of it into buckets `getDelayedBlocks` is about to drop.
    if (stop_after_current_bucket)
        return false;

    addBlockToJoinImpl(materializeBlock(block), context);

    /// In legacy mode these limits make us spill instead (see `hasMemoryOverflow`), so don't fail on them.
    if (!context.joinChecksLimits() || table_join->legacyJoinSizeLimitsTriggerSpilling())
        return true;

    /// Spilling does not earn a query the right to go over the limits.
    if (checkSizeLimits())
        return true;

    /// `join_overflow_mode = 'break'`: the caller stops feeding this side, but the buckets already on disk
    /// would still be joined in `getDelayedBlocks` - well past the cap. Latch the stop here as well, so
    /// the join ends with the bucket in memory, the way `HashJoin` ends with the block that crossed the cap.
    stop_after_current_bucket = true;
    return false;
}

bool GraceHashJoin::checkSizeLimits() const
{
    /// Count what the hash tables hold, exactly as `HashJoin` does when it checks the same limits: the
    /// buckets built so far plus the one in memory now. Rows a bucket drops (NULL keys) or collapses
    /// behind one key are not counted, and the map overhead is. A bucket still on disk has no hash table
    /// yet, so a spilled join reaches the cap only once that bucket is loaded in `getDelayedBlocks`.
    return table_join->sizeLimits().check(
        accounted_right_rows + getTotalRowCount(),
        accounted_right_bytes + getTotalByteCount(),
        "JOIN",
        ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

bool GraceHashJoin::forcedSpillPending() const
{
    /// A spill the scheduler asked for is a hint, so it must never fail the query on the bucket limit.
    /// The request is kept for later rather than cleared: a bucket too small to split may still grow.
    return force_spill && canForceRepartition();
}

bool GraceHashJoin::hasMemoryOverflow(size_t total_rows, size_t total_bytes) const
{
    if (forcedSpillPending())
        return true;
    /// One row can't be split, avoid loop
    if (total_rows < 2)
        return false;
    /// Only the threshold decides when to spill now, the size limits are caps checked in `addBlockToJoin`.
    /// Half of it, because the hash table doubles in power-of-two steps and briefly holds 3x while resizing.
    bool has_overflow = external_join_threshold > 0 && total_bytes * 2 >= external_join_threshold;

    /// Legacy mode: the size limits spill as well. Standalone `grace_hash` is built with no threshold
    /// then, `SpillingHashJoin` still passes one, so whichever comes first wins for the adaptive path.
    if (!has_overflow && table_join->legacyJoinSizeLimitsTriggerSpilling())
        has_overflow = !table_join->sizeLimits().softCheck(total_rows, total_bytes);

    if (has_overflow)
        LOG_TRACE(log, "Memory overflow at {} in {} rows (spill threshold {}, max_bytes_in_join {}, max_rows_in_join {})",
            ReadableSize(total_bytes), total_rows, ReadableSize(external_join_threshold),
            ReadableSize(table_join->sizeLimits().max_bytes), table_join->sizeLimits().max_rows);

    return has_overflow;
}

bool GraceHashJoin::canSplitBucket(size_t bucket_index) const
{
    return buckets.size() < max_num_buckets && bucket_routing->depth(bucket_index) < std::numeric_limits<UInt32>::digits;
}

GraceHashJoin::BucketPtr GraceHashJoin::splitBucket(size_t bucket_index, bool spilled)
{
    const size_t to_size = buckets.size() + 1;

    if (to_size > max_num_buckets)
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Too many grace hash join buckets ({} > {}). Raise grace_hash_join_max_buckets, or raise the spill "
            "threshold max_bytes_before_external_join / max_bytes_ratio_before_external_join: a threshold below "
            "the size of an empty hash table can never be reached, so the buckets keep splitting",
            to_size,
            max_num_buckets);
    }

    if (bucket_routing->depth(bucket_index) >= std::numeric_limits<UInt32>::digits)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Cannot split grace hash join bucket {} beyond the join-key hash width", bucket_index);

    auto new_routing = std::make_shared<BucketRouting>(*bucket_routing);
    new_routing->split(bucket_index);
    /// Prepare all allocations before sealing the parent or publishing new routing.
    buckets.reserve(to_size);
    TemporaryBlockStreamHolder left_file(left_sample_block, tmp_data);
    TemporaryBlockStreamHolder right_file(std::make_shared<const Block>(prepareRightBlock(*right_sample_block)), tmp_data);
    auto sibling = std::make_shared<FileBucket>(buckets.size(), std::move(left_file), std::move(right_file), tmp_data, log);
    /// An in-memory split writes the sibling's rows physically, so only a spilled split leaves an estimate to inherit.
    if (spilled)
        sibling->setInheritedEstimate(buckets[bucket_index]->sealRightPartition());

    buckets.emplace_back(sibling);
    bucket_routing = std::move(new_routing);
    ++stats.num_rehashes;

    LOG_TRACE(log, "Splitting bucket {} into {} and {}, {} buckets", bucket_index, bucket_index, sibling->idx, to_size);
    return sibling;
}

size_t GraceHashJoin::estimateBuildBytes(size_t logical_bytes) const
{
    /// A heuristic, so relaxed ordering is enough.
    const double estimate
        = static_cast<double>(empty_build_bytes) + build_amplification.load(std::memory_order_relaxed) * static_cast<double>(logical_bytes);
    /// Saturate, so that callers can double the estimate without overflow.
    constexpr size_t max_estimate = std::numeric_limits<size_t>::max() / 2;
    if (estimate >= static_cast<double>(max_estimate))
        return max_estimate;
    return static_cast<size_t>(estimate);
}

void GraceHashJoin::updateBuildAmplification(size_t build_bytes)
{
    /// Spilled buckets do not split during delayed loading, so there is nothing left to plan.
    if (reading_delayed_buckets)
        return;
    /// A small table gives a noisy ratio. Once the map grows past its initial buffer, that buffer is freed, so
    /// subtracting `empty_build_bytes` is off by up to `empty_build_bytes`; 16 times that in logical bytes keeps the
    /// error below 1/16. The 64 KiB floor means thousands of narrow rows - several growth steps past the initial
    /// 256-cell map - and many blocks, so the allocation padding of a few small blocks does not dominate.
    if (resident_logical_bytes < std::max<size_t>(64 * 1024, 16 * empty_build_bytes))
        return;

    /// The resize that crossed the threshold can overshoot it several times, but a table of these logical bytes
    /// does reach it. Counting no more than the threshold, a spilled bucket splits once it holds as many logical
    /// bytes as this table; counting the overshoot would split spilled buckets that fit.
    const size_t sample_bytes
        = external_join_threshold > 0 ? std::min(build_bytes, external_join_threshold / 2 + external_join_threshold % 2) : build_bytes;
    const size_t table_bytes = sample_bytes > empty_build_bytes ? sample_bytes - empty_build_bytes : 0;
    /// The logical bytes are stored in the table as well, so the estimate never drops below them.
    const double amplification = std::max(1.0, static_cast<double>(table_bytes) / static_cast<double>(resident_logical_bytes));
    build_amplification.store(amplification, std::memory_order_relaxed);

    LOG_TRACE(
        log,
        "Build amplification {:.2f} from {} logical bytes and {} build bytes ({} empty) before a split",
        amplification,
        resident_logical_bytes,
        build_bytes,
        empty_build_bytes);
}

bool GraceHashJoin::hasSpilledBucketOverflow(size_t rows, size_t logical_bytes) const
{
    if (rows < 2)
        return false;
    /// A spilled bucket has no hash table yet. Same convention as `hasMemoryOverflow` for the estimated table.
    const size_t estimated_bytes = estimateBuildBytes(logical_bytes);
    if (external_join_threshold > 0 && estimated_bytes * 2 >= external_join_threshold)
        return true;
    return table_join->legacyJoinSizeLimitsTriggerSpilling() && !table_join->sizeLimits().softCheck(rows, estimated_bytes);
}

void GraceHashJoin::splitSpilledBuckets(std::vector<size_t> candidates, bool build_finished)
{
    if (candidates.empty())
        return;
    /// All splits take the hash-table lock first, so a spill hint cannot lose its last available leaf to another split.
    std::lock_guard hash_join_lock(hash_join_mutex);
    std::unique_lock lock(rehash_mutex);
    if (stop_after_current_bucket)
        return;

    /// During input each candidate splits at most once: its children are checked when later blocks reach them.
    /// After the build no rows arrive, so both halves of every split are checked again, until all leaves fit.
    /// This ends: nothing is added to a leaf any more, so a split leaves each half at most half of the parent's
    /// estimated rows and bytes, and the `rows < 2` guard ends every chain of splits. `canSplitBucket` also bounds
    /// the leaf count and the hash depth, and the loop stops after a round that splits nothing.
    while (!candidates.empty())
    {
        std::vector<size_t> split_halves;
        for (size_t bucket_index : candidates)
        {
            if (bucket_index == current_bucket->idx || !canSplitBucket(bucket_index))
                continue;
            const auto [rows, logical_bytes] = buckets[bucket_index]->plannedRightSize();
            /// A size estimate is a hint; the actual hash-table limits are checked when this leaf is loaded.
            if (!hasSpilledBucketOverflow(rows, logical_bytes))
                continue;
            LOG_TRACE(
                log,
                "Splitting spilled bucket {} {} with an estimated {} rows and {} logical bytes, estimated build size {}",
                bucket_index,
                build_finished ? "after the build" : "during input",
                rows,
                logical_bytes,
                ReadableSize(estimateBuildBytes(logical_bytes)));
            const auto sibling = splitBucket(bucket_index, true);
            if (build_finished)
            {
                split_halves.push_back(bucket_index);
                split_halves.push_back(sibling->idx);
            }
        }
        candidates = std::move(split_halves);
    }
}

void GraceHashJoin::addBuckets(const size_t bucket_count)
{
    // Exception can be thrown in number of cases:
    // - during creation of temporary files for buckets
    // - in CI tests, there is a certain probability of failure in allocating memory, see memory_tracker_fault_probability
    // Therefore, new buckets are added only after all of them created successfully,
    // otherwise we can end up having unexpected number of buckets

    const size_t current_size = buckets.size();
    Buckets tmp_buckets;
    tmp_buckets.reserve(bucket_count);
    for (size_t i = 0; i < bucket_count; ++i)
        try
        {
            TemporaryBlockStreamHolder left_file(left_sample_block, tmp_data);
            TemporaryBlockStreamHolder right_file(std::make_shared<const Block>(prepareRightBlock(*right_sample_block)), tmp_data);

            BucketPtr new_bucket
                = std::make_shared<FileBucket>(current_size + i, std::move(left_file), std::move(right_file), tmp_data, log);
            tmp_buckets.emplace_back(std::move(new_bucket));
        }
        catch (...)
        {
            LOG_ERROR(
                getLogger("GraceHashJoin"),
                "Can't create bucket {} due to error: {}",
                current_size + i,
                getCurrentExceptionMessage(false));
            throw;
        }

    buckets.reserve(buckets.size() + bucket_count);
    for (auto & bucket : tmp_buckets)
        buckets.emplace_back(std::move(bucket));
}

void GraceHashJoin::checkTypesOfKeys(const Block & block) const
{
    chassert(hash_join);
    hash_join->checkTypesOfKeys(block);
}

void GraceHashJoin::initialize(const Block & sample_block)
{
    left_sample_block = std::make_shared<const Block>(sample_block.cloneEmpty());
    output_sample_block = left_sample_block->cloneEmpty();
    auto res = hash_join->joinBlock(output_sample_block)->next();
    output_sample_block = std::move(res.block);
    initBuckets();
}

JoinResultPtr GraceHashJoin::joinBlock(Block block)
{
    /// Check if hash join post build optimizations could be performed.
    if (hash_join && getNumBuckets() <= 1)
    {
        std::lock_guard lock(hash_join_mutex);
        hash_join->runPostBuildPhase();
    }

    if (block.rows() == 0)
        return hash_join->joinBlock(block);

    materializeBlockInplace(block);

    /// number of buckets doesn't change after right table is split to buckets, i.e. read-only access to buckets
    /// so, no need to copy buckets here
    Blocks blocks = bucket_routing->scatter(left_key_names, block);

    block = std::move(blocks[current_bucket->idx]);
    flushBlocksToBuckets<JoinTableSide::Left>(blocks, buckets);
    blocks.clear();

    auto res = hash_join->joinBlock(std::move(block));
    return res;
}

void GraceHashJoin::setTotals(const Block & block)
{
    std::lock_guard lock(totals_mutex);
    IJoin::setTotals(block);
}

const Block & GraceHashJoin::getTotals() const
{
    std::lock_guard lock(totals_mutex);
    return IJoin::getTotals();
}

size_t GraceHashJoin::getTotalRowCount() const
{
    std::lock_guard lock(hash_join_mutex);
    /// A failed rehash leaves no in-memory join until the next `addBlockToJoinImpl` rebuilds one,
    /// and the spill scheduler reads these counts from another thread meanwhile.
    if (!hash_join)
        return 0;
    return hash_join->getTotalRowCount();
}

size_t GraceHashJoin::getSpillableBytes() const
{
    std::lock_guard lock(hash_join_mutex);
    /// Offer nothing once this bucket cannot be split any further, so that the scheduler moves on to a
    /// join that can still free memory instead of asking this one again.
    if (!canForceRepartition())
        return 0;
    return hash_join->getTotalByteCount();
}

size_t GraceHashJoin::getTotalByteCount() const
{
    std::lock_guard lock(hash_join_mutex);
    if (!hash_join)
        return 0;
    return hash_join->getTotalByteCount();
}

void GraceHashJoin::GraceHashJoinStats::foldIn(const HashJoin & in_memory_join)
{
    UInt64 right_table_rows = in_memory_join.getRightTableRowCount();
    right_rows += right_table_rows;
    unique_keys += in_memory_join.getTotalRowCount();
    peak_in_memory_bytes = std::max(peak_in_memory_bytes, in_memory_join.getPeakBuildBytes());

    if (const auto * match_stats = in_memory_join.getMatchStats())
    {
        left_rows_total += match_stats->getInputLeft();

        matched_left.add(match_stats->getMatchedLeft());
        matched_right.add(match_stats->getMatchedRight(right_table_rows));
    }
}

GraceHashJoin::GraceHashJoinStats GraceHashJoin::collectStats() const
{
    GraceHashJoinStats result = stats;

    if (current_bucket && hash_join)
        result.foldIn(*hash_join);

    Buckets buckets_snapshot = getCurrentBuckets();
    result.num_buckets = buckets_snapshot.size();
    for (const auto & bucket : buckets_snapshot)
    {
        result.left_spilled_compressed_bytes += bucket->leftSpillStat().compressed_size;
        result.right_spilled_compressed_bytes += bucket->rightSpillStat().compressed_size;
    }
    return result;
}

StepAnalysisReport GraceHashJoin::getAnalysisReport() const
{
    const GraceHashJoinStats stats_snapshot = collectStats();

    StepAnalysisReport report = buildMatchedRowsReport({
        .left_rows = stats_snapshot.left_rows_total,
        .matched_left = stats_snapshot.matched_left.get(),
        .right_rows = stats_snapshot.right_rows,
        .matched_right = stats_snapshot.matched_right.get()});

    MetricList hash_table_metrics;
    hash_table_metrics.emplace_back(MetricKey::UniqueKeys, stats_snapshot.unique_keys);
    hash_table_metrics.emplace_back(MetricKey::Memory, stats_snapshot.peak_in_memory_bytes);
    hash_table_metrics.emplace_back(MetricKey::Buckets, stats_snapshot.num_buckets);
    hash_table_metrics.emplace_back(MetricKey::Rehashes, stats_snapshot.num_rehashes);
    report.push_back({MetricGroupKey::HashTable, std::move(hash_table_metrics)});

    MetricList spill_metrics;
    spill_metrics.emplace_back(MetricKey::LeftSpilled, stats_snapshot.left_spilled_compressed_bytes);
    spill_metrics.emplace_back(MetricKey::RightSpilled, stats_snapshot.right_spilled_compressed_bytes);
    report.push_back({MetricGroupKey::Spill, std::move(spill_metrics)});

    return report;
}

bool GraceHashJoin::alwaysReturnsEmptySet() const
{
    if (!isInnerOrRight(table_join->kind()) && !(isLeft(table_join->kind()) && table_join->strictness() == JoinStrictness::Semi))
        return false;

    bool file_buckets_are_empty = [this]()
    {
        std::shared_lock lock(rehash_mutex);
        return std::all_of(buckets.begin(), buckets.end(), [](const auto & bucket) { return bucket->empty(); });
    }();

    if (!file_buckets_are_empty)
        return false;

    chassert(hash_join);
    bool hash_join_is_empty = hash_join->alwaysReturnsEmptySet();

    return hash_join_is_empty;
}

/// Each bucket are handled by the following steps
/// 1. build hash_join by the right side blocks.
/// 2. join left side with the hash_join,
/// 3. read right non-joined blocks from hash_join.
/// buckets are handled one by one, each hash_join will not be release before the right non-joined blocks are emitted.
///
/// There is a finished counter in JoiningTransform/DelayedJoinedBlocksWorkerTransform,
/// only one processor could take the non-joined blocks from right stream, and ensure all rows from
/// left stream have been emitted before this.
IBlocksStreamPtr
GraceHashJoin::getNonJoinedBlocks(const Block & left_sample_block_, const Block & result_sample_block_, UInt64 max_block_size_) const
{
    return hash_join->getNonJoinedBlocks(left_sample_block_, result_sample_block_, max_block_size_);
}

class GraceHashJoin::DelayedBlocks : public IBlocksStream
{
public:
    explicit DelayedBlocks(
        size_t current_bucket_,
        Buckets buckets_,
        InMemoryJoinPtr hash_join_,
        const Names & left_key_names_,
        const Names & right_key_names_,
        std::shared_ptr<const BucketRouting> bucket_routing_)
        : current_bucket(current_bucket_)
        , buckets(std::move(buckets_))
        , hash_join(std::move(hash_join_))
        , left_reader(buckets[current_bucket]->getLeftTableReader())
        , left_key_names(left_key_names_)
        , right_key_names(right_key_names_)
        , bucket_routing(std::move(bucket_routing_))
    {
    }

    Block nextImpl() override
    {
        JoinResultPtr not_processed = nullptr;
        std::shared_lock shared(eof_mutex);

        while (true)
        {
            if (not_processed)
            {
                auto res = not_processed->next();
                if (res.is_last && res.next_block)
                {
                    res.next_block->filterBySelector();
                    auto next_block = std::move(*res.next_block).getSourceBlock();
                    if (next_block.rows() > 0)
                    {
                        auto new_res = hash_join->joinBlock(std::move(next_block));
                        std::lock_guard lock(extra_block_mutex);
                        not_processed_results.emplace_back(std::move(new_res));
                    }
                }
                if (!res.is_last)
                {
                    std::lock_guard lock(extra_block_mutex);
                    not_processed_results.emplace_back(std::move(not_processed));
                }

                return std::move(res.block);
            }

            std::lock_guard lock(extra_block_mutex);
            if (not_processed_results.empty())
                break;

            not_processed = std::move(not_processed_results.front());
            not_processed_results.pop_front();
        }

        Block block;
        size_t num_buckets = buckets.size();
        size_t current_idx = buckets[current_bucket]->idx;

        do
        {
            // One DelayedBlocks is shared among multiple DelayedJoinedBlocksWorkerTransform.
            // There is a lock inside left_reader.read() .
            block = left_reader.read();
            if (block.empty())
            {
                shared.unlock();
                bool there_are_still_might_be_rows_to_process = false;
                {
                    /// The following race condition could happen without this mutex:
                    /// * we're called from `IBlocksStream::next()`
                    /// * another thread just read the last block from `left_reader` and now is in the process of or about to call `joinBlock()`
                    /// * it might be that `joinBlock()` will leave some rows in the `not_processed`
                    /// * but if the current thread will return now an empty block `finished` will be set to true in `IBlocksStream::next()` and
                    ///   these not processed rows will be lost
                    /// So we shouldn't finish execution while there is at least one in-flight `joinBlock()` call. Let's wait until we're alone
                    /// and double check if there are any not processed rows left.
                    std::unique_lock exclusive(eof_mutex);

                    std::lock_guard lock(extra_block_mutex);
                    if (!not_processed_results.empty())
                        there_are_still_might_be_rows_to_process = true;
                }
                return there_are_still_might_be_rows_to_process ? nextImpl() : Block();
            }

            // block comes from left_reader, need to join with right table to get the result.
            Blocks blocks = bucket_routing->scatter(left_key_names, block);
            block = std::move(blocks[current_idx]);

            /*
             * A local split keeps the current leaf's ID and appends its sibling.
             * Historical left rows can therefore move only to unprocessed descendants with larger IDs.
             */
            for (size_t bucket_idx = 0; bucket_idx < num_buckets; ++bucket_idx)
            {
                if (blocks[bucket_idx].rows() == 0)
                    continue;

                if (bucket_idx == current_idx) // Rows that are still in our bucket
                    continue;

                chassert(bucket_idx > current_idx);
                buckets[bucket_idx]->addLeftBlock(blocks[bucket_idx]);
            }
        } while (block.rows() == 0);

        auto res = hash_join->joinBlock(block);
        auto next = res->next();

        if (next.is_last && next.next_block)
        {
            next.next_block->filterBySelector();
            auto next_block = std::move(*next.next_block).getSourceBlock();
            if (next_block.rows() > 0)
            {
                auto new_res = hash_join->joinBlock(std::move(next_block));
                std::lock_guard lock(extra_block_mutex);
                not_processed_results.emplace_back(std::move(new_res));
            }
        }
        if (!next.is_last)
        {
            std::lock_guard lock(extra_block_mutex);
            not_processed_results.emplace_back(std::move(res));
        }

        return std::move(next.block);
    }

    const size_t current_bucket;
    Buckets buckets;
    InMemoryJoinPtr hash_join;

    AccumulatedBlockReader left_reader;

    Names left_key_names;
    Names right_key_names;
    std::shared_ptr<const BucketRouting> bucket_routing;

    std::mutex extra_block_mutex;
    std::list<JoinResultPtr> not_processed_results TSA_GUARDED_BY(extra_block_mutex);

    SharedMutex eof_mutex;
};

IBlocksStreamPtr GraceHashJoin::getDelayedBlocks()
{
    std::lock_guard current_bucket_lock(current_bucket_mutex);

    if (current_bucket == nullptr)
        return nullptr;

    reading_delayed_buckets = true;
    size_t bucket_idx = current_bucket->idx;

    if (hash_join)
    {
        stats.foldIn(*hash_join);
        /// This bucket is about to be released, so its share of the hard cap has to survive it.
        accounted_right_rows += hash_join->getTotalRowCount();
        accounted_right_bytes += hash_join->getTotalByteCount();
    }

    if (stop_after_current_bucket)
    {
        std::lock_guard hash_join_lock(hash_join_mutex);
        current_bucket = nullptr;
        return nullptr;
    }

    for (bucket_idx = bucket_idx + 1; bucket_idx < buckets.size(); ++bucket_idx)
    {
        if (buckets[bucket_idx]->empty())
        {
            LOG_TRACE(log, "Skipping empty bucket {}", bucket_idx);
            continue;
        }

        /// The preceding leaf can be much larger than this one, so do not reserve its full table here.
        {
            std::lock_guard hash_join_lock(hash_join_mutex);
            current_bucket = buckets[bucket_idx].get();
            hash_join = makeInMemoryJoin(fmt::format("grace{}", bucket_idx));
            resident_logical_bytes = 0;
        }
        auto right_reader = current_bucket->startJoining();
        size_t num_rows = 0; /// count rows that were written and rehashed
        for (Block block = right_reader.read(); !block.empty(); block = right_reader.read())
        {
            num_rows += block.rows();
            addBlockToJoinImpl(std::move(block), JoinBuildContext::serial());
        }
        {
            std::lock_guard hash_join_lock(hash_join_mutex);
            hash_join->onBuildPhaseFinish();
        }

        /// The same hard cap as during the build phase, now that this bucket's hash table is complete.
        /// `join_overflow_mode = 'break'` keeps this bucket, the way `HashJoin` keeps the block that crossed
        /// the cap, and stops before the next one.
        if (!table_join->legacyJoinSizeLimitsTriggerSpilling() && !checkSizeLimits())
            stop_after_current_bucket = true;

        LOG_TRACE(log, "Loaded bucket {} with {}(/{}) rows, {}",
            bucket_idx, hash_join->getTotalRowCount(), num_rows, ReadableSize(hash_join->getTotalByteCount()));

        return std::make_unique<DelayedBlocks>(current_bucket->idx, buckets, hash_join, left_key_names, right_key_names, bucket_routing);
    }

    LOG_TRACE(log, "Finished loading all {} buckets", buckets.size());

    std::lock_guard hash_join_lock(hash_join_mutex);
    current_bucket = nullptr;
    return nullptr;
}

GraceHashJoin::InMemoryJoinPtr GraceHashJoin::makeInMemoryJoin(const String & bucket_id, size_t reserve_num)
{
    auto join = std::make_unique<HashJoin>(
        table_join, right_sample_block, any_take_last_row, reserve_num, bucket_id, /*is_concurrent_hash_join*/ false);
    /// A bucket that outgrows memory is rebucketed, which reads its right blocks back out - and that
    /// can happen at any point, so these blocks are never dropped.
    join->keepRightBlocksForAnotherAlgorithm();
    return join;
}

Block GraceHashJoin::prepareRightBlock(const Block & block)
{
    return HashJoin::prepareRightBlock(block, hash_join_sample_block);
}

bool GraceHashJoin::canForceRepartition() const
{
    /// A forced split must not fail the query, so skip it once the bucket count is at the limit. Nor
    /// after `join_overflow_mode = 'break'` latched: the halves it would flush are about to be dropped.
    std::shared_lock lock(rehash_mutex);
    return hash_join && current_bucket && !stop_after_current_bucket && hash_join->getTotalRowCount() > 1
        && canSplitBucket(current_bucket->idx);
}

/// Split only the active leaf. Caller holds `hash_join_mutex`; `leftover` is not in the table yet.
void GraceHashJoin::repartitionCurrentBucket(size_t prev_keys_num, Block leftover, JoinBuildContext context)
{
    const size_t bucket_index = current_bucket->idx;
    std::unique_lock routing_lock(rehash_mutex);
    const size_t split_bit = bucket_routing->depth(bucket_index);
    auto sibling = splitBucket(bucket_index, false);
    force_spill = false;
    /// Capture the peak before the split releases the old hash table.
    stats.peak_in_memory_bytes = std::max(stats.peak_in_memory_bytes, hash_join->getPeakBuildBytes());
    updateBuildAmplification(hash_join->getTotalByteCount());
    /// `releaseJoinedBlocks` resets the join's data before it finishes allocating, so detach
    /// first: a throw must not leave `hash_join` pointing at a join whose data is gone.
    auto released_join = std::move(hash_join);
    auto right_blocks = released_join->releaseJoinedBlocks(/* restructure */ false);
    released_join.reset();

    Blocks retained;
    retained.reserve(right_blocks.size() + 1);
    size_t retained_rows = 0;
    auto partition = [&](const Block & right_block)
    {
        if (!right_block.rows())
            return;
        auto hashes = JoinCommon::computeJoinKeyHashes(right_key_names, right_block);
        auto selector = JoinCommon::hashToSelector(hashes, [split_bit](size_t hash) { return (hash >> split_bit) & 1; });
        auto parts = JoinCommon::scatterBlockBySelector(right_block, 2, selector);
        if (parts[1].rows())
        {
            /// Exclusive routing access excludes writers to the newly created sibling.
            const bool written = sibling->tryAddRightBlock(parts[1], reading_delayed_buckets);
            chassert(written);
        }
        retained_rows += parts[0].rows();
        if (parts[0].rows())
            retained.emplace_back(std::move(parts[0]));
    };

    /// Rebuild in input order, including a pre-insert overflow's incoming block last.
    for (auto & right_block : right_blocks)
    {
        partition(right_block);
        right_block.clear();
    }
    right_blocks.clear();
    partition(leftover);
    leftover.clear();

    /// Retained rows bound the key count; duplicate rows must not reserve more than the old key count.
    hash_join = makeInMemoryJoin(fmt::format("grace{}", bucket_index), std::min(prev_keys_num, retained_rows));
    resident_logical_bytes = 0;
    for (const auto & retained_block : retained)
    {
        hash_join->addBlockToJoin(retained_block, retained_block.rows(), context.callerChecksLimits());
        resident_logical_bytes += retained_block.bytes();
    }
}

void GraceHashJoin::addBlockToJoinImpl(Block block, JoinBuildContext context)
{
    block = prepareRightBlock(block);
    size_t bucket_index = current_bucket->idx;
    Block current_block;
    std::shared_ptr<const BucketRouting> previous_routing;
    std::vector<size_t> split_candidates;

    {
        /// Keep routing stable through the writes: a split cannot seal a file underneath an old selector.
        std::shared_lock routing_lock(rehash_mutex);
        previous_routing = bucket_routing;
        Blocks blocks = bucket_routing->scatter(right_key_names, block);
        if (!reading_delayed_buckets)
        {
            for (size_t i = 0; i < blocks.size(); ++i)
                if (i != bucket_index && blocks[i].rows())
                    split_candidates.push_back(i);
        }
        flushBlocksToBuckets<JoinTableSide::Right>(blocks, buckets, bucket_index, reading_delayed_buckets);
        /// Ordinary spill writes keep their parallelism; only likely splits need exclusive routing access.
        std::erase_if(
            split_candidates,
            [&](size_t index)
            {
                const auto [rows, logical_bytes] = buckets[index]->plannedRightSize();
                return !hasSpilledBucketOverflow(rows, logical_bytes);
            });
        current_block = std::move(blocks[bucket_index]);
    }
    block.clear();
    splitSpilledBuckets(std::move(split_candidates), /*build_finished=*/false);

    if (current_block.rows() == 0)
    {
        /// No rows for this bucket, but the scheduler asked us to spill: split what is in memory anyway,
        /// otherwise the request is dropped and it frees nothing.
        std::lock_guard lock(hash_join_mutex);
        if (forcedSpillPending())
            repartitionCurrentBucket(hash_join->getTotalRowCount(), {}, context);
        return;
    }

    // Add block to the in-memory join
    {
        std::lock_guard lock(hash_join_mutex);
        if (!hash_join)
        {
            hash_join = makeInMemoryJoin(fmt::format("grace{}", bucket_index));
            resident_logical_bytes = 0;
        }

        /// Another builder may have split the active leaf since this block was scattered.
        {
            std::shared_lock routing_lock(rehash_mutex);
            if (previous_routing->depth(bucket_index) != bucket_routing->depth(bucket_index))
            {
                Blocks blocks = bucket_routing->scatter(right_key_names, current_block);
                flushBlocksToBuckets<JoinTableSide::Right>(blocks, buckets, bucket_index, reading_delayed_buckets);
                current_block = std::move(blocks[bucket_index]);
                if (!current_block.rows())
                    return;
            }
        }
        auto prev_keys_num = hash_join->getTotalRowCount();
        size_t pre_total_bytes = hash_join->getTotalByteCount();

        /// Pre-check: rehash when the in-memory bucket alone is already past half of the
        /// owner-supplied auto-spill cap. The inner `HashJoin::addBlockToJoin` grows its
        /// hash buffer in power-of-two steps. Doubling from X to 2X transiently holds 3X
        /// (old buffer + new buffer being filled), so a post-add check can race with that
        /// doubling and observe the OOM only as an allocator exception.
        ///
        /// We trigger the rehash on the existing table size and deliberately do NOT factor
        /// in the incoming block: `block.allocatedBytes()` is a misleading lower bound on
        /// the actual hash-table cost (cell overhead, load-factor padding, resize peaks),
        /// so a tiny block could leave a near-full bucket bypassing the pre-check and OOM
        /// during the resize. Mirrors `SpillingHashJoin::addBlockToJoin`.
        const bool pre_threshold_overflow
            = prev_keys_num > 1 && external_join_threshold > 0 && pre_total_bytes * 2 >= external_join_threshold;

        bool block_added = false;
        if (!pre_threshold_overflow)
        {
            hash_join->addBlockToJoin(current_block, current_block.rows(), context.callerChecksLimits());
            block_added = true;
            resident_logical_bytes += current_block.bytes();
            size_t hash_join_total_keys = hash_join->getAndSetRightTableKeys();
            size_t hash_join_total_bytes = hash_join->getTotalByteCount();
            if (!hasMemoryOverflow(hash_join_total_keys, hash_join_total_bytes))
                return;
        }

        if (block_added)
            current_block = {};
        /// else: we did not add the block, so we must include it when re-scattering after rehash.

        repartitionCurrentBucket(prev_keys_num, std::move(current_block), context);

        /// One split per block, so a bucket can end the build phase above the threshold - a single huge block,
        /// or one whose rows nearly all belong here. The threshold says when to start spilling, it is not a
        /// ceiling on the bucket: splitting until it fits would fail outright on data one key cannot split.
    }
}

size_t GraceHashJoin::getNumBuckets() const
{
    std::shared_lock lock(rehash_mutex);
    return buckets.size();
}

GraceHashJoin::Buckets GraceHashJoin::getCurrentBuckets() const
{
    std::shared_lock lock(rehash_mutex);
    return buckets;
}

void GraceHashJoin::onBuildPhaseFinish()
{
    // It cannot be called concurrently with other IJoin methods
    if (!hash_join)
        return;

    /// The last spill the scheduler asked for may have arrived after the final block for this bucket.
    {
        std::lock_guard hash_join_lock(hash_join_mutex);
        if (current_bucket && forcedSpillPending())
            repartitionCurrentBucket(hash_join->getTotalRowCount(), {}, JoinBuildContext::serial());
    }

    /// Also consider the sibling written by the last in-memory split, which may receive no more input.
    std::vector<size_t> candidates(getNumBuckets());
    std::iota(candidates.begin(), candidates.end(), 0);
    splitSpilledBuckets(std::move(candidates), /*build_finished=*/true);

    std::lock_guard hash_join_lock(hash_join_mutex);
    hash_join->onBuildPhaseFinish();
}
}
