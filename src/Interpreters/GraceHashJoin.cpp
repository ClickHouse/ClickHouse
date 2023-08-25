#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/TableJoin.h>

#include <Interpreters/TemporaryDataOnDisk.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include "GraceHashJoin.h"

#include <base/FnTraits.h>
#include <fmt/format.h>

#include <Formats/formatBlock.h>

#include <numeric>


namespace CurrentMetrics
{
    extern const Metric TemporaryFilesForJoin;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


namespace
{
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

class GraceHashJoin::FileBucket : boost::noncopyable
{
    enum class State : int
    {
        WRITING_BLOCKS,
        JOINING_BLOCKS,
        FINISHED,
    };

public:
    using BucketLock = std::unique_lock<std::mutex>;
    using TemporaryFileStreamImpl = GraceHashJoin::TemporaryFileStreamImpl;
    using TemporaryFileStreamImplPtr = GraceHashJoin::TemporaryFileStreamImplPtr;

    explicit FileBucket(
        size_t bucket_index_,
        TemporaryDataOnDisk & tmp_data_disk,
        const Block & left_stream_block,
        const Block & right_stream_block,
        size_t max_block_size_,
        Poco::Logger * log_)
        : idx(bucket_index_)
        , left_file(createTemporaryFileStream(tmp_data_disk, left_stream_block, max_block_size_))
        , right_file(createTemporaryFileStream(tmp_data_disk, right_stream_block, max_block_size_))
        , state{State::WRITING_BLOCKS}
        , log{log_}
    {}

    static TemporaryFileStreamImplPtr
    createTemporaryFileStream(TemporaryDataOnDisk & tmp_data_disk, const Block & header, size_t max_block_size)
    {
        return std::make_shared<TemporaryFileStreamImpl>(header, tmp_data_disk.createRawStream(), max_block_size);
    }

    inline void addLeftBlock(Block && block)
    {
        addBlockImpl(std::move(block), *left_file);
    }

    inline void addRightBlock(Block && block)
    {
        addBlockImpl(std::move(block), *right_file);
    }

    inline bool tryAddLeftBlock(Block && block)
    {
        return addBlockImpl(std::move(block), *left_file);
    }

    inline bool tryAddRightBlock(Block && block)
    {
        return addBlockImpl(std::move(block), *right_file);
    }

    bool finished() const
    {
        return left_file->isEof();
    }

    bool empty() const { return is_empty.load(); }

    void startJoining()
    {
        LOG_TRACE(log, "Joining file bucket {}", idx);
        {
            left_file->finishWriting();
            right_file->finishWriting();

            state = State::JOINING_BLOCKS;
        }
    }

    TemporaryFileStreamImplPtr getLeftTableReader()
    {
        ensureState(State::JOINING_BLOCKS);
        return left_file;
    }

    TemporaryFileStreamImplPtr getRightTableReader()
    {
        ensureState(State::JOINING_BLOCKS);
        return right_file;
    }

    const size_t idx;

private:
    bool addBlockImpl(Block && block, TemporaryFileStreamImpl & writer)
    {
        ensureState(State::WRITING_BLOCKS);

        if (block.rows())
            is_empty = false;

        writer.write(std::move(block));
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

    TemporaryFileStreamImplPtr left_file;
    TemporaryFileStreamImplPtr right_file;

    std::atomic_bool is_empty = true;

    std::atomic<State> state;

    Poco::Logger * log;
};

namespace
{

template <JoinTableSide table_side>
void flushBlocksToBuckets(Blocks & blocks, const GraceHashJoin::Buckets & buckets, size_t except_index = 0)
{
    chassert(blocks.size() == buckets.size());
    retryForEach(
        generateRandomPermutation(1, buckets.size()), // skipping 0 block, since we join it in memory w/o spilling on disk
        [&](size_t i)
        {
            /// Skip empty and current bucket
            if (!blocks[i].rows() || i == except_index)
                return true;

            bool flushed = false;
            if constexpr (table_side == JoinTableSide::Left)
                flushed = buckets[i]->tryAddLeftBlock(std::move(blocks[i]));
            if constexpr (table_side == JoinTableSide::Right)
                flushed = buckets[i]->tryAddRightBlock(std::move(blocks[i]));

            if (flushed)
                blocks[i].clear();

            return flushed;
        });
}
}

GraceHashJoin::GraceHashJoin(
    ContextPtr context_, std::shared_ptr<TableJoin> table_join_,
    const Block & left_sample_block_,
    const Block & right_sample_block_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    bool any_take_last_row_)
    : log{&Poco::Logger::get("GraceHashJoin")}
    , context{context_}
    , table_join{std::move(table_join_)}
    , left_sample_block{left_sample_block_}
    , right_sample_block{right_sample_block_}
    , any_take_last_row{any_take_last_row_}
    , max_num_buckets{context->getSettingsRef().grace_hash_join_max_buckets}
    , max_block_size{context->getSettingsRef().max_block_size}
    , left_key_names(table_join->getOnlyClause().key_names_left)
    , right_key_names(table_join->getOnlyClause().key_names_right)
    , tmp_data(std::make_unique<TemporaryDataOnDisk>(tmp_data_, CurrentMetrics::TemporaryFilesForJoin))
    , hash_join(makeInMemoryJoin())
    , hash_join_sample_block(hash_join->savedBlockSample())
{
    if (!GraceHashJoin::isSupported(table_join))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "GraceHashJoin is not supported for this join type");
}

void GraceHashJoin::initBuckets()
{
    if (!buckets.empty())
        return;

    const auto & settings = context->getSettingsRef();

    size_t initial_num_buckets = roundUpToPowerOfTwoOrZero(std::clamp<size_t>(settings.grace_hash_join_initial_buckets, 1, settings.grace_hash_join_max_buckets));

    addBuckets(initial_num_buckets);

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

bool GraceHashJoin::addBlockToJoin(const Block & block, bool /*check_limits*/)
{
    if (current_bucket == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "GraceHashJoin is not initialized");

    Block materialized = materializeBlock(block);
    addBlockToJoinImpl(std::move(materialized));
    return true;
}

bool GraceHashJoin::hasMemoryOverflow(size_t total_rows, size_t total_bytes) const
{
    /// One row can't be split, avoid loop
    if (total_rows < 2)
        return false;
    bool has_overflow = !table_join->sizeLimits().softCheck(total_rows, total_bytes);

    if (has_overflow)
        LOG_TRACE(log, "Memory overflow, size exceeded {} / {} bytes, {} / {} rows",
            ReadableSize(total_bytes), ReadableSize(table_join->sizeLimits().max_bytes),
            total_rows, table_join->sizeLimits().max_rows);

    return has_overflow;
}

bool GraceHashJoin::hasMemoryOverflow(const BlocksList & blocks) const
{
    size_t total_rows = 0;
    size_t total_bytes = 0;
    for (const auto & block : blocks)
    {
        total_rows += block.rows();
        total_bytes += block.allocatedBytes();
    }
    return hasMemoryOverflow(total_rows, total_bytes);
}

bool GraceHashJoin::hasMemoryOverflow(const InMemoryJoinPtr & hash_join_) const
{
    size_t total_rows = hash_join_->getTotalRowCount();
    size_t total_bytes = hash_join_->getTotalByteCount();

    return hasMemoryOverflow(total_rows, total_bytes);
}

GraceHashJoin::Buckets GraceHashJoin::rehashBuckets()
{
    std::unique_lock lock(rehash_mutex);

    if (!isPowerOf2(buckets.size())) [[unlikely]]
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of buckets should be power of 2 but it's {}", buckets.size());

    const size_t to_size = buckets.size() * 2;
    size_t current_size = buckets.size();

    if (to_size > max_num_buckets)
    {
        throw Exception(
            ErrorCodes::LIMIT_EXCEEDED,
            "Too many grace hash join buckets ({} > {}), "
            "consider increasing grace_hash_join_max_buckets or max_rows_in_join/max_bytes_in_join",
            to_size,
            max_num_buckets);
    }

    LOG_TRACE(log, "Rehashing from {} to {}", current_size, to_size);

    addBuckets(to_size - current_size);

    return buckets;
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
            BucketPtr new_bucket = std::make_shared<FileBucket>(
                current_size + i, *tmp_data, left_sample_block, prepareRightBlock(right_sample_block), max_block_size, log);
            tmp_buckets.emplace_back(std::move(new_bucket));
        }
        catch (...)
        {
            LOG_ERROR(
                &Poco::Logger::get("GraceHashJoin"),
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
    return hash_join->checkTypesOfKeys(block);
}

void GraceHashJoin::initialize(const Block & sample_block)
{
    left_sample_block = sample_block.cloneEmpty();
    output_sample_block = left_sample_block.cloneEmpty();
    ExtraBlockPtr not_processed;
    hash_join->joinBlock(output_sample_block, not_processed);
    initBuckets();
}

void GraceHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed)
{
    if (block.rows() == 0)
    {
        hash_join->joinBlock(block, not_processed);
        return;
    }

    materializeBlockInplace(block);

    /// number of buckets doesn't change after right table is split to buckets, i.e. read-only access to buckets
    /// so, no need to copy buckets here
    size_t num_buckets = getNumBuckets();
    Blocks blocks = JoinCommon::scatterBlockByHash(left_key_names, block, num_buckets);

    block = std::move(blocks[current_bucket->idx]);

    hash_join->joinBlock(block, not_processed);
    if (not_processed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unhandled not processed block in GraceHashJoin");

    flushBlocksToBuckets<JoinTableSide::Left>(blocks, buckets);
}

void GraceHashJoin::setTotals(const Block & block)
{
    if (block.rows() > 0)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Totals are not supported for GraceHashJoin, got '{}'", block.dumpStructure());
}

size_t GraceHashJoin::getTotalRowCount() const
{
    std::lock_guard lock(hash_join_mutex);
    assert(hash_join);
    return hash_join->getTotalRowCount();
}

size_t GraceHashJoin::getTotalByteCount() const
{
    std::lock_guard lock(hash_join_mutex);
    chassert(hash_join);
    return hash_join->getTotalByteCount();
}

bool GraceHashJoin::alwaysReturnsEmptySet() const
{
    if (!isInnerOrRight(table_join->kind()))
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
        FileBucket & current_bucket_,
        GraceHashJoin & grace_hash_join_)
        : grace_hash_join(grace_hash_join_)
        , left_reader(current_bucket_.getLeftTableReader())
        , right_reader(current_bucket_.getRightTableReader())
    {
    }

    Block nextImpl() override
    {
        Block block;
        do
        {
            // Wait data of the right file finish loading into hash table.
            // The data of right file is loaded currently.
            if (!checkAndReadRight())
            {
                continue;
            }
            // One DelayedBlocks is shared among multiple DelayedJoinedBlocksWorkerTransform.
            // There is a lock inside left_reader.read() .
            block = left_reader->read();
            if (!block)
            {
                return {};
            }

            /*
             * We need to filter out blocks that were written to the current bucket `B_{n}`
             * but then virtually moved to another bucket `B_{n+i}` on rehash.
             * Bucket `B_{n+i}` is waiting for the buckets with smaller index to be processed,
             * and rows can be moved only forward (because we increase hash modulo twice on each rehash),
             * so it is safe to add blocks.
             */
            ExtraBlockPtr not_processed;
            grace_hash_join.joinBlock(block, not_processed);
            if (not_processed)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported hash join type");
        } while (block.rows() == 0);
        return block;
    }

    GraceHashJoin & grace_hash_join;

    GraceHashJoin::TemporaryFileStreamImplPtr left_reader;
    GraceHashJoin::TemporaryFileStreamImplPtr right_reader;

    // Whether current thread finish loading the right file.
    std::atomic<bool> is_right_reader_finished = false;
    // The number of threads in reading right file.
    std::atomic<Int32> right_reader_count = 0;
    // This lock is used to reduce cpu cost.
    std::shared_mutex right_reader_finished_mutex;

    bool checkAndReadRight()
    {
        if (is_right_reader_finished)
        {
            // There are still other threads in reading right file, if right_reader_count != 0.
            if (right_reader_count == 0)
                return true;
            std::unique_lock lock(right_reader_finished_mutex);
            assert(right_reader_count == 0);
            return true;
        }

        // Allow multiple threads to read the right file.
        std::shared_lock lock(right_reader_finished_mutex);
        /// increase right_reader_count to indicate that there is a thread in reading right file.
        right_reader_count.fetch_add(1);
        while (Block block = right_reader->read())
        {
            grace_hash_join.addBlockToJoin(block, false);
        }
        is_right_reader_finished = true;
        // decrease right_reader_count. If it's the last thread, the value is 1.
        return right_reader_count.fetch_sub(1) == 1;
    }
};

IBlocksStreamPtr GraceHashJoin::getDelayedBlocks()
{
    std::lock_guard current_bucket_lock(current_bucket_mutex);

    if (current_bucket == nullptr)
        return nullptr;

    size_t bucket_idx = current_bucket->idx;

    size_t prev_keys_num = 0;
    if (hash_join && buckets.size() > 1)
    {
        prev_keys_num = hash_join->getTotalRowCount();
    }

    for (bucket_idx = bucket_idx + 1; bucket_idx < buckets.size(); ++bucket_idx)
    {
        current_bucket = buckets[bucket_idx].get();
        if (current_bucket->finished() || current_bucket->empty())
        {
            LOG_TRACE(log, "Skipping {} {} bucket {}",
                current_bucket->finished() ? "finished" : "",
                current_bucket->empty() ? "empty" : "",
                bucket_idx);
            continue;
        }

        hash_join = makeInMemoryJoin(prev_keys_num);
        current_bucket->startJoining();
        // The right stream data is loaded inside DelayedBlocks before any left stream data is read.
        return std::make_unique<DelayedBlocks>(*current_bucket, *this);
    }

    LOG_TRACE(log, "Finished loading all {} buckets", buckets.size());

    current_bucket = nullptr;
    return nullptr;
}

GraceHashJoin::InMemoryJoinPtr GraceHashJoin::makeInMemoryJoin(size_t reserve_num)
{
    auto ret = std::make_unique<InMemoryJoin>(table_join, right_sample_block, any_take_last_row, reserve_num);
    return std::move(ret);
}

Block GraceHashJoin::prepareRightBlock(const Block & block)
{
    return HashJoin::prepareRightBlock(block, hash_join_sample_block);
}

void GraceHashJoin::addBlockToJoinImpl(Block block)
{
    block = prepareRightBlock(block);
    Buckets buckets_snapshot = getCurrentBuckets();
    size_t bucket_index = current_bucket->idx;
    Block current_block;

    {
        Blocks blocks = JoinCommon::scatterBlockByHash(right_key_names, block, buckets_snapshot.size());
        flushBlocksToBuckets<JoinTableSide::Right>(blocks, buckets_snapshot, bucket_index);
        current_block = std::move(blocks[bucket_index]);
    }

    // Add block to the in-memory join
    if (current_block.rows() > 0)
    {
        std::lock_guard lock(hash_join_mutex);
        if (!hash_join)
            hash_join = makeInMemoryJoin();

        // buckets size has been changed in other threads. Need to scatter current_block again.
        // rehash could only happen under hash_join_mutex's scope.
        auto current_buckets = getCurrentBuckets();
        if (buckets_snapshot.size() != current_buckets.size())
        {
            LOG_TRACE(log, "mismatch buckets size. previous:{}, current:{}", buckets_snapshot.size(), getCurrentBuckets().size());
            Blocks blocks = JoinCommon::scatterBlockByHash(right_key_names, current_block, current_buckets.size());
            flushBlocksToBuckets<JoinTableSide::Right>(blocks, current_buckets, bucket_index);
            current_block = std::move(blocks[bucket_index]);
            if (!current_block.rows())
                return;
        }

        auto prev_keys_num = hash_join->getTotalRowCount();
        hash_join->addBlockToJoin(current_block, /* check_limits = */ false);

        if (!hasMemoryOverflow(hash_join))
            return;

        current_block = {};

        // Must use the latest buckets snapshot in case that it has been rehashed by other threads.
        buckets_snapshot = rehashBuckets();
        auto right_blocks = hash_join->releaseJoinedBlocks(/* restructure */ false);
        hash_join = nullptr;

        {
            Blocks current_blocks;
            current_blocks.reserve(right_blocks.size());
            for (const auto & right_block : right_blocks)
            {
                Blocks blocks = JoinCommon::scatterBlockByHash(right_key_names, right_block, buckets_snapshot.size());
                flushBlocksToBuckets<JoinTableSide::Right>(blocks, buckets_snapshot, bucket_index);
                current_blocks.emplace_back(std::move(blocks[bucket_index]));
            }

            if (current_blocks.size() == 1)
                current_block = std::move(current_blocks.front());
            else
                current_block = concatenateBlocks(current_blocks);
        }

        hash_join = makeInMemoryJoin(prev_keys_num);

        if (current_block.rows() > 0)
            hash_join->addBlockToJoin(current_block, /* check_limits = */ false);
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

}
