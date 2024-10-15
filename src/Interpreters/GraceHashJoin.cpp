#include <Compression/CompressedWriteBuffer.h>
#include <Formats/NativeWriter.h>
#include <Formats/formatBlock.h>
#include <Interpreters/Context.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <base/FnTraits.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <Core/Settings.h>

#include <numeric>
#include <fmt/format.h>


namespace CurrentMetrics
{
    extern const Metric TemporaryFilesForJoin;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 grace_hash_join_initial_buckets;
    extern const SettingsUInt64 grace_hash_join_max_buckets;
}

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    class AccumulatedBlockReader
    {
    public:
        AccumulatedBlockReader(TemporaryFileStream & reader_,
                               std::mutex & mutex_,
                               size_t result_block_size_ = 0)
            : reader(reader_)
            , mutex(mutex_)
            , result_block_size(result_block_size_)
        {
            if (!reader.isWriteFinished())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading not finished file");
        }

        Block read()
        {
            std::lock_guard lock(mutex);

            if (eof)
                return {};

            Blocks blocks;
            size_t rows_read = 0;
            do
            {
                Block block = reader.read();
                rows_read += block.rows();
                if (!block)
                {
                    eof = true;
                    if (blocks.size() == 1)
                        return blocks.front();
                    return concatenateBlocks(blocks);
                }
                blocks.push_back(std::move(block));
            } while (rows_read < result_block_size);

            if (blocks.size() == 1)
                return blocks.front();
            return concatenateBlocks(blocks);
        }

    private:
        TemporaryFileStream & reader;
        std::mutex & mutex;

        const size_t result_block_size;
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

class GraceHashJoin::FileBucket : boost::noncopyable
{
    enum class State : uint8_t
    {
        WRITING_BLOCKS,
        JOINING_BLOCKS,
        FINISHED,
    };

public:
    using BucketLock = std::unique_lock<std::mutex>;

    explicit FileBucket(size_t bucket_index_, TemporaryFileStream & left_file_, TemporaryFileStream & right_file_, LoggerPtr log_)
        : idx{bucket_index_}
        , left_file{left_file_}
        , right_file{right_file_}
        , state{State::WRITING_BLOCKS}
        , log{log_}
    {
    }

    void addLeftBlock(const Block & block)
    {
        std::unique_lock<std::mutex> lock(left_file_mutex);
        addBlockImpl(block, left_file, lock);
    }

    void addRightBlock(const Block & block)
    {
        std::unique_lock<std::mutex> lock(right_file_mutex);
        addBlockImpl(block, right_file, lock);
    }

    bool tryAddLeftBlock(const Block & block)
    {
        std::unique_lock<std::mutex> lock(left_file_mutex, std::try_to_lock);
        return addBlockImpl(block, left_file, lock);
    }

    bool tryAddRightBlock(const Block & block)
    {
        std::unique_lock<std::mutex> lock(right_file_mutex, std::try_to_lock);
        return addBlockImpl(block, right_file, lock);
    }

    bool finished() const
    {
        std::unique_lock<std::mutex> left_lock(left_file_mutex);
        return left_file.isEof();
    }

    bool empty() const { return is_empty.load(); }

    AccumulatedBlockReader startJoining()
    {
        LOG_TRACE(log, "Joining file bucket {}", idx);
        {
            std::unique_lock<std::mutex> left_lock(left_file_mutex);
            std::unique_lock<std::mutex> right_lock(right_file_mutex);

            left_file.finishWriting();
            right_file.finishWriting();

            state = State::JOINING_BLOCKS;
        }
        return AccumulatedBlockReader(right_file, right_file_mutex);
    }

    AccumulatedBlockReader getLeftTableReader()
    {
        ensureState(State::JOINING_BLOCKS);
        return AccumulatedBlockReader(left_file, left_file_mutex);
    }

    const size_t idx;

private:
    bool addBlockImpl(const Block & block, TemporaryFileStream & writer, std::unique_lock<std::mutex> & lock)
    {
        ensureState(State::WRITING_BLOCKS);

        if (!lock.owns_lock())
            return false;

        if (block.rows())
            is_empty = false;

        writer.write(block);
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

    TemporaryFileStream & left_file;
    TemporaryFileStream & right_file;
    mutable std::mutex left_file_mutex;
    mutable std::mutex right_file_mutex;

    std::atomic_bool is_empty = true;

    std::atomic<State> state;

    LoggerPtr log;
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
                flushed = buckets[i]->tryAddLeftBlock(blocks[i]);
            if constexpr (table_side == JoinTableSide::Right)
                flushed = buckets[i]->tryAddRightBlock(blocks[i]);

            if (flushed)
                blocks[i].clear();

            return flushed;
        });
}
}

GraceHashJoin::GraceHashJoin(
    ContextPtr context_,
    std::shared_ptr<TableJoin> table_join_,
    const Block & left_sample_block_,
    const Block & right_sample_block_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    bool any_take_last_row_)
    : log{getLogger("GraceHashJoin")}
    , context{context_}
    , table_join{std::move(table_join_)}
    , left_sample_block{left_sample_block_}
    , right_sample_block{right_sample_block_}
    , any_take_last_row{any_take_last_row_}
    , max_num_buckets{context->getSettingsRef()[Setting::grace_hash_join_max_buckets]}
    , left_key_names(table_join->getOnlyClause().key_names_left)
    , right_key_names(table_join->getOnlyClause().key_names_right)
    , tmp_data(std::make_unique<TemporaryDataOnDisk>(tmp_data_, CurrentMetrics::TemporaryFilesForJoin))
    , hash_join(makeInMemoryJoin("grace0"))
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

    size_t initial_num_buckets = roundUpToPowerOfTwoOrZero(
        std::clamp<size_t>(settings[Setting::grace_hash_join_initial_buckets], 1, settings[Setting::grace_hash_join_max_buckets]));

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
            auto & left_file = tmp_data->createStream(left_sample_block);
            auto & right_file = tmp_data->createStream(prepareRightBlock(right_sample_block));

            BucketPtr new_bucket = std::make_shared<FileBucket>(current_size + i, left_file, right_file, log);
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
    left_sample_block = sample_block.cloneEmpty();
    output_sample_block = left_sample_block.cloneEmpty();
    ExtraBlockPtr not_processed = nullptr;
    hash_join->joinBlock(output_sample_block, not_processed);
    if (not_processed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unhandled not processed block in GraceHashJoin");
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
        size_t current_bucket_,
        Buckets buckets_,
        InMemoryJoinPtr hash_join_,
        const Names & left_key_names_,
        const Names & right_key_names_)
        : current_bucket(current_bucket_)
        , buckets(std::move(buckets_))
        , hash_join(std::move(hash_join_))
        , left_reader(buckets[current_bucket]->getLeftTableReader())
        , left_key_names(left_key_names_)
        , right_key_names(right_key_names_)
    {
    }

    Block nextImpl() override
    {
        ExtraBlockPtr not_processed = nullptr;

        {
            std::lock_guard lock(extra_block_mutex);
            if (!not_processed_blocks.empty())
            {
                not_processed = std::move(not_processed_blocks.front());
                not_processed_blocks.pop_front();
            }
        }

        if (not_processed)
        {
            Block block = std::move(not_processed->block);
            hash_join->joinBlock(block, not_processed);
            if (not_processed)
            {
                std::lock_guard lock(extra_block_mutex);
                not_processed_blocks.emplace_back(std::move(not_processed));
            }
            return block;
        }

        Block block;
        size_t num_buckets = buckets.size();
        size_t current_idx = buckets[current_bucket]->idx;

        do
        {
            // One DelayedBlocks is shared among multiple DelayedJoinedBlocksWorkerTransform.
            // There is a lock inside left_reader.read() .
            block = left_reader.read();
            if (!block)
            {
                return {};
            }

            // block comes from left_reader, need to join with right table to get the result.
            Blocks blocks = JoinCommon::scatterBlockByHash(left_key_names, block, num_buckets);
            block = std::move(blocks[current_idx]);

            /*
             * We need to filter out blocks that were written to the current bucket `B_{n}`
             * but then virtually moved to another bucket `B_{n+i}` on rehash.
             * Bucket `B_{n+i}` is waiting for the buckets with smaller index to be processed,
             * and rows can be moved only forward (because we increase hash modulo twice on each rehash),
             * so it is safe to add blocks.
             */
            for (size_t bucket_idx = 0; bucket_idx < num_buckets; ++bucket_idx)
            {
                if (blocks[bucket_idx].rows() == 0)
                    continue;

                if (bucket_idx == current_idx) // Rows that are still in our bucket
                    continue;

                buckets[bucket_idx]->addLeftBlock(blocks[bucket_idx]);
            }
        } while (block.rows() == 0);

        hash_join->joinBlock(block, not_processed);
        if (not_processed)
        {
            std::lock_guard lock(extra_block_mutex);
            not_processed_blocks.emplace_back(std::move(not_processed));
        }
        return block;
    }

    size_t current_bucket;
    Buckets buckets;
    InMemoryJoinPtr hash_join;

    AccumulatedBlockReader left_reader;

    Names left_key_names;
    Names right_key_names;

    std::mutex extra_block_mutex;
    std::list<ExtraBlockPtr> not_processed_blocks TSA_GUARDED_BY(extra_block_mutex);
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

        hash_join = makeInMemoryJoin(fmt::format("grace{}", bucket_idx), prev_keys_num);
        auto right_reader = current_bucket->startJoining();
        size_t num_rows = 0; /// count rows that were written and rehashed
        while (Block block = right_reader.read())
        {
            num_rows += block.rows();
            addBlockToJoinImpl(std::move(block));
        }

        LOG_TRACE(log, "Loaded bucket {} with {}(/{}) rows",
            bucket_idx, hash_join->getTotalRowCount(), num_rows);

        return std::make_unique<DelayedBlocks>(current_bucket->idx, buckets, hash_join, left_key_names, right_key_names);
    }

    LOG_TRACE(log, "Finished loading all {} buckets", buckets.size());

    current_bucket = nullptr;
    return nullptr;
}

GraceHashJoin::InMemoryJoinPtr GraceHashJoin::makeInMemoryJoin(const String & bucket_id, size_t reserve_num)
{
    return std::make_unique<HashJoin>(table_join, right_sample_block, any_take_last_row, reserve_num, bucket_id);
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
            hash_join = makeInMemoryJoin(fmt::format("grace{}", bucket_index));

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

        hash_join = makeInMemoryJoin(fmt::format("grace{}", bucket_index), prev_keys_num);

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
