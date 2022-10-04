#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/Context.h>

#include <Formats/NativeWriter.h>
#include <Interpreters/TemporaryDataOnDisk.h>

#include <Compression/CompressedWriteBuffer.h>
#include <Core/ProtocolDefines.h>
#include <Disks/IVolume.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <IO/WriteBufferFromTemporaryFile.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include <base/FnTraits.h>
#include <fmt/format.h>

#include <Formats/formatBlock.h>

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


    void debugBlock(const Block & block, size_t line, std::string msg = "", bool st = false)
    {
        size_t count = 0;
        String colname;

        if (block.has("key"))
            colname = "key";
        if (block.has("t2.key"))
            colname = "t2.key";

        if (colname.empty())
            return;

        auto col = block.getByName(colname).column;
        for (size_t i = 0; i < col->size(); ++i)
        {
            if (col->get64(i) == 31)
                count++;
        }

        if (count > 0)
        {
            LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} AAA {}: {} | {}", __FILE__, msg, line, count, block.dumpStructure());
            if (st)
            {
                LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} {} : {}", __FILE__, __LINE__, line, StackTrace().toString());
            }
        }
    }

    class BlocksAccumulator
    {
    public:
        explicit BlocksAccumulator(size_t desired_block_size_)
            : desired_block_size(desired_block_size_)
        {
            assert(desired_block_size > 0);
        }

        void addBlock(Block block)
        {
            sum_size += block.rows();
            blocks.push_back(block);
        }

        Block peek()
        {
            if (sum_size < desired_block_size)
                return {};
            return flush();
        }

        Block flush()
        {
            if (blocks.empty())
                return {};
            Block result = concatenateBlocks(blocks);
            blocks = {};
            sum_size = 0;
            return result;
        }

    private:
        const size_t desired_block_size;
        size_t sum_size = 0;
        Blocks blocks;
    };

    class MergingBlockReader
    {
    public:
        explicit MergingBlockReader(TemporaryFileStream & reader_, size_t desired_block_size = DEFAULT_BLOCK_SIZE * 8)
            : reader{reader_}
            , accumulator{desired_block_size}
        {
            if (!reader.isWriteFinished())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading not finished file");
        }

        Block read()
        {
            if (eof)
                return {};

            Block res;
            for (; !res; res = accumulator.peek())
            {
                Block tmp = reader.read();
                debugBlock(tmp, __LINE__);

                if (!tmp)
                {
                    eof = true;
                    return accumulator.flush();
                }
                accumulator.addBlock(std::move(tmp));
            }
            debugBlock(res, __LINE__);

            return res;
        }

    private:
        TemporaryFileStream & reader;
        BlocksAccumulator accumulator;
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
            size_t bucket = indices.front();
            indices.pop_front();

            if (!callback(bucket))
                indices.push_back(bucket);
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
    struct Stats
    {
        TemporaryFileStream::Stat left;
        TemporaryFileStream::Stat right;
    };

    explicit FileBucket(size_t bucket_index_,
                        TemporaryFileStream & left_file_,
                        TemporaryFileStream & right_file_,
                        const FileBucket * parent_,
                        Poco::Logger * log_)
        : bucket_index{bucket_index_}
        , left_file{left_file_}
        , right_file{right_file_}
        , parent{parent_}
        , state{State::WRITING_BLOCKS}
        , log(log_)
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

    void startJoining()
    {
        LOG_TRACE(log, "Joining file bucket {}", bucket_index);

        ensureState(State::JOINING_BLOCKS);
        stats.left = left_file.finishWriting();
        stats.right = right_file.finishWriting();
    }

    size_t index() const { return bucket_index; }
    bool finished() const { return state.load() == State::FINISHED; }
    bool empty() const { return is_empty.load(); }

    Stats getStat() const { return stats; }

    bool tryLockForJoining()
    {
        if (parent && !parent->finished())
            return false;

        State expected = State::WRITING_BLOCKS;
        return state.compare_exchange_strong(expected, State::JOINING_BLOCKS);
    }

    void finish()
    {
        LOG_TRACE(log, "XXXX Finish joining file bucket {}, size: {} | {}",
            bucket_index, stats.left.num_rows, stats.right.num_rows);

        state.exchange(State::FINISHED);
        // transition(State::JOINING_BLOCKS, State::FINISHED);
    }

    MergingBlockReader openLeftTableReader() const { return MergingBlockReader(left_file); }

    MergingBlockReader openRightTableReader() const { return MergingBlockReader(right_file); }

    std::mutex & joinMutex() { return join_mutex; }

    ~FileBucket()
    {

        LOG_TRACE(log, "XXXX Destroying file bucket {} - {}({}): rows: {} | {}",
            bucket_index, fmt::ptr(this), fmt::ptr(parent), stats.left.num_rows, stats.right.num_rows);
    }

private:
    bool addBlockImpl(const Block & block, TemporaryFileStream & writer, std::unique_lock<std::mutex> & lock)
    {
        if (block.rows())
            is_empty = false;
        ensureState(State::WRITING_BLOCKS);
        if (!lock.owns_lock())
            return false;

        debugBlock(block, __LINE__, fmt::format("adding to {}", bucket_index), true);

        writer.write(block);
        return true;
    }

    void transition(State expected, State desired)
    {
        State prev = state.exchange(desired);
        if (prev != expected)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid state transition");
    }

    void ensureState(State expected)
    {
        if (state.load() != expected)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid state transition");
    }

    size_t bucket_index;

    TemporaryFileStream & left_file;
    TemporaryFileStream & right_file;
    std::mutex left_file_mutex;
    std::mutex right_file_mutex;

    std::atomic_bool is_empty = true;

    std::mutex join_mutex; /// Protects external in-memory join

    const FileBucket * parent;
    std::atomic<State> state;

    Stats stats;

    Poco::Logger * log;
};

class GraceHashJoin::InMemoryJoin
{
public:

    template <typename ... Args>
    InMemoryJoin(Args && ... args) : join(std::make_unique<HashJoin>(std::forward<Args>(args)...))
    {
    }

    template <typename ... Args>
    std::unique_ptr<HashJoin> emplace(Args && ... args)
    {
        std::unique_ptr<HashJoin> current = std::move(join);
        join = std::make_unique<HashJoin>(std::forward<Args>(args)...);
        return current;
    }

    std::unique_ptr<HashJoin> join;

    std::mutex mutex;
};

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
    , first_bucket{makeInMemoryJoin()}
    , tmp_data(std::make_unique<TemporaryDataOnDisk>(tmp_data_, CurrentMetrics::TemporaryFilesForJoin))
{
    if (!GraceHashJoin::isSupported(table_join))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "GraceHashJoin is not supported for this join type");

    const auto & settings = context->getSettingsRef();

    size_t initial_num_buckets = roundUpToPowerOfTwoOrZero(settings.grace_hash_join_initial_buckets);
    for (size_t i = 0; i < initial_num_buckets; ++i)
    {
        addBucket(buckets, nullptr);
    }

    LOG_TRACE(log, "Initialize {} buckets", buckets.size());
}

bool GraceHashJoin::isSupported(const std::shared_ptr<TableJoin> & table_join)
{
    bool is_asof = (table_join->strictness() == JoinStrictness::Asof);
    return !is_asof && isInnerOrLeft(table_join->kind()) && table_join->oneDisjunct();
}

GraceHashJoin::~GraceHashJoin() = default;

bool GraceHashJoin::addJoinedBlock(const Block & block, bool /*check_limits*/)
{
    Block materialized = materializeBlock(block);
    addJoinedBlockImpl(first_bucket, 0, materialized);
    return true;
}

void GraceHashJoin::rehashInMemoryJoin(InMemoryJoinPtr & join, const Buckets & buckets_snapshot, size_t bucket)
{
    std::lock_guard<std::mutex> lock{join->mutex};

    auto prev_hash = join->emplace(table_join, right_sample_block, any_take_last_row);
    auto right_blocks = std::move(*prev_hash).releaseJoinedBlocks();

    for (const Block & block : right_blocks)
    {
        Blocks blocks = JoinCommon::scatterBlockByHash(right_key_names, block, buckets_snapshot.size());
        join->join->addJoinedBlock(blocks[bucket], /* check_limits = */ false);
        for (size_t i = 1; i < buckets_snapshot.size(); ++i)
        {
            if (i != bucket && blocks[i].rows())
                buckets_snapshot[i]->addRightBlock(blocks[i]);
        }
    }
}

bool GraceHashJoin::fitsInMemory(InMemoryJoin * join) const
{
    /// One row can't be split, avoid loop
    if (join->join->getTotalRowCount() < 2)
        return true;

    return table_join->sizeLimits().softCheck(join->join->getTotalRowCount(), join->join->getTotalByteCount());
}

GraceHashJoin::Buckets GraceHashJoin::rehashBuckets()
{
    std::unique_lock lock(rehash_mutex);
    size_t current_size = buckets.size();
    size_t next_size = current_size * 2;

    if (next_size > max_num_buckets)
    {
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "Too many grace hash join buckets ({} > {}), consider increasing grace_hash_join_max_buckets or max_rows_in_join/max_bytes_in_join",
            next_size, max_num_buckets);
    }

    LOG_TRACE(log, "Rehashing from {} to {}", current_size, next_size);

    buckets.reserve(next_size);
    for (size_t i = current_size; i < next_size; ++i)
    {
        addBucket(buckets, buckets[i % current_size].get());
    }
    return buckets;
}

void GraceHashJoin::startReadingDelayedBlocks()
{
    // Drop in-memory hash join for the first bucket to reduce memory footprint.
    first_bucket.reset();
    std::unique_lock lock(rehash_mutex);
    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} AAA startReadingDelayedBlocks {} > {} | {}", __FILE__, __LINE__,
        buckets[0]->empty() ? "empty" : "not empty",
        buckets[0]->getStat().left.num_rows, buckets[0]->getStat().right.num_rows);
    if (!buckets[0]->finished())
        buckets[0]->finish();
}

void GraceHashJoin::addBucket(Buckets & destination, const FileBucket * parent)
{
    size_t index = destination.size();
    auto new_bucket = std::make_unique<FileBucket>(index, tmp_data->createStream(left_sample_block), tmp_data->createStream(right_sample_block), parent, log);
    destination.emplace_back(std::move(new_bucket));
}

void GraceHashJoin::checkTypesOfKeys(const Block & block) const
{
    assert(first_bucket && first_bucket->join);
    return first_bucket->join->checkTypesOfKeys(block);
}

void GraceHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & /*not_processed*/)
{
    if (need_left_sample_block.exchange(false))
    {
        output_sample_block = left_sample_block.cloneEmpty();
        ExtraBlockPtr not_processed;
        first_bucket->join->joinBlock(output_sample_block, not_processed);
    }

    if (block.rows() == 0)
    {
        ExtraBlockPtr not_processed;
        first_bucket->join->joinBlock(block, not_processed);
        return;
    }

    materializeBlockInplace(block);

    Buckets buckets_snapshot = getCurrentBuckets();
    size_t num_buckets = buckets_snapshot.size();
    Blocks blocks = JoinCommon::scatterBlockByHash(left_key_names, block, num_buckets);

    ExtraBlockPtr not_processed;
    block = std::move(blocks[0]);
    debugBlock(block, __LINE__);
    first_bucket->join->joinBlock(block, not_processed);
    if (not_processed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unhandled not processed block in GraceHashJoin");

    // We need to skip the first bucket that is already joined in memory, so we start with 1.
    retryForEach(
        generateRandomPermutation(1, num_buckets),
        [&blocks, &buckets_snapshot](size_t idx)
        {
            if (blocks[idx].rows() == 0)
                return true;
            return buckets_snapshot[idx]->tryAddLeftBlock(blocks[idx]);
        });
}

void GraceHashJoin::setTotals(const Block & block)
{
    if (block)
    {
        std::lock_guard guard(totals_mutex);
        totals = block;
    }
}

const Block & GraceHashJoin::getTotals() const
{
    return totals;
}

size_t GraceHashJoin::getTotalRowCount() const
{
    assert(first_bucket && first_bucket->join);
    return first_bucket->join->getTotalRowCount();
}

size_t GraceHashJoin::getTotalByteCount() const
{
    assert(first_bucket && first_bucket->join);
    return first_bucket->join->getTotalByteCount();
}

bool GraceHashJoin::alwaysReturnsEmptySet() const
{
    if (!isInnerOrRight(table_join->kind()))
        return false;

    std::shared_lock lock(rehash_mutex);

    bool file_buckets_are_empty = std::all_of(buckets.begin(), buckets.end(), [](const auto & bucket) { return bucket->empty(); });
    bool first_bucket_is_empty = first_bucket && first_bucket->join && first_bucket->join->alwaysReturnsEmptySet();

    return first_bucket_is_empty && file_buckets_are_empty;
}

std::unique_ptr<NotJoinedBlocks> GraceHashJoin::getNonJoinedBlocks(const Block &, const Block &, UInt64) const
{
    /// We do no support returning non joined blocks here.
    /// They will be reported by getDelayedBlocks instead.
    return nullptr;
}

class GraceHashJoin::DelayedBlocks : public IDelayedJoinedBlocksStream
{
public:
    explicit DelayedBlocks(GraceHashJoin * parent_, FileBucket * bucket_, InMemoryJoinPtr join_)
        : parent{parent_}, bucket{bucket_}, left_reader{bucket->openLeftTableReader()}, join{std::move(join_)}
    {
    }

    Block next() override
    {
        Block result = parent->joinNextBlockInBucket(*this);
        if (result)
            debugBlock(result, __LINE__);

        if (result)
            return result;

        if (process_not_joined)
        {
            not_joined_blocks = join->join->getNonJoinedBlocks(parent->left_sample_block, parent->output_sample_block, parent->max_block_size);
            process_not_joined = false;
        }

        if (not_joined_blocks)
            return not_joined_blocks->read();

        return {};
    }

    GraceHashJoin * parent;
    FileBucket * bucket;
    MergingBlockReader left_reader;
    InMemoryJoinPtr join;
    bool process_not_joined = true;
    std::unique_ptr<NotJoinedBlocks> not_joined_blocks;
};

std::unique_ptr<IDelayedJoinedBlocksStream> GraceHashJoin::getDelayedBlocks(IDelayedJoinedBlocksStream * prev_cursor)
{
    if (prev_cursor)
    {
        assert_cast<DelayedBlocks *>(prev_cursor)->bucket->finish();
    }

    if (!started_reading_delayed_blocks.exchange(true))
    {
        startReadingDelayedBlocks();
    }

    auto snapshot = getCurrentBuckets();
    for (size_t buckets_left = snapshot.size() - 1, i = 0; buckets_left > 0; ++i)
    {
        auto & bucket = snapshot[i % (snapshot.size() - 1) + 1];
        if (bucket == nullptr)
            continue;

        // LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} {}({} | {}) / {} -> {} {}", __FILE__, __LINE__,
        //     i, i % (snapshot.size() - 1) + 1,
        //     bucket->index(), snapshot.size(), bucket->finished() ? "finished" : "not finished",
        //     bucket->empty() ? "empty" : "not empty");


        if (bucket->finished())
        {
            --buckets_left;
            bucket.reset();
            continue;
        }

        if (!bucket->tryLockForJoining())
        {
            continue;
        }

        if (bucket->empty())
        {
            bucket->finish();
        }
        else
        {
            InMemoryJoinPtr join = makeInMemoryJoin();
            fillInMemoryJoin(join, bucket.get());
            return std::make_unique<DelayedBlocks>(this, bucket.get(), std::move(join));
        }
    }

    // NB: this logic is a bit racy. There can be more buckets in the @snapshot in case of rehashing in different thread reading delayed blocks.
    // But it's ok to finish current thread: the thread that called rehashBuckets() will join the rest of the blocks.
    return nullptr;
}

Block GraceHashJoin::joinNextBlockInBucket(DelayedBlocks & iterator)
{
    Block block;
    size_t cur_index = iterator.bucket->index();
    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} joinNextBlockInBucket {}", __FILE__, __LINE__, cur_index);
    do
    {
        block = iterator.left_reader.read();
        if (!block) // EOF
            return block;

        auto buckets_snapshot = getCurrentBuckets();
        size_t num_buckets = buckets_snapshot.size();
        debugBlock(block, __LINE__);
        Blocks blocks = JoinCommon::scatterBlockByHash(left_key_names, block, num_buckets);
        for (size_t i = 0; i< blocks.size(); ++i)
        {
            debugBlock(block, __LINE__, fmt::format("virtually moved {} -> {}", cur_index, i));
        }

        block.clear();

        // We need to filter out blocks that were written to the current bucket B0,
        // But then virtually moved to another bucket B1 in rehashBuckets().
        // Note that B1 is waiting for current bucket B0 to be processed
        // (via @parent field, see @FileBucket::tryLockForJoining),
        // So it is safe to add blocks to it.
        for (size_t i = 0; i < num_buckets; ++i)
        {
            if (!blocks[i].rows()) // No rows with that hash modulo
                continue;

            if (i == iterator.bucket->index()) // Rows that are still in our bucket
                block = std::move(blocks[i]);

            else // Rows that were moved after rehashing
                buckets_snapshot[i]->addLeftBlock(blocks[i]);
        }
    } while (block.rows() == 0);

    debugBlock(block, __LINE__);
    ExtraBlockPtr not_processed;
    iterator.join->join->joinBlock(block, not_processed);
    iterator.join->join->debugKeys();
    debugBlock(block, __LINE__);

    if (not_processed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported hash join type");

    return block;
}

GraceHashJoin::InMemoryJoinPtr GraceHashJoin::makeInMemoryJoin()
{
    return std::make_unique<InMemoryJoin>(table_join, right_sample_block, any_take_last_row);
}

void GraceHashJoin::fillInMemoryJoin(InMemoryJoinPtr & join, FileBucket * bucket)
{
    bucket->startJoining();
    auto reader = bucket->openRightTableReader();

    while (auto block = reader.read())
    {
        debugBlock(block, __LINE__);
        addJoinedBlockImpl(join, bucket->index(), block);
    }
}

void GraceHashJoin::addJoinedBlockImpl(InMemoryJoinPtr & join, size_t bucket_index, const Block & block)
{
    Buckets buckets_snapshot = getCurrentBuckets();
    Blocks blocks = JoinCommon::scatterBlockByHash(right_key_names, block, buckets_snapshot.size());

    debugBlock(block, __LINE__);
    // Add block to the in-memory join
    {

        auto bucket = buckets_snapshot[bucket_index];
        std::lock_guard guard(bucket->joinMutex());
        debugBlock(blocks[bucket_index], __LINE__);

        join->join->addJoinedBlock(blocks[bucket_index], /*check_limits=*/false);

        // We need to rebuild block without bucket_index part in case of overflow.
        bool overflow = !fitsInMemory(join.get());


        Block to_write;
        if (overflow)
        {
            blocks.erase(blocks.begin() + bucket_index);
            to_write = concatenateBlocks(blocks);
        }

        size_t overflow_cnt = 0;
        while (overflow)
        {
            LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} size of join {}: {} {} - {} {}", __FILE__, __LINE__,
                bucket_index,
                join->join->getTotalRowCount(),
                join->join->getTotalByteCount(),
                overflow ? "overflow" : "fits", overflow_cnt);


            buckets_snapshot = rehashBuckets();
            rehashInMemoryJoin(join, buckets_snapshot, bucket_index);

            blocks = JoinCommon::scatterBlockByHash(right_key_names, block, buckets_snapshot.size());

            {
                WriteBufferFromOwnString out;
                auto output_format = context->getOutputFormat("PrettyCompactMonoBlock", out, block);
                formatBlock(output_format, block);
                auto block_string = out.str();

                Strings sizes;
                for (size_t i = 0; i < blocks.size(); ++i)
                {
                    auto & b = blocks[i];
                    if (b.rows())
                        sizes.emplace_back(fmt::format("[{}] - {}", i, b.rows()));
                }

                LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} overflow({}) block\n{} -> [{}]", __FILE__, __LINE__, overflow_cnt, block_string, fmt::join(sizes, ", "));

            }
            overflow = !fitsInMemory(join.get());
            overflow_cnt++;
        }
    }

    if (blocks.empty())
        // All blocks were added to the @join
        return;

    // Write the rest of the blocks to the disk buckets
    assert(blocks.size() == buckets_snapshot.size());
    retryForEach(
        generateRandomPermutation(1, buckets_snapshot.size()),
        [&](size_t i)
        {
            if (i == bucket_index || !blocks[i].rows())
                return true;
            return buckets_snapshot[i]->tryAddRightBlock(blocks[i]);
        });
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
