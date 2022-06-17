#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/TableJoin.h>

#include <Formats/NativeWriter.h>
#include <Formats/TemporaryFileStream.h>

#include <Compression/CompressedWriteBuffer.h>
#include <Disks/IVolume.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <IO/WriteBufferFromTemporaryFile.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int JOIN_BUCKETS_LIMIT_EXCEEDED;
}

namespace
{

    class FileBlockReader
    {
    public:
        explicit FileBlockReader(const TemporaryFileOnDisk & file, const Block & header)
            : file_reader{file.getDisk()->readFile(file.getPath())}
            , compressed_reader{*file_reader}
            , block_reader{compressed_reader, header, 0}
        {
        }

        Block read() { return block_reader.read(); }

    private:
        std::unique_ptr<ReadBufferFromFileBase> file_reader;
        CompressedReadBuffer compressed_reader;
        NativeReader block_reader;
    };

    class FileBlockWriter
    {
        static std::string buildTemporaryFilePrefix(ContextPtr context, JoinTableSide side, size_t index)
        {
            std::string_view suffix = side == JoinTableSide::Left ? "left" : "right";
            return fmt::format("tmp_{}_gracejoinbuf_{}_{}_", context->getCurrentQueryId(), suffix, index);
        }

    public:
        explicit FileBlockWriter(ContextPtr context, DiskPtr disk_, JoinTableSide side, size_t index)
            : disk{std::move(disk_)}
            , file{disk, buildTemporaryFilePrefix(context, side, index), true}
            , file_writer{disk->writeFile(file.getPath())}
            , compressed_writer{*file_writer}
        {
        }

        void write(const Block & block)
        {
            if (finished.load())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Writing to finished temporary file");
            if (!output)
                reset(block);
            output->write(block);
            ++num_blocks;
        }

        void finalize()
        {
            if (finished.exchange(true))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Flushing already finished temporary file");
            compressed_writer.finalize();
            file_writer->finalize();
        }

        FileBlockReader makeReader() const
        {
            if (!finished.load())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading not finished file");
            return FileBlockReader{file, header};
        }

        size_t numBlocks() const { return num_blocks; }

    private:
        void reset(const Block & sample)
        {
            header = sample.cloneEmpty();
            output.emplace(compressed_writer, 0, header);
        }

        Block header;
        DiskPtr disk;
        TemporaryFileOnDisk file;
        std::unique_ptr<WriteBufferFromFileBase> file_writer;
        CompressedWriteBuffer compressed_writer;
        std::optional<NativeWriter> output;
        std::atomic<bool> finished{false};
        size_t num_blocks = 0;
    };

}

class GraceHashJoin::FileBucket
{
    enum class State : int
    {
        WRITING_BLOCKS,
        JOINING_BLOCKS,
        FINISHED,
        ANY,
    };

public:
    explicit FileBucket(ContextPtr context_, TableJoin & join, size_t bucket_index_, const FileBucket * parent_)
        : bucket_index{bucket_index_}
        , left_file{context_, join.getTemporaryVolume()->getDisk(), JoinTableSide::Left, bucket_index}
        , right_file{context_, join.getTemporaryVolume()->getDisk(), JoinTableSide::Right, bucket_index}
        , parent{parent_}
        , state{State::WRITING_BLOCKS}
    {
    }

    void addRightBlock(const Block & block)
    {
        ensureState(State::WRITING_BLOCKS);
        right_file.write(block);
    }

    bool tryAddLeftBlock(const Block & block)
    {
        ensureState(State::WRITING_BLOCKS);
        std::unique_lock lock{left_file_mutex, std::try_to_lock};
        if (!lock.owns_lock())
        {
            return false;
        }
        left_file.write(block);
        return true;
    }

    void addLeftBlock(const Block & block)
    {
        ensureState(State::WRITING_BLOCKS);
        std::unique_lock lock{left_file_mutex};
        left_file.write(block);
    }

    void startJoining()
    {
        ensureState(State::JOINING_BLOCKS);
        left_file.finalize();
        right_file.finalize();
    }

    size_t index() const { return bucket_index; }
    bool finished() const { return state.load() == State::FINISHED; }
    bool empty() const { return right_file.numBlocks() == 0 && left_file.numBlocks() == 0; }

    bool tryLockForJoining()
    {
        if (parent && !parent->finished())
            return false;

        State expected = State::WRITING_BLOCKS;
        return state.compare_exchange_strong(expected, State::JOINING_BLOCKS);
    }

    void finish() { transition(State::JOINING_BLOCKS, State::FINISHED); }

    FileBlockReader openLeftTableReader() const { return left_file.makeReader(); }

    FileBlockReader openRightTableReader() const { return right_file.makeReader(); }

private:
    void transition(State expected, State desired)
    {
        State prev = state.exchange(desired);
        if (expected != State::ANY && prev != expected)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid state transition");
    }

    void ensureState(State expected)
    {
        if (state.load() != expected)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid state transition");
    }

    size_t bucket_index;
    FileBlockWriter left_file;
    FileBlockWriter right_file;
    std::mutex left_file_mutex;
    const FileBucket * parent;
    std::atomic<State> state;
};

GraceHashJoin::GraceHashJoin(
    ContextPtr context_, std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_, bool any_take_last_row_)
    : log{&Poco::Logger::get("GraceHashJoin")}
    , context{context_}
    , table_join{std::move(table_join_)}
    , right_sample_block{right_sample_block_}
    , any_take_last_row{any_take_last_row_}
    , initial_num_buckets{context->getSettingsRef().grace_hash_join_initial_buckets}
    , max_num_buckets{context->getSettingsRef().grace_hash_join_max_buckets}
    , first_bucket{makeInMemoryJoin()}
{
    initial_num_buckets = roundUpToPowerOfTwoOrZero(initial_num_buckets);
    auto tmp = std::make_unique<Buckets>();
    for (size_t i = 0; i < initial_num_buckets; ++i)
    {
        addBucket(*tmp, nullptr);
    }
    buckets.set(std::move(tmp));
    LOG_TRACE(log, "Initialize {} buckets", initial_num_buckets);
}

bool GraceHashJoin::addJoinedBlock(const Block & block, bool /*check_limits*/)
{
    LOG_TRACE(log, "addJoinedBlock(block: {} rows)", block.rows());
    Block materialized = materializeBlock(block);
    addJoinedBlockImpl(first_bucket, 0, materialized);
    return true;
}

void GraceHashJoin::rehashInMemoryJoin(InMemoryJoinPtr & join, const BucketsSnapshot & snapshot, size_t bucket)
{
    InMemoryJoinPtr prev = std::move(join);
    auto right_blocks = std::move(*prev).releaseJoinedBlocks();
    join = makeInMemoryJoin();

    for (const Block & block : right_blocks)
    {
        Blocks blocks = scatterBlock<true>(block, snapshot->size());
        join->addJoinedBlock(blocks[bucket], /*check_limits=*/false);
        for (size_t i = 1; i < snapshot->size(); ++i)
        {
            if (i != bucket && blocks[i].rows())
                snapshot->at(i)->addRightBlock(blocks[i]);
        }
    }
}

bool GraceHashJoin::fitsInMemory(InMemoryJoin * join) const
{
    return table_join->sizeLimits().softCheck(join->getTotalRowCount(), join->getTotalByteCount());
}

GraceHashJoin::BucketsSnapshot GraceHashJoin::rehash(size_t desired_size)
{
    desired_size = roundUpToPowerOfTwoOrZero(desired_size);

    std::scoped_lock lock{rehash_mutex};
    BucketsSnapshot snapshot = buckets.get();
    size_t current_size = snapshot->size();

    LOG_TRACE(log, "Rehashing from {} to {}", current_size, desired_size);

    if (current_size >= desired_size)
        return snapshot;

    auto next_snapshot = std::make_unique<Buckets>(*snapshot);
    size_t next_size = std::max(current_size * 2, desired_size);

    if (next_size > max_num_buckets)
    {
        throw Exception(ErrorCodes::JOIN_BUCKETS_LIMIT_EXCEEDED, "");
    }

    next_snapshot->reserve(next_size);
    while (next_snapshot->size() < next_size)
    {
        current_size = next_snapshot->size();
        for (size_t i = 0; i < current_size; ++i)
        {
            if (i == 0)
                addBucket(*next_snapshot, nullptr);
            else
                addBucket(*next_snapshot, next_snapshot->at(i).get());
        }
    }

    buckets.set(std::move(next_snapshot));
    return buckets.get();
}

void GraceHashJoin::startReadingDelayedBlocks()
{
    // Drop in-memory hash join for the first bucket to reduce memory footprint.
    first_bucket.reset();
}

void GraceHashJoin::addBucket(Buckets & destination, const FileBucket * parent)
{
    size_t index = destination.size();
    destination.emplace_back(std::make_unique<FileBucket>(context, *table_join, index, parent));
}

void GraceHashJoin::checkTypesOfKeys(const Block & block) const
{
    assert(first_bucket);
    return first_bucket->checkTypesOfKeys(block);
}

void GraceHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & /*not_processed*/)
{
    LOG_TRACE(log, "JoinBlock: {}", block.dumpStructure());

    if (block.rows() == 0)
    {
        ExtraBlockPtr not_processed;
        first_bucket->joinBlock(block, not_processed);
        return;
    }

    materializeBlockInplace(block);

    auto snapshot = buckets.get();
    auto blocks = scatterBlock<false>(block, snapshot->size());

    ExtraBlockPtr not_processed;
    first_bucket->joinBlock(blocks[0], not_processed);
    block = std::move(blocks[0]);
    if (not_processed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported hash join type");

    std::deque<size_t> indices(snapshot->size() - 1);
    std::iota(indices.begin(), indices.end(), 1);
    std::shuffle(indices.begin(), indices.end(), thread_local_rng);
    while (!indices.empty())
    {
        size_t bucket = indices.front();
        indices.pop_front();

        Block & block_shard = blocks[bucket];
        if (block_shard.rows() == 0)
        {
            continue;
        }
        if (!snapshot->at(bucket)->tryAddLeftBlock(block_shard))
        {
            indices.push_back(bucket);
        }
    }
}

size_t GraceHashJoin::getTotalRowCount() const
{
    assert(first_bucket);
    return first_bucket->getTotalRowCount();
}

size_t GraceHashJoin::getTotalByteCount() const
{
    assert(first_bucket);
    return first_bucket->getTotalByteCount();
}

bool GraceHashJoin::alwaysReturnsEmptySet() const
{
    auto snapshot = buckets.get();
    bool all_buckets_are_empty = std::all_of(snapshot->begin(), snapshot->end(), [](const auto & bucket) { return bucket->empty(); });
    return isInnerOrRight(table_join->kind()) && first_bucket->alwaysReturnsEmptySet() && all_buckets_are_empty;
}

std::shared_ptr<NotJoinedBlocks> GraceHashJoin::getNonJoinedBlocks(const Block &, const Block &, UInt64) const
{
    auto snapshot = buckets.get();
    size_t unfinished = 0;
    for (size_t i = 1; i < snapshot->size(); ++i)
    {
        if (!snapshot->at(i)->finished())
        {
            LOG_ERROR(log, "Bucket {} is not finished", i);
            ++unfinished;
        }
    }
    if (unfinished > 0)
    {
        LOG_ERROR(log, "Total {} unfinished buckets", unfinished);
    }

    if (!JoinCommon::hasNonJoinedBlocks(*table_join))
        return nullptr;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported join mode");
}

class GraceHashJoin::DelayedBlocks : public IDelayedJoinedBlocksStream
{
public:
    explicit DelayedBlocks(GraceHashJoin * parent_, FileBucket * bucket_, InMemoryJoinPtr join_)
        : parent{parent_}, bucket{bucket_}, left_reader{bucket->openLeftTableReader()}, join{std::move(join_)}
    {
    }

    Block next() override { return parent->joinNextBlockInBucket(*this); }

    GraceHashJoin * parent;
    FileBucket * bucket;
    FileBlockReader left_reader;
    InMemoryJoinPtr join;
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

    auto snapshot = buckets.get();
    for (size_t i = 1; i < snapshot->size(); ++i)
    {
        FileBucket * bucket = snapshot->at(i).get();
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
            LOG_TRACE(log, "getDelayedBlocks: start joining bucket {}", bucket->index());
            InMemoryJoinPtr join = makeInMemoryJoin();
            fillInMemoryJoin(join, bucket);
            return std::make_unique<DelayedBlocks>(this, bucket, std::move(join));
        }
    }

    // NB: this logic is a bit racy. Now can be more buckets in the @snapshot in case of rehashing in different thread reading delayed blocks.
    // But it's ok to finish current thread: the thread that called rehash() will join the rest of the blocks.
    return nullptr;
}

Block GraceHashJoin::joinNextBlockInBucket(DelayedBlocks & iterator)
{
    Block block;

    do
    {
        block = iterator.left_reader.read();
        if (!block) // EOF
            return block;

        BucketsSnapshot snapshot = buckets.get();
        Blocks blocks = scatterBlock<false>(block, snapshot->size());
        block.clear();

        // We need to filter out blocks that were written to the current bucket B0,
        // But then virtually moved to another bucket B1 in rehash().
        // Note that B1 is waiting for current bucket B0 to be processed
        // (via @parent field, see @FileBucket::tryLockForJoining),
        // So it is safe to add blocks to it.
        for (size_t i = 0; i < snapshot->size(); ++i)
        {
            if (!blocks[i].rows()) // No rows with that hash modulo
                continue;
            if (i == iterator.bucket->index()) // Rows that are still in our bucket
                block = std::move(blocks[i]);
            else // Rows that were moved after rehashing
                snapshot->at(i)->addLeftBlock(blocks[i]);
        }
    } while (block.rows() == 0);

    ExtraBlockPtr not_processed;
    iterator.join->joinBlock(block, not_processed);
    if (not_processed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported hash join type");

    return block;
}

std::unique_ptr<HashJoin> GraceHashJoin::makeInMemoryJoin()
{
    return std::make_unique<HashJoin>(table_join, right_sample_block, any_take_last_row);
}

void GraceHashJoin::fillInMemoryJoin(InMemoryJoinPtr & join, FileBucket * bucket)
{
    bucket->startJoining();
    auto reader = bucket->openRightTableReader();

    while (auto block = reader.read())
    {
        addJoinedBlockImpl(join, bucket->index(), block);
    }
}

void GraceHashJoin::addJoinedBlockImpl(InMemoryJoinPtr & join, size_t bucket_index, const Block & block)
{
    BucketsSnapshot snapshot = buckets.get();
    Blocks blocks = scatterBlock<true>(block, snapshot->size());

    join->addJoinedBlock(blocks[bucket_index], /*check_limits=*/false);

    // We need to rebuild block without bucket_index part in case of overflow.
    bool overflow = !fitsInMemory(join.get());
    Block to_write;
    if (overflow)
    {
        blocks.erase(blocks.begin() + bucket_index);
        to_write = concatenateBlocks(blocks);
    }

    while (overflow)
    {
        snapshot = rehash(snapshot->size() * 2);
        rehashInMemoryJoin(join, snapshot, bucket_index);
        blocks = scatterBlock<true>(to_write, snapshot->size());
        overflow = !fitsInMemory(join.get());
    }

    assert(blocks.empty() || blocks.size() == snapshot->size());
    for (size_t i = 1; i < blocks.size(); ++i)
    {
        if (i != bucket_index && blocks[i].rows())
            snapshot->at(i)->addRightBlock(blocks[i]);
    }
}

template <bool right>
Blocks GraceHashJoin::scatterBlock(const Block & block, size_t shards) const
{
    if (!block)
    {
        return {};
    }

    const Names & key_names = [](const TableJoin::JoinOnClause & clause) -> auto &
    {
        return right ? clause.key_names_right : clause.key_names_left;
    }
    (table_join->getOnlyClause());
    return JoinCommon::scatterBlockByHash(key_names, block, shards);
}

}
