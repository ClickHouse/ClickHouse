#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/Context.h>

#include <Formats/NativeWriter.h>
#include <Formats/TemporaryFileStream.h>

#include <Compression/CompressedWriteBuffer.h>
#include <Core/ProtocolDefines.h>
#include <Disks/IVolume.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <IO/WriteBufferFromTemporaryFile.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include <base/FnTraits.h>
#include <fmt/format.h>

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

    class FileBlockReader
    {
    public:
        explicit FileBlockReader(const TemporaryFileOnDisk & file, const Block & header)
            : file_reader{file.getDisk()->readFile(file.getPath())}
            , compressed_reader{*file_reader}
            , block_reader{compressed_reader, header, DBMS_TCP_PROTOCOL_VERSION}
        {
        }

        Block read() { return block_reader.read(); }

    private:
        std::unique_ptr<ReadBufferFromFileBase> file_reader;
        CompressedReadBuffer compressed_reader;
        NativeReader block_reader;
    };

    using FileBlockReaderPtr = std::unique_ptr<FileBlockReader>;

    class BlocksAccumulator
    {
    public:
        explicit BlocksAccumulator(size_t desired_block_size_) : desired_block_size(desired_block_size_) { }

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
        explicit MergingBlockReader(FileBlockReaderPtr reader_, size_t desired_block_size = DEFAULT_BLOCK_SIZE * 8)
            : reader{std::move(reader_)}, accumulator{desired_block_size}
        {
        }

        Block read()
        {
            if (eof)
                return {};

            Block res;
            while (!(res = accumulator.peek()))
            {
                Block tmp = reader->read();
                if (!tmp)
                {
                    eof = true;
                    return accumulator.flush();
                }
                accumulator.addBlock(std::move(tmp));
            }

            return res;
        }

    private:
        FileBlockReaderPtr reader;
        BlocksAccumulator accumulator;
        bool eof = false;
    };

    class FileBlockWriter
    {
        static std::string buildTemporaryFilePrefix(ContextPtr context, JoinTableSide side, size_t index)
        {
            std::string_view suffix = side == JoinTableSide::Left ? "left" : "right";
            return fmt::format("tmp_{}_gracejoinbuf_{}_{}_", context->getCurrentQueryId(), suffix, index);
        }

        static CompressionCodecPtr getCompressionCodec(TableJoin & join)
        {
            return CompressionCodecFactory::instance().get(join.temporaryFilesCodec(), std::nullopt);
        }

    public:
        explicit FileBlockWriter(ContextPtr context, TableJoin & join, JoinTableSide side, size_t index)
            : disk{join.getTemporaryVolume()->getDisk()}
            , file{disk, buildTemporaryFilePrefix(context, side, index)}
            , file_writer{disk->writeFile(file.getPath(), context->getSettingsRef().grace_hash_join_buffer_size)}
            , compressed_writer{*file_writer, getCompressionCodec(join), context->getSettingsRef().grace_hash_join_buffer_size}
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

        MergingBlockReader makeReader() const
        {
            if (!finished.load())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading not finished file");
            return MergingBlockReader{std::make_unique<FileBlockReader>(file, header)};
        }

        size_t numBlocks() const { return num_blocks; }

    private:
        void reset(const Block & sample)
        {
            header = sample.cloneEmpty();
            output.emplace(compressed_writer, DBMS_TCP_PROTOCOL_VERSION, header);
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
        , left_file{context_, join, JoinTableSide::Left, bucket_index}
        , right_file{context_, join, JoinTableSide::Right, bucket_index}
        , parent{parent_}
        , state{State::WRITING_BLOCKS}
    {
    }

    void addLeftBlock(const Block & block) { return addBlockImpl(block, left_file_mutex, left_file); }
    void addRightBlock(const Block & block) { return addBlockImpl(block, right_file_mutex, right_file); }
    bool tryAddLeftBlock(const Block & block) { return tryAddBlockImpl(block, left_file_mutex, left_file); }
    bool tryAddRightBlock(const Block & block) { return tryAddBlockImpl(block, right_file_mutex, right_file); }

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

    MergingBlockReader openLeftTableReader() const { return left_file.makeReader(); }

    MergingBlockReader openRightTableReader() const { return right_file.makeReader(); }

    std::mutex & joinMutex() { return join_mutex; }

private:
    bool tryAddBlockImpl(const Block & block, std::mutex & mutex, FileBlockWriter & writer)
    {
        ensureState(State::WRITING_BLOCKS);
        std::unique_lock lock{mutex, std::try_to_lock};
        if (!lock.owns_lock())
        {
            return false;
        }
        writer.write(block);
        return true;
    }

    void addBlockImpl(const Block & block, std::mutex & mutex, FileBlockWriter & writer)
    {
        ensureState(State::WRITING_BLOCKS);
        std::unique_lock lock{mutex};
        writer.write(block);
    }

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
    std::mutex right_file_mutex;
    std::mutex join_mutex; /// Protects external in-memory join
    const FileBucket * parent;
    std::atomic<State> state;
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
    ContextPtr context_, std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_, bool any_take_last_row_)
    : log{&Poco::Logger::get("GraceHashJoin")}
    , context{context_}
    , table_join{std::move(table_join_)}
    , right_sample_block{right_sample_block_}
    , any_take_last_row{any_take_last_row_}
    , initial_num_buckets{context->getSettingsRef().grace_hash_join_initial_buckets}
    , max_num_buckets{context->getSettingsRef().grace_hash_join_max_buckets}
    , max_block_size{context->getSettingsRef().max_block_size}
    , first_bucket{makeInMemoryJoin()}
{
    if (!GraceHashJoin::isSupported(table_join))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "GraceHashJoin is not supported for this join type");

    initial_num_buckets = roundUpToPowerOfTwoOrZero(initial_num_buckets);
    auto tmp = std::make_unique<Buckets>();
    for (size_t i = 0; i < initial_num_buckets; ++i)
    {
        addBucket(*tmp, nullptr);
    }
    buckets.set(std::move(tmp));
    LOG_TRACE(log, "Initialize {} buckets", initial_num_buckets);
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

void GraceHashJoin::rehashInMemoryJoin(InMemoryJoinPtr & join, const BucketsSnapshot & snapshot, size_t bucket)
{
    std::lock_guard<std::mutex> lock{join->mutex};

    auto prev_hash = join->emplace(table_join, right_sample_block, any_take_last_row);
    auto right_blocks = std::move(*prev_hash).releaseJoinedBlocks();

    for (const Block & block : right_blocks)
    {
        Blocks blocks = scatterBlock<true>(block, snapshot->size());
        join->join->addJoinedBlock(blocks[bucket], /* check_limits = */ false);
        for (size_t i = 1; i < snapshot->size(); ++i)
        {
            if (i != bucket && blocks[i].rows())
                snapshot->at(i)->addRightBlock(blocks[i]);
        }
    }
}

bool GraceHashJoin::fitsInMemory(InMemoryJoin * join) const
{
    /// One row can't be splitted, avoid loop
    if (join->join->getTotalRowCount() < 2)
        return true;

    return table_join->sizeLimits().softCheck(join->join->getTotalRowCount(), join->join->getTotalByteCount());
}

GraceHashJoin::BucketsSnapshot GraceHashJoin::rehash(size_t desired_size)
{
    desired_size = roundUpToPowerOfTwoOrZero(desired_size);

    std::scoped_lock lock{rehash_mutex};
    BucketsSnapshot snapshot = buckets.get();
    size_t current_size = snapshot->size();

    if (current_size >= desired_size)
        return snapshot;

    auto next_snapshot = std::make_unique<Buckets>(*snapshot);
    size_t next_size = std::max(current_size * 2, desired_size);

    if (next_size > max_num_buckets)
    {
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "Too many grace hash join buckets ({} > {}), consider increasing grace_hash_join_max_buckets or max_rows_in_join/max_bytes_in_join",
            next_size, max_num_buckets);
    }

    LOG_TRACE(log, "Rehashing from {} to {}", current_size, next_size);
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
    assert(first_bucket && first_bucket->join);
    return first_bucket->join->checkTypesOfKeys(block);
}

void GraceHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & /*not_processed*/)
{
    if (need_left_sample_block.exchange(false))
    {
        left_sample_block = block.cloneEmpty();
        output_sample_block = block.cloneEmpty();
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

    auto snapshot = buckets.get();
    auto blocks = scatterBlock<false>(block, snapshot->size());

    ExtraBlockPtr not_processed;
    first_bucket->join->joinBlock(blocks[0], not_processed);
    block = std::move(blocks[0]);
    if (not_processed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported hash join type");

    // We need to skip the first bucket that is already joined in memory, so we start with 1.
    auto indices = generateRandomPermutation(1, snapshot->size());
    retryForEach(
        indices,
        [&](size_t bucket)
        {
            Block & block_shard = blocks[bucket];
            if (block_shard.rows() == 0)
                return true;
            return snapshot->at(bucket)->tryAddLeftBlock(block_shard);
        });
}

void GraceHashJoin::setTotals(const Block & block)
{
    if (block)
    {
        std::scoped_lock guard{totals_mutex};
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
    auto snapshot = buckets.get();
    bool file_buckets_are_empty = std::all_of(snapshot->begin(), snapshot->end(), [](const auto & bucket) { return bucket->empty(); });
    bool first_bucket_is_empty = first_bucket && first_bucket->join && first_bucket->join->alwaysReturnsEmptySet();
    return isInnerOrRight(table_join->kind()) && first_bucket_is_empty && file_buckets_are_empty;
}

std::shared_ptr<NotJoinedBlocks> GraceHashJoin::getNonJoinedBlocks(const Block &, const Block &, UInt64) const
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
    std::shared_ptr<NotJoinedBlocks> not_joined_blocks;
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
            InMemoryJoinPtr join = makeInMemoryJoin();
            fillInMemoryJoin(join, bucket);
            return std::make_unique<DelayedBlocks>(this, bucket, std::move(join));
        }
    }

    // NB: this logic is a bit racy. There can be more buckets in the @snapshot in case of rehashing in different thread reading delayed blocks.
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
    iterator.join->join->joinBlock(block, not_processed);
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
        addJoinedBlockImpl(join, bucket->index(), block);
    }
}

void GraceHashJoin::addJoinedBlockImpl(InMemoryJoinPtr & join, size_t bucket_index, const Block & block)
{
    BucketsSnapshot snapshot = buckets.get();
    Blocks blocks = scatterBlock<true>(block, snapshot->size());

    // Add block to the in-memory join
    {
        auto bucket = snapshot->at(bucket_index);
        std::scoped_lock guard{bucket->joinMutex()};
        join->join->addJoinedBlock(blocks[bucket_index], /*check_limits=*/false);

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
    }

    if (blocks.empty())
        // All blocks were added to the @join
        return;

    // Write the rest of the blocks to the disk buckets
    assert(blocks.size() == snapshot->size());
    auto indices = generateRandomPermutation(1, snapshot->size());
    retryForEach(
        indices,
        [&](size_t bucket)
        {
            if (bucket == bucket_index || !blocks[bucket].rows())
                return true;
            return snapshot->at(bucket)->tryAddRightBlock(blocks[bucket]);
        });
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
