#include <Columns/ColumnSparse.h>
#include <Core/Block.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/Transforms/BlockNestedLoopJoinData.h>
#include <Common/ProfileEvents.h>

#include <optional>

namespace ProfileEvents
{
    extern const Event JoinBuildTableRowCount;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

bool needsBuildSideMatchFlags(JoinKind kind, JoinStrictness strictness)
{
    /// `ANY INNER` disables the cartesian product on both sides, so it needs the flags to give a
    /// build row to at most one probe row, even though it emits no build row of its own afterwards.
    if (strictness == JoinStrictness::Any && isInner(kind))
        return true;
    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
        return isRight(kind);
    return isRightOrFull(kind);
}

bool keepsUnmatchedBuildRows(JoinKind kind, JoinStrictness strictness)
{
    /// `RIGHT SEMI` is the exception among the right-driven kinds: it keeps a build row only when
    /// it does match, which is the other half of the same flag scan.
    if (strictness == JoinStrictness::Semi)
        return false;
    if (strictness == JoinStrictness::Anti)
        return isRight(kind);
    return isRightOrFull(kind);
}

namespace
{

/// The same rows, with every column decompressed. A block that was never compressed is returned as
/// it is, so this is cheap to call on a store that outgrew nothing.
BuildBlockPtr decompressBuildBlock(const StoredBlock & stored_block)
{
    Columns columns;
    columns.reserve(stored_block.columns.size());
    for (const auto & column : stored_block.columns)
        columns.push_back(column->decompress());

    auto result = std::make_shared<StoredBlock>(std::move(columns), ScatteredBlock::Selector(stored_block.selector.size()));
    result->block_no = stored_block.block_no;
    return result;
}

/// The same rows with every column that shrinks replaced by its compressed form, or `nullopt` when
/// none of them shrinks. A column that did not shrink comes back wrapped in a `ColumnCompressed`
/// that decompresses to that very column; keeping the wrapper would make every read materialize the
/// block anew - and charge the reader for memory it does not retain, cutting its output chunks
/// short - for no saving at all.
std::optional<StoredBlock> compressBuildBlock(const StoredBlock & stored_block)
{
    Columns columns;
    columns.reserve(stored_block.columns.size());
    bool any_compressed = false;
    for (const auto & column : stored_block.columns)
    {
        auto compressed_column = column->compress(/*force_compression=*/ false);
        if (compressed_column->byteSize() >= column->byteSize())
            compressed_column = column;
        else
            any_compressed = true;
        columns.push_back(std::move(compressed_column));
    }

    if (!any_compressed)
        return {};

    return StoredBlock(std::move(columns), ScatteredBlock::Selector(stored_block.selector.size()));
}

}

BlockNestedLoopJoinData::BlockNestedLoopJoinData(
    SharedHeader build_header_,
    JoinKind kind_,
    JoinStrictness strictness_,
    const SizeLimits & size_limits_,
    BlockNestedLoopStoreSettings store_settings_)
    : build_header(std::move(build_header_))
    , kind(kind_)
    , strictness(strictness_)
    , size_limits(size_limits_)
    , store_settings(std::move(store_settings_))
    , needs_match_flags(needsBuildSideMatchFlags(kind_, strictness_))
{
}

BlockNestedLoopJoinData::~BlockNestedLoopJoinData() = default;

bool BlockNestedLoopJoinData::canSpill() const
{
    /// A build side of columnless rows is row-count metadata rather than data: the `Native` format
    /// cannot persist it, and it costs no memory to keep.
    return store_settings.tmp_data != nullptr && build_header->columns() != 0;
}

bool BlockNestedLoopJoinData::addBlock(Block block, size_t num_rows)
{
    if (isFinished())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add a block to a finished block nested loop join build side");

    /// A block with no columns still carries rows, and the row count alone can decide the result
    /// (an empty build side is not the same as a build side of rows with nothing selected from it).
    chassert(block.columns() == 0 || block.rows() == num_rows);
    if (num_rows == 0)
        return true;

    assertCompatibleHeader(block, *build_header, "block nested loop join build side");

    /// The step outputs the concatenation of the input headers, whose columns are all full, and the
    /// probe builds its tiles by indexing into the stored columns. Const and Sparse cannot serve
    /// either purpose; Replicated can, and is kept as it is because unwrapping it would copy.
    Columns columns = block.getColumns();
    for (auto & column : columns)
        column = recursiveRemoveSparse(column->convertToFullColumnIfConst());

    StoredBlock stored_block(std::move(columns), ScatteredBlock::Selector(num_rows));
    const size_t block_bytes = stored_block.allocatedBytes();

    const size_t rows_in_join = total_rows.fetch_add(num_rows, std::memory_order_relaxed) + num_rows;
    const size_t bytes_in_join = total_bytes.fetch_add(block_bytes, std::memory_order_relaxed) + block_bytes;

    /// Compressing a block is per-block work with nothing shared in it, and under the store lock it
    /// would serialize the build streams. It has to happen before the lock decides whether the block
    /// stays in memory or is spilled, so it is speculative: a block that turns out to be spilled is
    /// written out uncompressed and the compressed copy is dropped. A store that has already started
    /// spilling never compresses again, since every later block goes to disk as well.
    std::optional<StoredBlock> compressed_block;
    if (getNumSpilledBlocks() == 0
        && ((store_settings.min_rows_to_compress != 0 && rows_in_join >= store_settings.min_rows_to_compress)
            || (store_settings.min_bytes_to_compress != 0 && bytes_in_join >= store_settings.min_bytes_to_compress)))
        compressed_block = compressBuildBlock(stored_block);

    {
        std::lock_guard lock(mutex);

        const size_t index = blocks.size();
        auto & entry = blocks.emplace_back();
        entry.num_rows = num_rows;

        const bool spill = canSpill()
            && (tmp_stream != nullptr
                || (store_settings.max_bytes_in_memory != 0
                    && getInMemoryBytes() + block_bytes > store_settings.max_bytes_in_memory));

        if (spill)
        {
            /// What is still in memory goes out first, so that the file order stays the index order.
            if (!tmp_stream)
                spillInMemoryBlocksLocked();
            spillBlock(entry, stored_block);
        }
        else if (compressed_block)
            storeBlock(entry, index, std::move(*compressed_block), /*compressed=*/ true, block_bytes);
        else
            storeBlock(entry, index, std::move(stored_block), /*compressed=*/ false, block_bytes);
    }

    ProfileEvents::increment(ProfileEvents::JoinBuildTableRowCount, num_rows);

    return size_limits.check(rows_in_join, bytes_in_join, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void BlockNestedLoopJoinData::storeBlock(
    BuildBlockEntry & entry, size_t index, StoredBlock stored_block, bool compressed, size_t uncompressed_bytes)
{
    stored_block.block_no = static_cast<UInt32>(index);
    entry.compressed = compressed;

    const size_t stored_bytes = stored_block.allocatedBytes();
    entry.block = std::make_shared<const StoredBlock>(std::move(stored_block));

    in_memory_bytes.fetch_add(stored_bytes, std::memory_order_relaxed);
    if (uncompressed_bytes > getMaxInMemoryBlockBytes())
        max_in_memory_block_bytes.store(uncompressed_bytes, std::memory_order_relaxed);
}

void BlockNestedLoopJoinData::spillBlock(BuildBlockEntry & entry, const StoredBlock & stored_block)
{
    if (!tmp_stream)
        tmp_stream = std::make_unique<TemporaryBlockStreamHolder>(build_header, store_settings.tmp_data);

    (*tmp_stream)->write(build_header->cloneWithColumns(stored_block.columns));
    entry.spill_ordinal = num_spilled_blocks.fetch_add(1, std::memory_order_relaxed);
}

/// TODO: a build side that does not fit in memory is spilled sequentially and re-read once per probe
/// chunk. Grace partitioning - partitioning both sides on a monotone part of the condition, if any -
/// would replace that with one pass per partition pair.
bool BlockNestedLoopJoinData::spillInMemoryBlocks(size_t min_bytes)
{
    std::lock_guard lock(mutex);

    /// Once the store is closed the temporary file is closed for writing too, and the readers the
    /// probe streams hold would not see anything appended to it anyway.
    if (finished.load(std::memory_order_relaxed) || !canSpill())
        return false;

    const size_t bytes_to_free = getInMemoryBytes();
    if (bytes_to_free == 0 || bytes_to_free < min_bytes)
        return false;

    spillInMemoryBlocksLocked();
    return true;
}

void BlockNestedLoopJoinData::spillInMemoryBlocksLocked()
{
    /// In increasing index order, so that the file order stays the index order: every block added
    /// after this point is written to the file too, and gets a higher index.
    for (auto & entry : blocks)
    {
        if (!entry.block)
            continue;

        auto to_write = entry.compressed ? decompressBuildBlock(*entry.block) : entry.block;
        spillBlock(entry, *to_write);
        entry.block.reset();
        entry.compressed = false;
    }

    in_memory_bytes.store(0, std::memory_order_relaxed);
    max_in_memory_block_bytes.store(0, std::memory_order_relaxed);
}

void BlockNestedLoopJoinData::setBuildSideTotals(Block totals)
{
    if (isFinished())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot set the totals of a finished block nested loop join build side");

    std::lock_guard lock(mutex);
    build_side_totals = std::move(totals);
}

const Block & BlockNestedLoopJoinData::getBuildSideTotals() const
{
    assertFinished("the build side totals");
    return TSA_SUPPRESS_WARNING_FOR_READ(build_side_totals);
}

void BlockNestedLoopJoinData::finish()
{
    std::lock_guard lock(mutex);

    if (finished.load(std::memory_order_relaxed))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block nested loop join build side is finished twice");

    row_offsets.resize(blocks.size() + 1);
    size_t offset = 0;
    for (size_t i = 0; i < blocks.size(); ++i)
    {
        row_offsets[i] = offset;
        offset += blocks[i].num_rows;
    }
    row_offsets.back() = offset;
    chassert(offset == total_rows.load(std::memory_order_relaxed));

    if (needs_match_flags && offset != 0)
        matched_flags = std::make_unique<std::atomic_bool[]>(offset);

    /// Nothing more will be written, and the readers can only be created once it is flushed.
    if (tmp_stream)
        tmp_stream->finishWriting();

    finished.store(true, std::memory_order_release);
}

void BlockNestedLoopJoinData::setBuildRowMatched(size_t global_row)
{
    chassert(global_row < getTotalRows());
    matched_flags[global_row].store(true, std::memory_order_relaxed);
}

bool BlockNestedLoopJoinData::claimBuildRow(size_t global_row)
{
    chassert(global_row < getTotalRows());
    /// An atomic read-modify-write, so exactly one probe stream finds the flag unset and takes the
    /// row. Relaxed is enough: nothing but the claim itself travels through this access.
    return !matched_flags[global_row].exchange(true, std::memory_order_relaxed);
}

bool BlockNestedLoopJoinData::isBuildRowMatched(size_t global_row) const
{
    chassert(global_row < getTotalRows());
    return matched_flags[global_row].load(std::memory_order_relaxed);
}

void BlockNestedLoopJoinData::assertFinished(const char * what) const
{
    if (!isFinished())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block nested loop join build side is not finished, cannot read {}", what);
}

size_t BlockNestedLoopJoinData::getNumBlocks() const
{
    assertFinished("the stored blocks");
    return TSA_SUPPRESS_WARNING_FOR_READ(blocks).size();
}

size_t BlockNestedLoopJoinData::getBlockNumRows(size_t index) const
{
    return getBlockEntry(index).num_rows;
}

const BlockNestedLoopJoinData::BuildBlockEntry & BlockNestedLoopJoinData::getBlockEntry(size_t index) const
{
    assertFinished("the stored blocks");
    return TSA_SUPPRESS_WARNING_FOR_READ(blocks).at(index);
}

TemporaryBlockStreamReaderHolder BlockNestedLoopJoinData::createSpillReadStream() const
{
    assertFinished("the spilled blocks");
    const auto & stream = TSA_SUPPRESS_WARNING_FOR_READ(tmp_stream);
    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block nested loop join build side has no spilled blocks");
    return stream->getReadStream();
}

const std::vector<size_t> & BlockNestedLoopJoinData::getRowOffsets() const
{
    assertFinished("the row offsets");
    return TSA_SUPPRESS_WARNING_FOR_READ(row_offsets);
}

bool BlockNestedLoopJoinData::isBlockSharedInMemory(size_t index) const
{
    const auto & entry = getBlockEntry(index);
    return entry.block != nullptr && !entry.compressed;
}

BuildSideBlockReader::BuildSideBlockReader(BlockNestedLoopJoinDataPtr data_)
    : data(std::move(data_))
{
}

BuildSideBlockReader::~BuildSideBlockReader() = default;

BuildBlockPtr BuildSideBlockReader::read(size_t index)
{
    if (current && current_index == index)
        return current;

    /// A read that throws must not leave `current_index` naming a block that `current` does not hold,
    /// or the fast path above would hand out the previous block under the new index.
    current.reset();
    const auto & entry = data->getBlockEntry(index);
    if (entry.block)
        current = entry.compressed ? decompressBuildBlock(*entry.block) : entry.block;
    else
        current = readSpilledBlock(index, entry.spill_ordinal);
    current_index = index;
    return current;
}

void BuildSideBlockReader::release()
{
    current.reset();
    spill_stream.reset();
    next_spill_ordinal = 0;
}

BuildBlockPtr BuildSideBlockReader::readSpilledBlock(size_t index, size_t spill_ordinal)
{
    /// The file holds the spilled blocks in index order and cannot be seeked, so a block behind the
    /// reader is only reachable by starting the file over - which is what a new probe chunk does.
    if (!spill_stream || spill_ordinal < next_spill_ordinal)
    {
        spill_stream = std::make_unique<TemporaryBlockStreamReaderHolder>(data->createSpillReadStream());
        next_spill_ordinal = 0;
    }

    Block block;
    while (next_spill_ordinal <= spill_ordinal)
    {
        block = (*spill_stream)->read();
        if (block.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Block nested loop join build side is missing the spilled block {} of {}",
                spill_ordinal, data->getNumSpilledBlocks());
        ++next_spill_ordinal;
    }

    /// The global row numbering was fixed when the block went out, so a block that comes back with
    /// a different row count would misname every build row after it.
    if (block.rows() != data->getBlockNumRows(index))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Block nested loop join read back {} rows for the spilled block {}, expected {}",
            block.rows(), index, data->getBlockNumRows(index));

    auto stored_block = std::make_shared<StoredBlock>(block.getColumns(), ScatteredBlock::Selector(block.rows()));
    stored_block->block_no = static_cast<UInt32>(index);
    return stored_block;
}

BlockNestedLoopBuildTransform::BlockNestedLoopBuildTransform(
    SharedHeader input_header, BlockNestedLoopJoinDataPtr data_, FinishCounterPtr finish_counter_)
    : IProcessor({std::move(input_header)}, {Block()})
    , data(std::move(data_))
    , finish_counter(std::move(finish_counter_))
{
    spillable = data->canSpill();
}

InputPort * BlockNestedLoopBuildTransform::addTotalsPort()
{
    if (inputs.size() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Totals port was already added to BlockNestedLoopBuildTransform");

    return &inputs.emplace_back(inputs.front().getHeader(), this);
}

IProcessor::Status BlockNestedLoopBuildTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

    if (output.isFinished())
    {
        for (auto & in : inputs)
            in.close();
        if (!build_finished)
        {
            finish_build_requested = true;
            return Status::Ready;
        }
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & in : inputs)
            in.setNotNeeded();
        return Status::PortFull;
    }

    if (stop_reading)
        input.close();
    else if (!input.isFinished())
    {
        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        chunk = input.pull(true);
        return Status::Ready;
    }

    /// The totals row is stored after the build rows, so that the store is closed only once it is in.
    if (inputs.size() > 1)
    {
        auto & totals_input = inputs.back();
        if (!totals_input.isFinished())
        {
            totals_input.setNeeded();

            if (!totals_input.hasData())
                return Status::NeedData;

            chunk = totals_input.pull(true);
            for_totals = true;
            return Status::Ready;
        }
    }

    if (!build_finished)
    {
        finish_build_requested = true;
        return Status::Ready;
    }

    output.finish();
    return Status::Finished;
}

void BlockNestedLoopBuildTransform::work()
{
    /// Closing the store allocates the match flags over the whole build side and flushes the
    /// temporary file, which is this processor's own work rather than something to do in `prepare`:
    /// that runs under the executor's node lock and is not timed as this processor's time.
    if (finish_build_requested)
    {
        finish_build_requested = false;
        finishBuild();
        return;
    }

    auto num_rows = chunk.getNumRows();
    auto block = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());
    if (for_totals)
        data->setBuildSideTotals(std::move(block));
    else
        stop_reading = !data->addBlock(std::move(block), num_rows);
}

ProcessorMemoryStats BlockNestedLoopBuildTransform::getMemoryStats()
{
    ProcessorMemoryStats stats;
    stats.spillable_memory_bytes = data->getInMemoryBytes();
    /// The blocks are written out one at a time, so what the spill needs on top of what it frees is
    /// the largest of them, counted uncompressed - once as the block it decompresses into, once as
    /// the buffer the temporary stream writes it through.
    stats.need_reserved_memory_bytes = 2 * data->getMaxInMemoryBlockBytes();
    return stats;
}

bool BlockNestedLoopBuildTransform::spillOnSize(size_t bytes)
{
    return data->spillInMemoryBlocks(bytes);
}

void BlockNestedLoopBuildTransform::finishBuild()
{
    /// Exactly one stream observes `isLast`, and it does so only once every stream has stopped
    /// appending, so the store is closed exactly once and never while a block is still going in.
    if (std::exchange(build_finished, true))
        return;

    if (finish_counter->isLast())
        data->finish();
}

}
