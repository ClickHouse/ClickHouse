#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Processors/Port.h>
#include <Processors/Transforms/BufferedShardByHashTransform.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int TOO_MANY_ROWS_OR_BYTES;
}

namespace
{

/// A `LowCardinality(String)` column whose dictionary dominates its size: `num_distinct` distinct
/// `value_len`-byte values (so the dictionary is ~`num_distinct * value_len` bytes) referenced by a
/// small `num_rows`-entry index. `num_rows >= num_distinct` so every distinct value is present.
ColumnPtr makeBigDictLowCardinalityColumn(size_t num_rows, size_t num_distinct, size_t value_len)
{
    auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto column = type->createColumn();
    const std::string base(value_len, 'x');
    for (size_t i = 0; i < num_rows; ++i)
    {
        const std::string value = base + std::to_string(i % num_distinct);
        column->insertData(value.data(), value.size());
    }
    return column;
}

/// UInt64 key column 0, 1, ..., num_rows - 1. Distinct keys spread across the shards by hash, so a
/// block of them lands non-empty chunks on (essentially) every shard.
ColumnPtr makeDistinctKeyColumn(size_t num_rows)
{
    auto column = ColumnUInt64::create();
    for (size_t i = 0; i < num_rows; ++i)
        column->insertValue(i);
    return column;
}

/// A plain UInt64 value column with distinct entries (no shared buffers), used as a shuffle payload.
ColumnPtr makeUInt64ValueColumn(size_t num_rows)
{
    auto column = ColumnUInt64::create();
    for (size_t i = 0; i < num_rows; ++i)
        column->insertValue(i * 2654435761ULL);
    return column;
}

/// Feed one input block (keyed by `key_columns`) to a `BufferedShardByHashTransform` with an unbounded
/// budget, drive a single pull-and-split cycle, and return the shared buffered-bytes counter with the
/// block's shard chunks still resident (parked in the output ports, none pulled downstream). This is the
/// exact quantity `aggregation_in_order_shuffle_max_buffered_bytes` is compared against, observed with no
/// dependence on pipeline scheduling.
Int64 bufferedBytesAfterSplit(const SharedHeader & header, Columns columns, size_t num_shards, const ColumnNumbers & key_columns)
{
    const size_t num_rows = columns.at(0)->size();
    auto budget = std::make_shared<BufferedShardByHashBudget>();

    /// Unbounded budget: this test measures the counter, it must not throw.
    BufferedShardByHashTransform transform(
        header, num_shards, key_columns, /*max_buffered_bytes_=*/ (1ULL << 40), budget);

    OutputPort source_output(header);
    connect(source_output, transform.getInputs().front());

    std::vector<std::unique_ptr<InputPort>> sinks;
    for (auto & output : transform.getOutputs())
    {
        auto sink = std::make_unique<InputPort>(header);
        connect(output, *sink);
        sinks.push_back(std::move(sink));
    }

    source_output.push(Chunk(std::move(columns), num_rows));
    /// The downstream sinks want data, so the transform's outputs can accept pushed chunks (canPush()).
    for (auto & sink : sinks)
        sink->setNeeded();

    /// prepare() pulls the single input chunk (Ready), work() splits it into the per-shard queues and pushes
    /// one chunk per shard into the ports. The next prepare() finds nothing pullable (no more input, ports
    /// hold data) and returns a non-Ready status, ending the loop with the whole block still buffered.
    for (int step = 0; step < 8; ++step)
    {
        if (transform.prepare() != IProcessor::Status::Ready)
            break;
        transform.work();
    }

    return budget->total_buffered_bytes.load();
}

struct PortResidencyOutcome
{
    Int64 while_parked;
    Int64 while_retained;
    Int64 after_released;
};

/// Drive one pull-and-split cycle, read the shared counter while the shard chunks are still parked in the
/// output ports, then pull them into retained chunks. A sorted merge does the same before it can advance an
/// input, so the budget must stay charged until those retained chunks are released.
PortResidencyOutcome portResidentThenRetainedThenReleasedBytes(
    const SharedHeader & header, Columns columns, size_t num_shards, const ColumnNumbers & key_columns)
{
    const size_t num_rows = columns.at(0)->size();
    auto budget = std::make_shared<BufferedShardByHashBudget>();

    BufferedShardByHashTransform transform(
        header, num_shards, key_columns, /*max_buffered_bytes_=*/ (1ULL << 40), budget);

    OutputPort source_output(header);
    connect(source_output, transform.getInputs().front());

    std::vector<std::unique_ptr<InputPort>> sinks;
    for (auto & output : transform.getOutputs())
    {
        auto sink = std::make_unique<InputPort>(header);
        connect(output, *sink);
        sinks.push_back(std::move(sink));
    }

    source_output.push(Chunk(std::move(columns), num_rows));
    for (auto & sink : sinks)
        sink->setNeeded();

    for (int step = 0; step < 8; ++step)
    {
        if (transform.prepare() != IProcessor::Status::Ready)
            break;
        transform.work();
    }
    const Int64 while_parked = budget->total_buffered_bytes.load();

    /// A `MergingSortedTransform` keeps pulled chunks in `current_inputs` until it advances that input.
    std::vector<Chunk> retained;
    for (auto & sink : sinks)
        if (sink->hasData())
            retained.push_back(sink->pull());
    const Int64 while_retained = budget->total_buffered_bytes.load();

    retained.clear();
    const Int64 after_released = budget->total_buffered_bytes.load();

    return {while_parked, while_retained, after_released};
}

struct BudgetedSplitOutcome
{
    bool work_threw_budget_error = false;  /// work() threw TOO_MANY_ROWS_OR_BYTES for the shared buffer budget.
    Int64 buffered_bytes = 0;              /// The shared counter after the drive (block parked in the ports if no throw).
};

/// Feed one input block to a transform with a budget of `max_buffered_bytes` bytes and drive prepare()/work()
/// cycles until the transform stops or work() throws the budget error. The single block exercises the admission
/// path end to end: the admission decision runs purely on the pulled chunk's measured size.
BudgetedSplitOutcome splitOneChunkUnderBudget(
    const SharedHeader & header, Columns columns, size_t num_shards, const ColumnNumbers & key_columns, size_t max_buffered_bytes)
{
    const size_t num_rows = columns.at(0)->size();
    auto budget = std::make_shared<BufferedShardByHashBudget>();

    BufferedShardByHashTransform transform(
        header, num_shards, key_columns, max_buffered_bytes, budget);

    OutputPort source_output(header);
    connect(source_output, transform.getInputs().front());

    std::vector<std::unique_ptr<InputPort>> sinks;
    for (auto & output : transform.getOutputs())
    {
        auto sink = std::make_unique<InputPort>(header);
        connect(output, *sink);
        sinks.push_back(std::move(sink));
    }

    source_output.push(Chunk(std::move(columns), num_rows));
    for (auto & sink : sinks)
        sink->setNeeded();

    BudgetedSplitOutcome outcome;
    for (int step = 0; step < 8; ++step)
    {
        if (transform.prepare() != IProcessor::Status::Ready)
            break;
        try
        {
            transform.work();
        }
        catch (const Exception & e)
        {
            outcome.work_threw_budget_error = e.code() == ErrorCodes::TOO_MANY_ROWS_OR_BYTES;
            break;
        }
    }
    outcome.buffered_bytes = budget->total_buffered_bytes.load();
    return outcome;
}

struct SecondPullOutcome
{
    bool second_chunk_pulled;        /// The second input chunk was pulled (consumed from the input port).
    bool work_threw_budget_error;    /// work() threw TOO_MANY_ROWS_OR_BYTES for the shared buffer budget.
};

/// Drive one scatter to the point where it pulls a *second* input chunk (of `second_rows` rows) while the first
/// block (of `first_rows` rows) is still buffered, under a shared budget of `max_buffered_bytes` bytes. Priming
/// the first block leaves its charge resident in the output ports; draining one lane makes that output starve so
/// the transform wants to pull again. Returns whether the second chunk was pulled and whether work() then threw
/// the buffer-budget error. The admission decision runs on the second chunk's own measured size: a second chunk
/// whose actual bytes fit the remaining budget proceeds, and one whose actual bytes cross the cap makes work()
/// throw before it is split.
SecondPullOutcome attemptSecondPull(size_t first_rows, size_t second_rows, size_t num_shards, size_t max_buffered_bytes)
{
    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "v"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));
    auto budget = std::make_shared<BufferedShardByHashBudget>();

    BufferedShardByHashTransform transform(
        header, num_shards, ColumnNumbers{0}, max_buffered_bytes, budget);

    OutputPort source_output(header);
    connect(source_output, transform.getInputs().front());

    std::vector<std::unique_ptr<InputPort>> sinks;
    for (auto & output : transform.getOutputs())
    {
        auto sink = std::make_unique<InputPort>(header);
        connect(output, *sink);
        sinks.push_back(std::move(sink));
    }
    for (auto & sink : sinks)
        sink->setNeeded();

    /// Prime: push the first block and split it into the ports (parked, charged).
    source_output.push(Chunk(Columns{makeDistinctKeyColumn(first_rows), makeUInt64ValueColumn(first_rows)}, first_rows));
    for (int step = 0; step < 8; ++step)
    {
        if (transform.prepare() != IProcessor::Status::Ready)
            break;
        transform.work();
    }

    /// The downstream merge consumes one lane, so that output starves for more data on the next prepare(). The
    /// block's charge stays resident (its other shard chunks are still parked), so the shared counter still
    /// holds the first block's bytes when the second pull is considered.
    if (sinks.front()->hasData())
        sinks.front()->pull();

    /// Present a second block and run one admission cycle on the now-starving lane.
    source_output.push(Chunk(Columns{makeDistinctKeyColumn(second_rows), makeUInt64ValueColumn(second_rows)}, second_rows));
    const auto status = transform.prepare();
    const bool second_chunk_pulled = !source_output.hasData();

    bool work_threw_budget_error = false;
    if (status == IProcessor::Status::Ready)
    {
        try
        {
            transform.work();
        }
        catch (const Exception & e)
        {
            work_threw_budget_error = e.code() == ErrorCodes::TOO_MANY_ROWS_OR_BYTES;
        }
    }

    return {second_chunk_pulled, work_threw_budget_error};
}

/// Drive one scatter through two SEPARATE input blocks, draining exactly one lane between the two pushes so
/// the first block's remaining shard chunks stay resident while a starving output opens up to admit the
/// second. Returns the shared counter with both blocks' shard chunks still buffered. Used to check that a
/// physical buffer referenced, by pointer, by both blocks (not just within one of them) is only charged once.
Int64 twoBlocksResidentBytes(
    const SharedHeader & header, Columns first_columns, Columns second_columns, size_t num_shards, const ColumnNumbers & key_columns)
{
    const size_t first_rows = first_columns.at(0)->size();
    const size_t second_rows = second_columns.at(0)->size();
    auto budget = std::make_shared<BufferedShardByHashBudget>();

    /// Unbounded budget: this test measures the counter, it must not throw.
    BufferedShardByHashTransform transform(
        header, num_shards, key_columns, /*max_buffered_bytes_=*/ (1ULL << 40), budget);

    OutputPort source_output(header);
    connect(source_output, transform.getInputs().front());

    std::vector<std::unique_ptr<InputPort>> sinks;
    for (auto & output : transform.getOutputs())
    {
        auto sink = std::make_unique<InputPort>(header);
        connect(output, *sink);
        sinks.push_back(std::move(sink));
    }
    for (auto & sink : sinks)
        sink->setNeeded();

    source_output.push(Chunk(std::move(first_columns), first_rows));
    for (int step = 0; step < 8; ++step)
    {
        if (transform.prepare() != IProcessor::Status::Ready)
            break;
        transform.work();
    }

    /// The downstream merge consumes one lane, so that output starves for more data on the next prepare(). The
    /// first block's charge otherwise stays resident (its other shard chunks are still parked).
    if (sinks.front()->hasData())
        sinks.front()->pull();

    source_output.push(Chunk(std::move(second_columns), second_rows));
    for (int step = 0; step < 8; ++step)
    {
        if (transform.prepare() != IProcessor::Status::Ready)
            break;
        transform.work();
    }

    return budget->total_buffered_bytes.load();
}

/// Drive TWO separate scatters that SHARE one budget, each buffering a block whose payload column is the
/// identical `ColumnPtr` (by pointer, as a constant the query evaluates once and every stream references).
/// Both blocks are left resident in their scatters' ports simultaneously. Returns the shared counter, used to
/// check that a physical buffer held by more than one scatter at once is charged once for the whole stage, not
/// once per scatter.
Int64 twoScattersSharingBudgetResidentBytes(
    const SharedHeader & header, Columns first_columns, Columns second_columns, size_t num_shards, const ColumnNumbers & key_columns)
{
    auto budget = std::make_shared<BufferedShardByHashBudget>();

    /// Everything below must outlive the measurement: each scatter holds raw pointers to its connected ports.
    std::vector<std::shared_ptr<BufferedShardByHashTransform>> transforms;
    std::vector<std::unique_ptr<OutputPort>> sources;
    std::vector<std::vector<std::unique_ptr<InputPort>>> sinks_per_transform;

    auto add_and_buffer = [&](Columns columns)
    {
        const size_t num_rows = columns.at(0)->size();
        auto transform = std::make_shared<BufferedShardByHashTransform>(
            header, num_shards, key_columns, /*max_buffered_bytes_=*/ (1ULL << 40), budget);

        auto source = std::make_unique<OutputPort>(header);
        connect(*source, transform->getInputs().front());

        std::vector<std::unique_ptr<InputPort>> sinks;
        for (auto & output : transform->getOutputs())
        {
            auto sink = std::make_unique<InputPort>(header);
            connect(output, *sink);
            sinks.push_back(std::move(sink));
        }
        for (auto & sink : sinks)
            sink->setNeeded();

        source->push(Chunk(std::move(columns), num_rows));
        /// Split the block into the ports and leave it resident (nothing is pulled downstream).
        for (int step = 0; step < 8; ++step)
        {
            if (transform->prepare() != IProcessor::Status::Ready)
                break;
            transform->work();
        }

        transforms.push_back(std::move(transform));
        sources.push_back(std::move(source));
        sinks_per_transform.push_back(std::move(sinks));
    };

    add_and_buffer(std::move(first_columns));
    add_and_buffer(std::move(second_columns));

    return budget->total_buffered_bytes.load();
}

/// One scatter of a stage, with the standalone ports it is connected to (both must outlive it: the transform
/// keeps raw pointers to them).
struct ConnectedScatter
{
    std::shared_ptr<BufferedShardByHashTransform> transform;
    std::unique_ptr<OutputPort> source;
    std::vector<std::unique_ptr<InputPort>> sinks;
};

/// Drive prepare()/work() cycles until the transform stops or work() throws the buffer-budget error, which is
/// what the returned flag reports.
bool drive(const ConnectedScatter & scatter)
{
    for (int step = 0; step < 8; ++step)
    {
        if (scatter.transform->prepare() != IProcessor::Status::Ready)
            break;
        try
        {
            scatter.transform->work();
        }
        catch (const Exception & e)
        {
            return e.code() == ErrorCodes::TOO_MANY_ROWS_OR_BYTES;
        }
    }
    return false;
}

ConnectedScatter makeConnectedScatter(
    const SharedHeader & header,
    size_t num_shards,
    const ColumnNumbers & key_columns,
    size_t max_buffered_bytes,
    const std::shared_ptr<BufferedShardByHashBudget> & budget)
{
    ConnectedScatter scatter;
    scatter.transform = std::make_shared<BufferedShardByHashTransform>(
        header, num_shards, key_columns, max_buffered_bytes, budget);

    scatter.source = std::make_unique<OutputPort>(header);
    connect(*scatter.source, scatter.transform->getInputs().front());

    for (auto & output : scatter.transform->getOutputs())
    {
        auto sink = std::make_unique<InputPort>(header);
        connect(output, *sink);
        scatter.sinks.push_back(std::move(sink));
    }
    for (auto & sink : scatter.sinks)
        sink->setNeeded();

    return scatter;
}

struct StalePullOutcome
{
    Int64 bytes_while_first_parked = 0; /// Shared counter with the first scatter's block parked in its ports.
    Int64 bytes_after_second_buffered = 0; /// Shared counter once the second scatter has buffered its own block.
    bool second_threw_budget_error = false; /// The second scatter threw TOO_MANY_ROWS_OR_BYTES.
};

/// Two scatters sharing one budget, as a shuffle stage runs them (one per input stream). The first buffers a
/// block, and its downstream merges then pull `first_sinks_to_pull` of the parked chunks out of its ports and
/// destroy them - but the first scatter is NOT scheduled again, so it has not itself released those charges. The
/// second scatter then runs its own block through the admission check and buffers it.
///
/// The second scatter's block is deliberately built AFTER the pulled chunks are freed, so its columns can be
/// allocated at their recycled addresses: the budget's de-duplication table is keyed by column address, so this
/// is exactly the interleaving in which a stale entry for a consumed chunk would be mistaken for the new block's
/// buffer (charged nothing, then billed the stale entry's bytes when it is eventually released).
StalePullOutcome runSecondScatterAfterFirstIsDrained(
    size_t first_rows, size_t second_rows, size_t num_shards, size_t max_buffered_bytes, size_t first_sinks_to_pull)
{
    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "v"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));
    auto budget = std::make_shared<BufferedShardByHashBudget>();

    auto first = makeConnectedScatter(header, num_shards, ColumnNumbers{0}, max_buffered_bytes, budget);
    auto second = makeConnectedScatter(header, num_shards, ColumnNumbers{0}, max_buffered_bytes, budget);

    first.source->push(
        Chunk(Columns{makeDistinctKeyColumn(first_rows), makeUInt64ValueColumn(first_rows)}, first_rows));

    /// The first scatter buffers its block: one chunk parked in each of its output ports, all charged.
    drive(first);

    StalePullOutcome outcome;
    outcome.bytes_while_first_parked = budget->total_buffered_bytes.load();

    /// Its downstream merges pull the parked chunks and drop them. Nothing calls back into the first scatter at
    /// the pull, and the executor does not have to schedule it before the second scatter runs, so its charges
    /// stay in the shared counter although the bytes are gone.
    size_t pulled = 0;
    for (auto & sink : first.sinks)
    {
        if (pulled == first_sinks_to_pull)
            break;
        if (sink->hasData())
        {
            sink->pull();
            ++pulled;
        }
    }

    second.source->push(
        Chunk(Columns{makeDistinctKeyColumn(second_rows), makeUInt64ValueColumn(second_rows)}, second_rows));
    outcome.second_threw_budget_error = drive(second);
    outcome.bytes_after_second_buffered = budget->total_buffered_bytes.load();

    return outcome;
}

}

/// The admission decision for the shared buffer budget runs on each pulled chunk's measured size: a chunk whose
/// actual bytes cross the cap is rejected right after the pull (work() throws TOO_MANY_ROWS_OR_BYTES before the
/// chunk is split, so no over-budget data buffers), while under a budget with room for it the same chunk
/// proceeds normally. This drives one scatter to a second pull that lands the counter past the cap and checks
/// work() throws, versus a control budget with room for it.
TEST(BufferedShardByHashTransform, ChunkThatCrossesBudgetThrowsBeforeSplit)
{
    const size_t num_rows = 4000;
    const size_t num_shards = 8;

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "v"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    /// Bytes actually resident after one block is split (what the counter holds while the block is parked)...
    const Int64 resident_after_split = bufferedBytesAfterSplit(
        header, Columns{makeDistinctKeyColumn(num_rows), makeUInt64ValueColumn(num_rows)}, num_shards, ColumnNumbers{0});
    ASSERT_GT(resident_after_split, 0);
    /// ...and the pre-split size of the next chunk, which is what admission charges when it is pulled.
    const Int64 second_chunk_bytes = static_cast<Int64>(
        Chunk(Columns{makeDistinctKeyColumn(num_rows), makeUInt64ValueColumn(num_rows)}, num_rows).allocatedBytes());
    ASSERT_GT(second_chunk_bytes, 0);

    /// Budget large enough to hold the first block but not the first block plus the second chunk: the second
    /// chunk's measured size crosses the cap, so work() must throw before splitting it. (The budget exceeds
    /// `resident_after_split`, so priming the first block never trips it.)
    const auto tight = attemptSecondPull(
        num_rows, num_rows, num_shards, static_cast<size_t>(resident_after_split + second_chunk_bytes / 2));
    EXPECT_TRUE(tight.second_chunk_pulled);
    EXPECT_TRUE(tight.work_threw_budget_error);

    /// Budget with room for both: the second chunk is admitted and split normally (control).
    const auto loose = attemptSecondPull(
        num_rows, num_rows, num_shards, static_cast<size_t>(resident_after_split + 2 * second_chunk_bytes));
    EXPECT_TRUE(loose.second_chunk_pulled);
    EXPECT_FALSE(loose.work_threw_budget_error);
}

/// Admission is decided on each chunk's *own* measured size, never on an estimate carried over from an earlier,
/// wider chunk. On a variable-width stream a wide block can leave the buffered bytes high, but a following
/// narrow chunk whose actual bytes fit the remaining headroom must still be admitted - rejecting it on anything
/// but its measured size would fail a query whose buffered bytes never cross the cap. This primes a wide block,
/// then pulls a narrow chunk under a budget with headroom for the narrow chunk's actual bytes (but not for
/// another wide chunk), and checks the narrow chunk is pulled and processed without the budget error.
TEST(BufferedShardByHashTransform, NarrowChunkAfterWideBlockAdmittedOnMeasuredSize)
{
    const size_t wide_rows = 20000;
    const size_t narrow_rows = 500;
    const size_t num_shards = 8;

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "v"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    const Int64 wide_resident_after_split = bufferedBytesAfterSplit(
        header, Columns{makeDistinctKeyColumn(wide_rows), makeUInt64ValueColumn(wide_rows)}, num_shards, ColumnNumbers{0});
    ASSERT_GT(wide_resident_after_split, 0);
    /// Half a wide chunk of headroom above the first block's resident bytes: enough for the narrow chunk (both
    /// its pre-split measured size and its post-split resident bytes), but far short of another wide chunk.
    const Int64 wide_chunk_bytes = static_cast<Int64>(
        Chunk(Columns{makeDistinctKeyColumn(wide_rows), makeUInt64ValueColumn(wide_rows)}, wide_rows).allocatedBytes());
    const Int64 narrow_chunk_bytes = static_cast<Int64>(
        Chunk(Columns{makeDistinctKeyColumn(narrow_rows), makeUInt64ValueColumn(narrow_rows)}, narrow_rows).allocatedBytes());
    const Int64 narrow_resident_after_split = bufferedBytesAfterSplit(
        header, Columns{makeDistinctKeyColumn(narrow_rows), makeUInt64ValueColumn(narrow_rows)}, num_shards, ColumnNumbers{0});
    const Int64 headroom = wide_chunk_bytes / 2;
    ASSERT_LT(narrow_chunk_bytes, headroom);
    ASSERT_LT(narrow_resident_after_split, headroom);

    /// The narrow chunk's actual bytes fit this budget; only a stale wide-chunk estimate would spuriously throw.
    const auto outcome = attemptSecondPull(
        wide_rows, narrow_rows, num_shards, static_cast<size_t>(wide_resident_after_split + headroom));
    EXPECT_TRUE(outcome.second_chunk_pulled);
    EXPECT_FALSE(outcome.work_threw_budget_error);
}

/// The canonical shuffle buffer-budget regression: `ColumnLowCardinality::scatter` shares one dictionary
/// across all shard chunks of a block, so the budget must charge that dictionary exactly once per block.
/// Charging it per shard (a naive `allocatedBytes()` sum) inflates the counter up to `num_shards` times and
/// trips the budget on safe inputs; dropping it (`Chunk::bytes()` reports zero for a shared dictionary)
/// under-counts so a query buffering many dictionaries never trips the budget. With the dictionary sized to
/// dominate the block, a correct "once per block" charge lands the counter in [one dictionary, two
/// dictionaries); the two bugs land it below one and at/above `num_shards`, respectively.
TEST(BufferedShardByHashTransform, LowCardinalityDictionaryChargedOncePerBlock)
{
    const size_t num_rows = 8000;
    const size_t num_distinct = 2000;
    const size_t value_len = 500;
    const size_t num_shards = 8;

    ColumnPtr key = makeDistinctKeyColumn(num_rows);
    ColumnPtr low_cardinality = makeBigDictLowCardinalityColumn(num_rows, num_distinct, value_len);

    /// `scatter` clones the whole dictionary once and shares it across every shard chunk regardless of the
    /// selector, so this is the exact per-block dictionary size the transform must charge once.
    IColumn::Selector selector(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        selector[i] = i % num_shards;
    auto scattered = low_cardinality->scatter(num_shards, selector);
    const Int64 dict_bytes
        = static_cast<Int64>(assert_cast<const ColumnLowCardinality &>(*scattered[0]).getDictionary().allocatedBytes());
    ASSERT_GT(dict_bytes, 0);

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "s"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    Columns columns{key, low_cardinality};
    const Int64 buffered = bufferedBytesAfterSplit(header, std::move(columns), num_shards, ColumnNumbers{0});

    /// Charged once: at least one dictionary (a dropped dictionary would leave only the small index and key
    /// bytes, below one), and below two dictionaries (charging per shard would reach ~`num_shards`).
    EXPECT_GE(buffered, dict_bytes);
    EXPECT_LT(buffered, 2 * dict_bytes);
}

/// The pre-split admission charge must count an *owned* dictionary exactly once. A freshly read block owns its
/// dictionary (sharing only appears once `scatter` splits it), `forEachSubcolumn` visits an owned dictionary
/// (it skips only a shared one), and `allocatedBytes` contains it exactly once - so also running the explicit
/// shared-dictionary case for it subtracted the dictionary from the column's own bytes twice, billing the block
/// a full dictionary short on the admission path: a chunk already over
/// `aggregation_in_order_shuffle_max_buffered_bytes` was admitted and only failed after the split had
/// materialized it. This reads the counter between prepare() (which pulls and registers the provisional
/// pre-split charge, the value the admission decision runs on) and work().
TEST(BufferedShardByHashTransform, OwnedLowCardinalityDictionaryCountedOnAdmission)
{
    const size_t num_rows = 8000;
    const size_t num_distinct = 2000;
    const size_t value_len = 500;
    const size_t num_shards = 8;

    ColumnPtr key = makeDistinctKeyColumn(num_rows);
    ColumnPtr low_cardinality = makeBigDictLowCardinalityColumn(num_rows, num_distinct, value_len);

    const auto & lc = assert_cast<const ColumnLowCardinality &>(*low_cardinality);
    ASSERT_FALSE(lc.isSharedDictionary());
    const Int64 dict_bytes = static_cast<Int64>(lc.getDictionary().allocatedBytes());
    ASSERT_GT(dict_bytes, 0);

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "s"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    auto budget = std::make_shared<BufferedShardByHashBudget>();

    /// Unbounded budget: this test measures the counter, it must not throw.
    BufferedShardByHashTransform transform(
        header, num_shards, ColumnNumbers{0}, /*max_buffered_bytes_=*/ (1ULL << 40), budget);

    OutputPort source_output(header);
    connect(source_output, transform.getInputs().front());

    std::vector<std::unique_ptr<InputPort>> sinks;
    for (auto & output : transform.getOutputs())
    {
        auto sink = std::make_unique<InputPort>(header);
        connect(output, *sink);
        sinks.push_back(std::move(sink));
    }

    source_output.push(Chunk(Columns{key, low_cardinality}, num_rows));
    for (auto & sink : sinks)
        sink->setNeeded();

    ASSERT_EQ(transform.prepare(), IProcessor::Status::Ready);
    const Int64 admission_bytes = budget->total_buffered_bytes.load();

    /// At least one dictionary (the double subtraction left only the small index and key bytes, below one),
    /// and below two (it must still be billed once, not once per reference).
    EXPECT_GE(admission_bytes, dict_bytes);
    EXPECT_LT(admission_bytes, 2 * dict_bytes);
}

/// Same invariant for a dictionary nested inside a composite column: `ColumnTuple::scatter` delegates to the
/// nested `ColumnLowCardinality::scatter`, which shares one dictionary across the shards the same way. The
/// budget walks the shard chunk recursively, so a `Tuple(LowCardinality(String))` dictionary must also be
/// charged exactly once per block - special-casing only a top-level `LowCardinality` would charge the nested
/// dictionary per shard and trip the budget spuriously on safe composite inputs.
TEST(BufferedShardByHashTransform, WrappedLowCardinalityDictionaryChargedOncePerBlock)
{
    const size_t num_rows = 8000;
    const size_t num_distinct = 2000;
    const size_t value_len = 500;
    const size_t num_shards = 8;

    ColumnPtr key = makeDistinctKeyColumn(num_rows);
    ColumnPtr low_cardinality = makeBigDictLowCardinalityColumn(num_rows, num_distinct, value_len);
    ColumnPtr tuple = ColumnTuple::create(Columns{low_cardinality});

    IColumn::Selector selector(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        selector[i] = i % num_shards;
    auto scattered = tuple->scatter(num_shards, selector);
    const auto & scattered_tuple = assert_cast<const ColumnTuple &>(*scattered[0]);
    const Int64 dict_bytes = static_cast<Int64>(
        assert_cast<const ColumnLowCardinality &>(scattered_tuple.getColumn(0)).getDictionary().allocatedBytes());
    ASSERT_GT(dict_bytes, 0);

    auto tuple_type = std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())});
    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(tuple_type, "s"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    Columns columns{key, tuple};
    const Int64 buffered = bufferedBytesAfterSplit(header, std::move(columns), num_shards, ColumnNumbers{0});

    EXPECT_GE(buffered, dict_bytes);
    EXPECT_LT(buffered, 2 * dict_bytes);
}

/// The same invariant for the arena of a `ColumnAggregateFunction` (an `AggregatingMergeTree` payload, or any
/// `-State` aggregate argument): the states live in an arena the column merely keeps alive, and
/// `ColumnAggregateFunction::scatter` hands every shard a *view* whose data are the same state pointers, with the
/// source's arena moved into the view's foreign arenas and the source column itself held alive.
/// `allocatedBytes()` misreports that in both directions - it counts an owned arena in full (so several columns
/// reaching one arena would charge it once each, tripping `aggregation_in_order_shuffle_max_buffered_bytes`
/// spuriously) and ignores a foreign one entirely (so the shard views of a block would charge the states nothing,
/// letting an arbitrarily large arena stay buffered outside the cap). The budget must charge the arena exactly
/// once for as long as any buffered chunk reaches it.
TEST(BufferedShardByHashTransform, AggregateFunctionArenaChargedOncePerBlock)
{
    tryRegisterAggregateFunctions();

    const size_t num_rows = 2000;
    const size_t num_shards = 8;
    /// Each `groupArray` state accumulates its elements in the arena, so the arena dominates the block: the
    /// column itself holds only one 8-byte state pointer per row.
    const size_t values_per_state = 64;

    AggregateFunctionProperties properties;
    auto aggregate_function = AggregateFunctionFactory::instance().get(
        "groupArray", NullsAction::EMPTY, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{}, properties);

    auto states = ColumnAggregateFunction::create(aggregate_function);
    auto argument = ColumnUInt64::create();
    for (size_t i = 0; i < values_per_state; ++i)
        argument->insertValue(i);
    const IColumn * argument_columns[1] = {argument.get()};

    Arena & arena = states->createOrGetArena();
    for (size_t row = 0; row < num_rows; ++row)
    {
        states->insertDefault();
        for (size_t i = 0; i < values_per_state; ++i)
            aggregate_function->add(states->getData()[row], argument_columns, i, &arena);
    }

    const Int64 arena_bytes = static_cast<Int64>(arena.allocatedBytes());
    /// The arena must dominate: otherwise the assertions below could not tell the two failure modes apart.
    ASSERT_GT(arena_bytes, static_cast<Int64>(4 * num_rows * sizeof(void *)));

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeAggregateFunction>(aggregate_function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{}), "s"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    Columns columns{makeDistinctKeyColumn(num_rows), std::move(states)};
    const Int64 buffered = bufferedBytesAfterSplit(header, std::move(columns), num_shards, ColumnNumbers{0});

    /// Charged once: at least the whole arena (dropping the foreign arenas of the shard views would leave only
    /// the state-pointer arrays, far below it), and below two arenas (charging it per column that reaches it
    /// would reach ~`num_shards` arenas, since every shard view holds the same one).
    EXPECT_GE(buffered, arena_bytes);
    EXPECT_LT(buffered, 2 * arena_bytes);
}

/// A foreign arena attached from outside any column (`addArena` - e.g. an aggregator pool arena) may still be
/// grown concurrently by whoever created it, and `ColumnAggregateFunction` documents even reading such arenas
/// as unsafe. The budget walk therefore must not measure it: only the owned arena - grown exclusively through
/// the column being walked, which the transform holds immutably - is charged. This deliberately leaves an
/// externally attached arena uncharged (it cannot be sized without a data race), which this test pins down.
TEST(BufferedShardByHashTransform, ExternallyAttachedForeignArenaIsChargedOncePerBlock)
{
    tryRegisterAggregateFunctions();

    const size_t num_rows = 2000;
    const size_t num_shards = 8;
    const size_t values_per_state = 64;

    AggregateFunctionProperties properties;
    auto aggregate_function = AggregateFunctionFactory::instance().get(
        "groupArray", NullsAction::EMPTY, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{}, properties);

    auto states = ColumnAggregateFunction::create(aggregate_function);
    auto argument = ColumnUInt64::create();
    for (size_t i = 0; i < values_per_state; ++i)
        argument->insertValue(i);
    const IColumn * argument_columns[1] = {argument.get()};

    Arena & arena = states->createOrGetArena();
    for (size_t row = 0; row < num_rows; ++row)
    {
        states->insertDefault();
        for (size_t i = 0; i < values_per_state; ++i)
            aggregate_function->add(states->getData()[row], argument_columns, i, &arena);
    }

    const Int64 arena_bytes = static_cast<Int64>(arena.allocatedBytes());

    /// An external arena much larger than everything else in the block, attached the way
    /// `FunctionBinaryArithmetic` and the `Aggregator` output attach theirs: `addArena` alone, owned by no
    /// column. Its states are resident for as long as the block is buffered, so the budget must bill it -
    /// and, since every shard view of the block shares the same arena, bill it exactly once.
    auto external_arena = std::make_shared<Arena>();
    external_arena->alloc(64 * 1024 * 1024);
    const Int64 external_bytes = static_cast<Int64>(external_arena->allocatedBytes());
    states->addArena(external_arena);
    ASSERT_GT(external_bytes, 8 * arena_bytes);

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeAggregateFunction>(aggregate_function, DataTypes{std::make_shared<DataTypeUInt64>()}, Array{}), "s"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    Columns columns{makeDistinctKeyColumn(num_rows), std::move(states)};
    const Int64 buffered = bufferedBytesAfterSplit(header, std::move(columns), num_shards, ColumnNumbers{0});

    /// The external arena is billed on top of the owned one (dropping it would leave the states of an
    /// `addArena`-only column entirely uncounted), and billed once - not once per shard view that shares it.
    EXPECT_GE(buffered, external_bytes + arena_bytes);
    EXPECT_LT(buffered, 2 * external_bytes);
}

/// The same invariant for a shared `ColumnConst` payload: `ColumnConst::scatter` wraps the same backing `data`
/// column for every shard chunk while each wrapper's `allocatedBytes()` reports the full payload, so the budget
/// must charge a shared const payload exactly once per block - charging it per shard would inflate the counter
/// up to `num_shards` times and trip `aggregation_in_order_shuffle_max_buffered_bytes` spuriously on queries
/// with a wide constant column (e.g. a large constant aggregate argument).
TEST(BufferedShardByHashTransform, ConstColumnPayloadChargedOncePerBlock)
{
    const size_t num_rows = 8000;
    const size_t num_shards = 8;
    const size_t value_len = 1 << 20;  /// A 1 MiB constant string payload - dominates the block's bytes.

    ColumnPtr key = makeDistinctKeyColumn(num_rows);

    auto payload = ColumnString::create();
    const std::string big_value(value_len, 'x');
    payload->insertData(big_value.data(), big_value.size());
    ColumnPtr constant = ColumnConst::create(std::move(payload), num_rows);
    const Int64 payload_bytes
        = static_cast<Int64>(assert_cast<const ColumnConst &>(*constant).getDataColumn().allocatedBytes());
    ASSERT_GT(payload_bytes, static_cast<Int64>(value_len));

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "c"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    Columns columns{key, constant};
    const Int64 buffered = bufferedBytesAfterSplit(header, std::move(columns), num_shards, ColumnNumbers{0});

    /// Charged once: at least one payload, and below two payloads (charging per shard would reach
    /// ~`num_shards` payloads).
    EXPECT_GE(buffered, payload_bytes);
    EXPECT_LT(buffered, 2 * payload_bytes);
}

/// The admission charge must not double-count a buffer the source chunk references more than once. One
/// `ColumnConst` literal projected into two columns of the block (`SELECT big AS a, big AS b`, a constant
/// argument fed to two aggregate functions) puts the same column object into the chunk twice, and the chunk's
/// raw `allocatedBytes()` sums both references; charging that raw sum on admission arms the budget rejection for
/// bytes the chunk does not actually hold, so the query throws TOO_MANY_ROWS_OR_BYTES even though the exact
/// resident bytes (the post-split accounting de-duplicates the shared payload) fit under the budget. With a
/// budget between the payload counted once and counted twice, the aliased chunk must be admitted, split, and
/// charged for a single payload.
TEST(BufferedShardByHashTransform, AliasedColumnsChargedOncePerChunkOnAdmission)
{
    const size_t num_rows = 8000;
    const size_t num_shards = 8;
    const size_t value_len = 1 << 20;  /// A 1 MiB constant string payload - dominates the chunk's bytes.

    ColumnPtr key = makeDistinctKeyColumn(num_rows);

    auto payload = ColumnString::create();
    const std::string big_value(value_len, 'x');
    payload->insertData(big_value.data(), big_value.size());
    ColumnPtr constant = ColumnConst::create(std::move(payload), num_rows);
    const Int64 payload_bytes
        = static_cast<Int64>(assert_cast<const ColumnConst &>(*constant).getDataColumn().allocatedBytes());
    ASSERT_GT(payload_bytes, static_cast<Int64>(value_len));

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "a"),
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "b"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    /// The same `ColumnConst` object referenced twice: a raw pre-split sum counts its payload twice.
    Columns columns{key, constant, constant};
    /// The chunk's raw double-counted size (what a naive admission charge would use)...
    const Int64 raw_chunk_bytes = static_cast<Int64>(Chunk(columns, num_rows).allocatedBytes());
    ASSERT_GE(raw_chunk_bytes, 2 * payload_bytes);

    /// ...and a budget below it, with a half-payload margin over the single-counted size: an admission charge
    /// that counts the aliased payload once fits comfortably, one that counts it per reference throws.
    const auto budget = static_cast<size_t>(raw_chunk_bytes - payload_bytes / 2);
    const auto outcome = splitOneChunkUnderBudget(header, std::move(columns), num_shards, ColumnNumbers{0}, budget);

    EXPECT_FALSE(outcome.work_threw_budget_error);
    /// The block parked in the ports is charged for one payload, not one per reference or per shard.
    EXPECT_GE(outcome.buffered_bytes, payload_bytes);
    EXPECT_LT(outcome.buffered_bytes, 2 * payload_bytes);
}

/// A chunk pushed to an output port stays resident in the port state (still consuming memory) until the
/// downstream merge pulls it, so its budget charge must be held until then. Releasing it the moment the chunk
/// leaves the scatter's internal queue (when it is pushed to the port) let a scatter park a full block in each
/// of its ports while the shared counter read far less than the memory held, defeating the budget. This drives
/// a block into the ports and checks the charge is held while parked and while the downstream retains it.
TEST(BufferedShardByHashTransform, PortResidentChunksChargedUntilConsumed)
{
    const size_t num_rows = 4000;
    const size_t num_shards = 8;

    ColumnPtr key = makeDistinctKeyColumn(num_rows);
    ColumnPtr value = makeUInt64ValueColumn(num_rows);

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "v"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    Columns columns{key, value};
    const auto outcome = portResidentThenRetainedThenReleasedBytes(header, std::move(columns), num_shards, ColumnNumbers{0});

    /// Held while the chunks sit in the ports (releasing on dequeue would leave the counter at ~0)...
    EXPECT_GT(outcome.while_parked, 0);
    /// A sorted merge retains pulled chunks until it advances its input, so they remain charged then.
    EXPECT_EQ(outcome.while_retained, outcome.while_parked);
    /// The charge is released only when that downstream owner drops the chunks.
    EXPECT_EQ(outcome.after_released, 0);
}

/// The buffer budget must de-duplicate a physical buffer shared, by pointer, across more than one *buffered
/// block*, not only within one block. `ColumnConst::cloneResized` - used both by `scatter` and by the query
/// engine when it hands out an already-evaluated constant to a later block - keeps the same backing `data`
/// object, so two SEPARATE blocks pulled one after another can reference the identical payload column.
/// Charging each block's registration independently (a fresh de-duplication set per pull, forgotten once the
/// pull is over) would bill the payload once per block that references it; the shared counter must instead
/// charge it exactly once for as long as either block still holds it.
TEST(BufferedShardByHashTransform, SharedPayloadAcrossBufferedBlocksChargedOnce)
{
    const size_t num_shards = 8;
    const size_t first_rows = 4000;
    const size_t second_rows = 4000;
    const size_t value_len = 1 << 20;  /// A 1 MiB constant string payload - dominates both blocks' bytes.

    auto payload = ColumnString::create();
    const std::string big_value(value_len, 'x');
    payload->insertData(big_value.data(), big_value.size());
    const ColumnPtr shared_data = std::move(payload);
    const Int64 payload_bytes = static_cast<Int64>(shared_data->allocatedBytes());
    ASSERT_GT(payload_bytes, static_cast<Int64>(value_len));

    /// Two SEPARATE `ColumnConst` wrappers - mirroring what `cloneResized` produces - both referencing the
    /// identical backing `data` object by pointer.
    ColumnPtr first_constant = ColumnConst::create(shared_data, first_rows);
    ColumnPtr second_constant = ColumnConst::create(shared_data, second_rows);

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "c"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    Columns first_columns{makeDistinctKeyColumn(first_rows), first_constant};
    Columns second_columns{makeDistinctKeyColumn(second_rows), second_constant};
    const Int64 buffered
        = twoBlocksResidentBytes(header, std::move(first_columns), std::move(second_columns), num_shards, ColumnNumbers{0});

    /// Charged once across BOTH blocks: at least one payload (not dropped), and below two payloads (the
    /// regression this guards: charging it once per block that references it would double it).
    EXPECT_GE(buffered, payload_bytes);
    EXPECT_LT(buffered, 2 * payload_bytes);
}

/// The budget must not fail a query for bytes that are no longer resident. A scatter releases the charge of a
/// chunk parked in one of its output ports only when it runs `prepare()` again: the downstream merge's pull just
/// queues an edge update, nothing calls back into the producer. Every scatter of the stage, however, consults the
/// shared counter on every admission decision, so a scatter whose chunks have all been pulled but which the
/// executor has not scheduled since would keep its bytes charged and make a sibling raise
/// TOO_MANY_ROWS_OR_BYTES against memory nobody holds. This drives exactly that interleaving on two scatters
/// sharing one budget under a cap that holds one block but not two: the first buffers its block, its downstream
/// pulls every parked chunk without the first scatter running again, and the second must then be admitted. The
/// control - the same run with the first scatter's chunks left parked - must still throw, so the cap is
/// genuinely tight against the first scatter's bytes and the test cannot pass by being too loose.
TEST(BufferedShardByHashTransform, SiblingNotFailedByAlreadyPulledPortResidentBytes)
{
    const size_t num_shards = 8;
    const size_t num_rows = 4000;

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "v"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    /// Bytes one block leaves resident once it is split and parked in the ports...
    const Int64 resident_after_split = bufferedBytesAfterSplit(
        header, Columns{makeDistinctKeyColumn(num_rows), makeUInt64ValueColumn(num_rows)}, num_shards, ColumnNumbers{0});
    ASSERT_GT(resident_after_split, 0);
    /// ...and the pre-split size of a chunk, which is what admission charges when it is pulled.
    const Int64 chunk_bytes = static_cast<Int64>(
        Chunk(Columns{makeDistinctKeyColumn(num_rows), makeUInt64ValueColumn(num_rows)}, num_rows).allocatedBytes());
    ASSERT_GT(chunk_bytes, 0);

    /// Room for one block (so the first scatter buffers its own without tripping the cap) but not for two: with
    /// the first scatter's bytes still counted, admitting the second crosses the cap.
    const Int64 max_buffered_bytes = resident_after_split + chunk_bytes / 2;
    ASSERT_GE(max_buffered_bytes, chunk_bytes);

    const auto drained = runSecondScatterAfterFirstIsDrained(
        num_rows, num_rows, num_shards, static_cast<size_t>(max_buffered_bytes), /*first_sinks_to_pull=*/ num_shards);
    /// The first scatter buffered its own block without tripping the cap (so the run reaches the interleaving
    /// under test)...
    ASSERT_GT(drained.bytes_while_first_parked, 0);
    ASSERT_LE(drained.bytes_while_first_parked, max_buffered_bytes);
    /// ...and once its chunks are pulled, the sibling must be admitted rather than fail on the stale charge.
    EXPECT_FALSE(drained.second_threw_budget_error);

    /// Control: with the first scatter's chunks still parked, those bytes ARE resident and the sibling throws.
    const auto parked = runSecondScatterAfterFirstIsDrained(
        num_rows, num_rows, num_shards, static_cast<size_t>(max_buffered_bytes), /*first_sinks_to_pull=*/ 0);
    EXPECT_TRUE(parked.second_threw_budget_error);
}

/// The de-duplication table is keyed by raw column address, so it must never keep an entry for a buffer that no
/// longer exists: a later chunk allocated at the recycled address would be taken for that buffer, charged nothing
/// (`chargeColumnAndDescendants` treats an entry it finds as already billed), and the stale cached bytes would
/// stay attached to it - the shared counter is then permanently wrong, letting an over-budget query through or
/// failing a query whose bytes fit. The charges of chunks the downstream has already pulled out of the ports are
/// the ones that can go stale, because their owning scatter only notices the pull when it is scheduled again, so
/// they are reclaimed across the whole stage before any registration, not only before enforcement.
///
/// This runs the interleaving that makes such an entry stale: the first scatter's parked chunks are all pulled and
/// destroyed while it is not scheduled again, and only then is the second scatter's (much smaller) block built and
/// buffered, so its columns can land on the freed addresses. Under a cap with room for everything, the counter
/// must then hold exactly the second block's own resident bytes: neither the first scatter's stale bytes (kept if
/// nothing reclaims them before the registration) nor zero for a second block mistaken for the freed one.
TEST(BufferedShardByHashTransform, ConsumedChunkChargesDoNotAliasLaterBuffers)
{
    const size_t num_shards = 8;
    const size_t first_rows = 4000;
    const size_t second_rows = 250; /// Much smaller, so the expected counter cannot be confused with the first.

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "v"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    const Int64 second_resident = bufferedBytesAfterSplit(
        header, Columns{makeDistinctKeyColumn(second_rows), makeUInt64ValueColumn(second_rows)}, num_shards, ColumnNumbers{0});
    ASSERT_GT(second_resident, 0);

    /// A cap far above anything buffered here: what is under test is what the counter holds, not enforcement.
    const auto drained = runSecondScatterAfterFirstIsDrained(
        first_rows, second_rows, num_shards, static_cast<size_t>(100 * second_resident), /*first_sinks_to_pull=*/ num_shards);
    ASSERT_FALSE(drained.second_threw_budget_error);
    ASSERT_GT(drained.bytes_while_first_parked, 4 * second_resident); /// The first block dominates, as intended.
    EXPECT_EQ(drained.bytes_after_second_buffered, second_resident);
}

/// A charge is released per buffered chunk, not per input block: the shard chunks of one block leave the pipeline
/// one at a time, as their own downstream merges consume them, so holding the whole block's charge until its last
/// shard chunk drained would keep the de-duplication table populated with the exclusive buffers of chunks that
/// were consumed - and freed - long before (see `ConsumedChunkChargesDoNotAliasLaterBuffers` for what a stale
/// entry does), and would bill their bytes all that time. Buffers genuinely shared across the shard chunks stay
/// charged for as long as any of them holds them, because the table refcounts them.
///
/// This buffers one block, has the downstream consume half of the parked chunks, and then makes a sibling scatter
/// buffer a small block, which reclaims the pulled chunks' charges. With per-chunk release the counter has to drop
/// below what the whole block held; with per-block release it would still carry every byte of it.
TEST(BufferedShardByHashTransform, PartiallyConsumedBlockReleasesTheConsumedChunks)
{
    const size_t num_shards = 8;
    const size_t first_rows = 4000;
    const size_t second_rows = 250; /// Small enough that what it adds cannot mask the release of half a block.

    /// An unbounded (but non-zero, or the transform would do no accounting at all) budget: the reclaim under
    /// test must happen when the second scatter REGISTERS its block, not because enforcement kicked in - a
    /// counter this far below the cap never reaches the reclaim inside `isOverBudget`.
    const auto partially_drained = runSecondScatterAfterFirstIsDrained(
        first_rows, second_rows, num_shards, /*max_buffered_bytes=*/ (1ULL << 40), /*first_sinks_to_pull=*/ num_shards / 2);
    ASSERT_GT(partially_drained.bytes_while_first_parked, 0);
    /// Half the block's shard chunks are gone, so their exclusive buffers must no longer be charged...
    EXPECT_LT(partially_drained.bytes_after_second_buffered, partially_drained.bytes_while_first_parked);
    /// ...while the half still parked, and the second scatter's block, are.
    EXPECT_GT(partially_drained.bytes_after_second_buffered, partially_drained.bytes_while_first_parked / 4);
}

/// The buffer budget must de-duplicate a physical buffer shared, by pointer, across SIBLING SCATTERS of the
/// stage, not only within one scatter. A shuffle stage runs one `BufferedShardByHashTransform` per input
/// stream, all sharing one `BufferedShardByHashBudget`; a constant aggregate argument is evaluated once and its
/// backing `data` column handed to every stream (`ColumnConst::cloneResized` keeps the same pointer), so two
/// scatters can buffer the identical payload at the same time. A per-scatter de-duplication table would bill
/// the payload once per scatter that holds it; the shared table in the budget must charge it exactly once for
/// as long as any scatter still holds it.
TEST(BufferedShardByHashTransform, SharedPayloadAcrossSiblingScattersChargedOnce)
{
    const size_t num_shards = 8;
    const size_t first_rows = 4000;
    const size_t second_rows = 4000;
    const size_t value_len = 1 << 20;  /// A 1 MiB constant string payload - dominates both scatters' bytes.

    auto payload = ColumnString::create();
    const std::string big_value(value_len, 'x');
    payload->insertData(big_value.data(), big_value.size());
    const ColumnPtr shared_data = std::move(payload);
    const Int64 payload_bytes = static_cast<Int64>(shared_data->allocatedBytes());
    ASSERT_GT(payload_bytes, static_cast<Int64>(value_len));

    /// Two `ColumnConst` wrappers - as `cloneResized` produces - both referencing the identical backing `data`.
    ColumnPtr first_constant = ColumnConst::create(shared_data, first_rows);
    ColumnPtr second_constant = ColumnConst::create(shared_data, second_rows);

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "c"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    Columns first_columns{makeDistinctKeyColumn(first_rows), first_constant};
    Columns second_columns{makeDistinctKeyColumn(second_rows), second_constant};
    const Int64 buffered = twoScattersSharingBudgetResidentBytes(
        header, std::move(first_columns), std::move(second_columns), num_shards, ColumnNumbers{0});

    /// Charged once across BOTH scatters: at least one payload (not dropped), and below two payloads (the
    /// regression this guards: a per-scatter table would charge it once per scatter that holds it).
    EXPECT_GE(buffered, payload_bytes);
    EXPECT_LT(buffered, 2 * payload_bytes);
}

namespace
{

/// A one-row `Array(String)` holding `value_len` bytes of string data - a composite payload whose bytes live in
/// the nested string column, not in the array node itself.
ColumnPtr makeBigArrayOfStringColumn(size_t value_len)
{
    auto strings = ColumnString::create();
    const std::string big_value(value_len, 'x');
    strings->insertData(big_value.data(), big_value.size());

    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insertValue(strings->size());

    return ColumnArray::create(std::move(strings), std::move(offsets));
}

}

/// A shared payload must stay charged with its WHOLE subtree for as long as any buffered chunk holds it, so the
/// de-duplication walk has to descend through a column it has already registered, adding this chunk's reference
/// to each descendant, instead of stopping at the shared node.
///
/// `ColumnConst::scatter` hands every shard chunk a fresh wrapper around the identical backing `data` object, and
/// for a composite payload the bytes are in that object's children (`ColumnArray` exposes its offsets and nested
/// data through `forEachSubcolumn`), not in the node the shards share. If only the shared node's refcount is
/// bumped on the second and later visits, its children end up referenced by the FIRST shard chunk alone: as soon
/// as that one chunk is consumed, their bytes are released and their table entries erased - although every other
/// shard chunk still keeps the very same buffers alive through the shared payload. The counter then reports far
/// less than is resident, and `aggregation_in_order_shuffle_max_buffered_bytes` admits read-ahead well past the
/// cap on a query with a composite constant argument (`ColumnConst(Array(String))`,
/// `ColumnConst(Tuple(LowCardinality(String)))`, ...).
///
/// This buffers one such block, has the downstream consume half of the parked shard chunks, and makes a sibling
/// scatter register a small block, which reclaims the consumed chunks' charges. The payload is held by the shard
/// chunks still parked, so it must still be charged in full afterwards.
TEST(BufferedShardByHashTransform, CompositeConstPayloadStaysChargedWhileAnyShardChunkHoldsIt)
{
    const size_t num_shards = 8;
    const size_t num_rows = 8000;
    const size_t sibling_rows = 250; /// Small enough that what it adds cannot mask a lost payload.
    const size_t value_len = 1 << 20;  /// A 1 MiB payload - dominates everything else buffered here.

    ColumnPtr payload = makeBigArrayOfStringColumn(value_len);
    const Int64 payload_bytes = static_cast<Int64>(payload->allocatedBytes());
    ASSERT_GT(payload_bytes, static_cast<Int64>(value_len));

    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(array_type, "c"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    Block sibling_header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "v"),
    };
    auto sibling_header = std::make_shared<const Block>(std::move(sibling_header_block));

    /// A cap far above anything buffered here: what is under test is what the counter holds, not enforcement.
    auto budget = std::make_shared<BufferedShardByHashBudget>();
    const size_t max_buffered_bytes = 1ULL << 40;
    auto scatter = makeConnectedScatter(header, num_shards, ColumnNumbers{0}, max_buffered_bytes, budget);
    auto sibling = makeConnectedScatter(sibling_header, num_shards, ColumnNumbers{0}, max_buffered_bytes, budget);

    ColumnPtr constant = ColumnConst::create(payload, num_rows);
    scatter.source->push(Chunk(Columns{makeDistinctKeyColumn(num_rows), constant}, num_rows));
    drive(scatter);

    const Int64 bytes_while_all_parked = budget->total_buffered_bytes.load();
    ASSERT_GE(bytes_while_all_parked, payload_bytes);

    /// The downstream merges consume half of the parked shard chunks, which drops half of the references to the
    /// shared payload - but not all of them.
    size_t pulled = 0;
    for (auto & sink : scatter.sinks)
    {
        if (pulled == num_shards / 2)
            break;
        if (sink->hasData())
        {
            sink->pull();
            ++pulled;
        }
    }
    ASSERT_EQ(pulled, num_shards / 2);

    /// A sibling registering its own block reclaims the consumed chunks' charges across the whole stage.
    sibling.source->push(
        Chunk(Columns{makeDistinctKeyColumn(sibling_rows), makeUInt64ValueColumn(sibling_rows)}, sibling_rows));
    ASSERT_FALSE(drive(sibling));

    /// The payload is still held by the shard chunks parked in the other half of the ports, so all of it must
    /// still be charged. Dropping the shared node's children with the first chunk released would leave the
    /// counter with little more than the sibling's small block.
    EXPECT_GE(budget->total_buffered_bytes.load(), payload_bytes);
    /// ...and it is still charged exactly once, not once per chunk that reached it.
    EXPECT_LT(budget->total_buffered_bytes.load(), 2 * payload_bytes);
}

namespace
{

struct NoBudgetOutcome
{
    /// Peaks, sampled after every cycle: the final state alone would not tell accounting from no accounting,
    /// since a charge is released as soon as its chunk leaves and the counter returns to zero either way.
    Int64 peak_buffered_bytes = 0;   /// Highest value the shared counter ever took.
    size_t peak_registered_objects = 0; /// Most entries the shared de-duplication table ever held.
    size_t registered_scatters = 0;  /// Scatters registered in the stage list.
    size_t rows_pushed = 0;          /// Rows the transform actually handed downstream.
    bool finished = false;           /// The transform reached Status::Finished after the input was closed.
};

/// Drive one block through a transform that has NO byte budget to enforce (`max_buffered_bytes == 0`, the cap
/// explicitly disabled) and report what it did to the shared budget state, how many rows it shuffled, and
/// whether it terminated.
NoBudgetOutcome runWithoutByteBudget(
    const SharedHeader & header, Columns columns, size_t num_shards, const ColumnNumbers & key_columns)
{
    const size_t num_rows = columns.at(0)->size();
    auto budget = std::make_shared<BufferedShardByHashBudget>();

    BufferedShardByHashTransform transform(
        header, num_shards, key_columns, /*max_buffered_bytes_=*/ 0, budget);

    OutputPort source_output(header);
    connect(source_output, transform.getInputs().front());

    std::vector<std::unique_ptr<InputPort>> sinks;
    for (auto & output : transform.getOutputs())
    {
        auto sink = std::make_unique<InputPort>(header);
        connect(output, *sink);
        sinks.push_back(std::move(sink));
    }

    source_output.push(Chunk(std::move(columns), num_rows));
    /// This is the whole input: the transform still pulls the pushed chunk (an input port with data is not
    /// finished yet), and afterwards runs its EOF drain path - which the port-residency bookkeeping gates, so
    /// skipping that bookkeeping must not leave an output open forever.
    source_output.finish();

    NoBudgetOutcome outcome;

    for (int step = 0; step < 64; ++step)
    {
        for (auto & sink : sinks)
        {
            sink->setNeeded();
            if (sink->hasData())
                outcome.rows_pushed += sink->pull().getNumRows();
        }

        const auto status = transform.prepare();
        if (status == IProcessor::Status::Finished)
        {
            outcome.finished = true;
            break;
        }

        if (status == IProcessor::Status::Ready)
            transform.work();

        outcome.peak_buffered_bytes = std::max(outcome.peak_buffered_bytes, budget->total_buffered_bytes.load());
        outcome.peak_registered_objects = std::max(outcome.peak_registered_objects, budget->shared_object_refcounts.size());
    }

    outcome.registered_scatters = budget->scatters.size();
    return outcome;
}

}

/// The ownership accounting the buffer budget needs - a recursive walk of every pulled block and of every
/// scattered column, plus per-chunk charge vectors and a shared hash table - must not run when there is no byte
/// cap to enforce, i.e. with `aggregation_in_order_shuffle_max_buffered_bytes = 0` (the cap explicitly
/// disabled): nothing there ever reads `total_buffered_bytes`, so paying two ownership walks and hash-table
/// churn per input block would be a pure regression. Skipping the accounting must not change what the transform shuffles, nor keep it from finishing (the drain
/// at EOF is gated on the port-residency bookkeeping the accounting maintains).
TEST(BufferedShardByHashTransform, NoOwnershipAccountingWithoutAByteBudget)
{
    const size_t num_shards = 8;
    const size_t num_rows = 4000;

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "lc"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    /// A `LowCardinality` payload: the one column whose accounting is most elaborate (a shared dictionary
    /// registered at any nesting depth), so any leftover charging would show up here.
    Columns columns{makeDistinctKeyColumn(num_rows), makeBigDictLowCardinalityColumn(num_rows, 500, 64)};
    const auto outcome = runWithoutByteBudget(header, std::move(columns), num_shards, ColumnNumbers{0});

    EXPECT_EQ(outcome.peak_buffered_bytes, 0);
    EXPECT_EQ(outcome.peak_registered_objects, 0u);
    EXPECT_EQ(outcome.registered_scatters, 0u);
    /// The shuffle itself is unaffected: every row is handed downstream, and the transform terminates.
    EXPECT_EQ(outcome.rows_pushed, num_rows);
    EXPECT_TRUE(outcome.finished);
}

/// A downstream `LimitTransform` closes every input it holds the moment it reaches its limit, and so does a
/// cancellation. When that lands after work() passed its `allOutputsFinished()` carve-out - so the split runs
/// with only part of the outputs closed - `generateOutputChunks` skips the finished shards. It must not keep
/// the per-shard copies it already materialized for them: nobody will ever consume those, and the pre-split
/// charge that covered them is released at the end of the split, so they would sit resident while
/// `total_buffered_bytes` no longer counts them - a query that already reached its outer `LIMIT` could hit
/// `max_memory_usage` on data nobody will read.
TEST(BufferedShardByHashTransform, ShardCopiesForFinishedOutputsAreFreed)
{
    const size_t num_rows = 4000;
    const size_t num_shards = 4;
    const size_t value_len = 1 << 20;  /// A 1 MiB constant payload - the bytes a leaked shard copy would hold.

    ColumnPtr key = makeDistinctKeyColumn(num_rows);

    auto payload = ColumnString::create();
    const std::string big_value(value_len, 'x');
    payload->insertData(big_value.data(), big_value.size());
    ColumnPtr constant = ColumnConst::create(std::move(payload), num_rows);
    /// `ColumnConst::scatter` hands every per-shard copy a reference to the same payload, so the payload's use
    /// count says exactly how many of those copies are still alive.
    ColumnPtr shared_payload = assert_cast<const ColumnConst &>(*constant).getDataColumnPtr();

    Block header_block{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k"),
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "c"),
    };
    auto header = std::make_shared<const Block>(std::move(header_block));

    auto budget = std::make_shared<BufferedShardByHashBudget>();
    /// Unbounded budget: this test observes residency, it must not throw.
    BufferedShardByHashTransform transform(
        header, num_shards, ColumnNumbers{0}, /*max_buffered_bytes_=*/ (1ULL << 40), budget);

    OutputPort source_output(header);
    connect(source_output, transform.getInputs().front());

    std::vector<std::unique_ptr<InputPort>> sinks;
    for (auto & output : transform.getOutputs())
    {
        auto sink = std::make_unique<InputPort>(header);
        connect(output, *sink);
        sinks.push_back(std::move(sink));
    }

    source_output.push(Chunk(Columns{key, constant}, num_rows));

    /// Only the first shard keeps a live consumer; the others are closed, as a `LimitTransform` closes them.
    sinks.front()->setNeeded();
    for (size_t shard = 1; shard < num_shards; ++shard)
        sinks[shard]->close();

    /// This test's own references: the local `ColumnPtr` and the `ColumnConst` holding the payload.
    const auto payload_uses_before = shared_payload->use_count();

    for (int step = 0; step < 8; ++step)
    {
        if (transform.prepare() != IProcessor::Status::Ready)
            break;
        transform.work();
    }

    /// Exactly one copy survives the split: the chunk parked in the one port that still has a consumer. The
    /// copies built for the three closed outputs are gone (keeping them would read `num_shards` copies).
    EXPECT_EQ(shared_payload->use_count(), payload_uses_before + 1);
}
