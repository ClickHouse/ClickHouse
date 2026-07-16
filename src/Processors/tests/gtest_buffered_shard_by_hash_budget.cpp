#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Processors/Port.h>
#include <Processors/Transforms/BufferedShardByHashTransform.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

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
        header, num_shards, key_columns, /*max_queue_length_=*/ 0, /*max_buffered_bytes_=*/ (1ULL << 40), budget);

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

/// Drive one pull-and-split cycle, read the shared counter while the shard chunks are still parked in the
/// output ports (pushed but not yet pulled downstream), then pull every parked chunk and drive one more
/// prepare() so the transform reclaims them. Returns {bytes while parked, bytes after they were consumed}.
std::pair<Int64, Int64> portResidentThenConsumedBytes(
    const SharedHeader & header, Columns columns, size_t num_shards, const ColumnNumbers & key_columns)
{
    const size_t num_rows = columns.at(0)->size();
    auto budget = std::make_shared<BufferedShardByHashBudget>();

    BufferedShardByHashTransform transform(
        header, num_shards, key_columns, /*max_queue_length_=*/ 0, /*max_buffered_bytes_=*/ (1ULL << 40), budget);

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

    /// The downstream merge pulls every chunk out of the ports; the next prepare() must reclaim their charges.
    for (auto & sink : sinks)
        if (sink->hasData())
            sink->pull();
    transform.prepare();
    const Int64 after_consumed = budget->total_buffered_bytes.load();

    return {while_parked, after_consumed};
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

    /// Demand-driven mode (max_queue_length == 0) is the only mode that enforces max_buffered_bytes.
    BufferedShardByHashTransform transform(
        header, num_shards, key_columns, /*max_queue_length_=*/ 0, max_buffered_bytes, budget);

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

    /// Demand-driven mode (max_queue_length == 0) is the only mode that enforces max_buffered_bytes.
    BufferedShardByHashTransform transform(
        header, num_shards, ColumnNumbers{0}, /*max_queue_length_=*/ 0, max_buffered_bytes, budget);

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
        header, num_shards, key_columns, /*max_queue_length_=*/ 0, /*max_buffered_bytes_=*/ (1ULL << 40), budget);

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
            header, num_shards, key_columns, /*max_queue_length_=*/ 0, /*max_buffered_bytes_=*/ (1ULL << 40), budget);

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
/// a block into the ports and checks the charge is held while parked, then released once the downstream pulls.
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
    const auto [while_parked, after_consumed]
        = portResidentThenConsumedBytes(header, std::move(columns), num_shards, ColumnNumbers{0});

    /// Held while the chunks sit in the ports (releasing on dequeue would leave the counter at ~0)...
    EXPECT_GT(while_parked, 0);
    /// ...and fully released once the downstream pulls them out of the ports (no leftover charge).
    EXPECT_EQ(after_consumed, 0);
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
