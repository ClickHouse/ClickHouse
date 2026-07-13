#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include <Columns/ColumnLowCardinality.h>
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
#include <Common/assert_cast.h>

using namespace DB;

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

/// Feed one input block (keyed by `key_columns`) to a `BufferedShardByHashTransform` with an unbounded
/// budget, drive a single pull-and-split cycle, and return the shared buffered-bytes counter with the
/// block's shard chunks still resident (parked in the output ports, none pulled downstream). This is the
/// exact quantity `aggregation_in_order_shuffle_max_buffered_bytes` is compared against, observed with no
/// dependence on pipeline scheduling.
Int64 bufferedBytesAfterSplit(const SharedHeader & header, Columns columns, size_t num_shards, const ColumnNumbers & key_columns)
{
    const size_t num_rows = columns.at(0)->size();
    auto counter = std::make_shared<std::atomic<Int64>>(0);

    /// Unbounded budget: this test measures the counter, it must not throw.
    BufferedShardByHashTransform transform(
        header, num_shards, key_columns, /*max_queue_length_=*/ 0, /*max_buffered_bytes_=*/ (1ULL << 40), counter);

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

    return counter->load();
}

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
