#include <gtest/gtest.h>

#include <bit>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/SortingTransform.h>

using namespace DB;

namespace
{

SharedHeader makeHeader(const DataTypePtr & key_type)
{
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(key_type, "key"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "payload")});
}

Chunk makeChunk(const Block & header, const Array & keys, UInt64 first_payload)
{
    auto columns = header.cloneEmptyColumns();
    for (const auto & key : keys)
    {
        columns[0]->insert(key);
        columns[1]->insert(first_payload++);
    }
    return Chunk(std::move(columns), keys.size());
}

std::vector<UInt64> readPayloads(MergeSorter & sorter, size_t block_size)
{
    std::vector<UInt64> result;
    while (auto chunk = sorter.read())
    {
        EXPECT_LE(chunk.getNumRows(), block_size);
        for (size_t row = 0; row < chunk.getNumRows(); ++row)
            result.push_back(chunk.getColumns().back()->getUInt(row));
    }
    EXPECT_FALSE(sorter.read());
    return result;
}

SortDescription ascending()
{
    SortDescription description;
    description.emplace_back("key", 1, 1);
    return description;
}

}

TEST(MergeSorter, UniqueChunksKeepFirstPayloadAcrossBatches)
{
    const auto numbers = std::make_shared<DataTypeUInt64>();
    const std::vector<std::pair<DataTypePtr, Array>> cases{
        {numbers, {0u, 1u, 2u, 3u, 4u}},
        {std::make_shared<DataTypeNullable>(numbers), {0u, 1u, 2u, 3u, Null{}}},
        {std::make_shared<DataTypeString>(), {"a", "b", "c", "d", "e"}},
        {std::make_shared<DataTypeArray>(numbers), {Array{0u}, Array{1u}, Array{2u}, Array{3u}, Array{4u}}}};

    for (const auto & [type, keys] : cases)
    {
        const auto header = makeHeader(type);
        for (const size_t block_size : {1, 2, 4, 64})
        {
            for (const UInt64 limit : {0, 1, 3, 5, 8})
            {
                SCOPED_TRACE(type->getName() + ", block_size=" + std::to_string(block_size) + ", limit=" + std::to_string(limit));
                Chunks chunks;
                chunks.push_back(makeChunk(*header, {keys[0], keys[2], keys[4]}, 100));
                chunks.push_back(makeChunk(*header, {}, 0));
                chunks.push_back(makeChunk(*header, keys, 200));
                chunks.push_back(makeChunk(*header, {keys[2], keys[4]}, 300));
                MergeSorter sorter(header, std::move(chunks), ascending(), block_size, limit, MergeSorter::Mode::MergeUniqueChunks);
                std::vector<UInt64> expected{100, 201, 101, 203, 102};
                if (limit && limit < expected.size())
                    expected.resize(limit);
                EXPECT_EQ(readPayloads(sorter, block_size), expected);
            }
        }
    }
}

TEST(MergeSorter, DuplicateBatchPrefixKeepsRemainingRange)
{
    const auto header = makeHeader(std::make_shared<DataTypeUInt64>());
    for (const size_t block_size : {2, 3, 64})
    {
        Chunks chunks;
        chunks.push_back(makeChunk(*header, {1u, 5u}, 100));
        chunks.push_back(makeChunk(*header, {1u, 2u, 3u, 4u, 5u, 6u, 7u}, 200));
        MergeSorter sorter(header, std::move(chunks), ascending(), block_size, 0, MergeSorter::Mode::MergeUniqueChunks);
        EXPECT_EQ(readPayloads(sorter, block_size), (std::vector<UInt64>{100, 201, 202, 203, 101, 205, 206}));
    }
}

TEST(MergeSorter, DuplicateOnlyReadsPreserveProgress)
{
    const auto header = makeHeader(std::make_shared<DataTypeUInt64>());
    Chunks chunks;
    for (size_t index = 0; index < 9; ++index)
        chunks.push_back(makeChunk(*header, {1u}, index));
    chunks.push_back(makeChunk(*header, {2u}, 9));
    MergeSorter sorter(header, std::move(chunks), ascending(), 2, 0, MergeSorter::Mode::MergeUniqueChunks);

    auto first = sorter.read();
    ASSERT_EQ(first.getNumRows(), 1);
    EXPECT_EQ(first.getColumns()[1]->getUInt(0), 0);
    for (size_t index = 0; index < 3; ++index)
    {
        auto progress = sorter.read();
        EXPECT_TRUE(progress);
        EXPECT_EQ(progress.getNumRows(), 0);
        EXPECT_EQ(progress.getNumColumns(), 2);
    }
    auto last = sorter.read();
    ASSERT_EQ(last.getNumRows(), 1);
    EXPECT_EQ(last.getColumns()[1]->getUInt(0), 9);
    EXPECT_FALSE(sorter.read());
}

TEST(MergeSorter, ExhaustedDuplicateTailFinishes)
{
    const auto header = makeHeader(std::make_shared<DataTypeUInt64>());
    Chunks chunks;
    chunks.push_back(makeChunk(*header, {1u}, 100));
    chunks.push_back(makeChunk(*header, {1u}, 200));
    MergeSorter sorter(header, std::move(chunks), ascending(), 1, 0, MergeSorter::Mode::MergeUniqueChunks);
    EXPECT_EQ(readPayloads(sorter, 1), (std::vector<UInt64>{100}));
}

TEST(MergeSorter, CompositeKeysAndDescendingOrder)
{
    const auto header = makeHeader(std::make_shared<DataTypeUInt64>());
    SortDescription description;
    description.emplace_back("key", 1, 1);
    description.emplace_back("payload", -1, -1);
    Chunks chunks;
    chunks.push_back(makeChunk(*header, {1u, 2u}, 100));
    chunks.push_back(makeChunk(*header, {1u, 2u}, 200));
    chunks.push_back(makeChunk(*header, {1u, 2u}, 100));
    MergeSorter sorter(header, std::move(chunks), description, 2, 0, MergeSorter::Mode::MergeUniqueChunks);
    EXPECT_EQ(readPayloads(sorter, 2), (std::vector<UInt64>{200, 100, 201, 101}));
}

TEST(MergeSorter, CollationKeepsFirstRepresentation)
{
    const auto header = makeHeader(std::make_shared<DataTypeString>());
    SortDescription description;
    description.emplace_back("key", 1, 1, std::make_shared<Collator>("en-u-ks-level2"));
    Chunks chunks;
    chunks.push_back(makeChunk(*header, {"a", "C"}, 100));
    chunks.push_back(makeChunk(*header, {"A", "b", "c"}, 200));
    MergeSorter sorter(header, std::move(chunks), description, 2, 0, MergeSorter::Mode::MergeUniqueChunks);
    std::vector<String> keys;
    std::vector<UInt64> payloads;
    while (auto chunk = sorter.read())
    {
        for (size_t row = 0; row < chunk.getNumRows(); ++row)
        {
            keys.emplace_back(chunk.getColumns()[0]->getDataAt(row));
            payloads.push_back(chunk.getColumns()[1]->getUInt(row));
        }
    }
    EXPECT_EQ(keys, (std::vector<String>{"a", "b", "C"}));
    EXPECT_EQ(payloads, (std::vector<UInt64>{100, 201, 101}));
}

TEST(MergeSorter, SortEqualFloatingPointKeysKeepFirstBits)
{
    const auto header = makeHeader(std::make_shared<DataTypeFloat64>());
    const auto nan = std::numeric_limits<Float64>::quiet_NaN();
    const auto other_nan = std::bit_cast<Float64>(std::bit_cast<UInt64>(nan) ^ 1);
    Chunks chunks;
    chunks.push_back(makeChunk(*header, {-0., nan}, 100));
    chunks.push_back(makeChunk(*header, {0., other_nan}, 200));
    MergeSorter sorter(header, std::move(chunks), ascending(), 1, 0, MergeSorter::Mode::MergeUniqueChunks);
    std::vector<UInt64> bits;
    while (auto chunk = sorter.read())
        for (size_t row = 0; row < chunk.getNumRows(); ++row)
            bits.push_back(std::bit_cast<UInt64>(chunk.getColumns()[0]->getFloat64(row)));
    EXPECT_EQ(bits, (std::vector<UInt64>{std::bit_cast<UInt64>(-0.), std::bit_cast<UInt64>(nan)}));
}

TEST(MergeSorter, EmptyAndSingleChunkRespectBounds)
{
    const auto header = makeHeader(std::make_shared<DataTypeUInt64>());
    for (const auto mode : {MergeSorter::Mode::PreserveRows, MergeSorter::Mode::MergeUniqueChunks})
    {
        MergeSorter empty(header, {}, ascending(), 2, 0, mode);
        EXPECT_FALSE(empty.read());
        for (const size_t block_size : {1, 2, 64})
        {
            for (const UInt64 limit : {0, 1, 3, 5})
            {
                SCOPED_TRACE(::testing::Message() << "mode=" << static_cast<int>(mode) << ", block_size=" << block_size << ", limit=" << limit);
                Chunks chunks;
                chunks.push_back(makeChunk(*header, {}, 0));
                chunks.push_back(makeChunk(*header, {1u, 2u, 3u}, 100));
                MergeSorter sorter(header, std::move(chunks), ascending(), block_size, limit, mode);
                std::vector<UInt64> expected{100, 101, 102};
                if (limit && limit < expected.size())
                    expected.resize(limit);
                EXPECT_EQ(readPayloads(sorter, block_size), expected);
            }
        }
    }
}

TEST(MergeSorter, PreserveRowsKeepsDuplicates)
{
    const auto header = makeHeader(std::make_shared<DataTypeUInt64>());
    for (const UInt64 limit : {0, 1, 3, 5, 8})
    {
        Chunks chunks;
        chunks.push_back(makeChunk(*header, {1u, 1u, 2u}, 100));
        chunks.push_back(makeChunk(*header, {1u, 2u}, 200));
        MergeSorter sorter(header, std::move(chunks), ascending(), 2, limit);
        std::vector<UInt64> expected{100, 101, 200, 102, 201};
        if (limit && limit < expected.size())
            expected.resize(limit);
        EXPECT_EQ(readPayloads(sorter, 2), expected);
    }
}

TEST(MergeSorter, ConstantSparseAndReplicatedColumns)
{
    const auto header = makeHeader(std::make_shared<DataTypeUInt64>());
    for (const auto mode : {MergeSorter::Mode::PreserveRows, MergeSorter::Mode::MergeUniqueChunks})
    {
        Chunks chunks;
        chunks.emplace_back(Columns{
            ColumnConst::create(ColumnUInt64::create(1, 1), 1),
            ColumnConst::create(ColumnUInt64::create(1, 100), 1)}, 1);
        auto sparse = ColumnSparse::create(ColumnUInt64::create());
        sparse->insert(2u);
        sparse->insert(3u);
        auto payloads = ColumnUInt64::create();
        payloads->insertValue(200);
        payloads->insertValue(201);
        chunks.emplace_back(Columns{std::move(sparse), ColumnReplicated::create(ColumnPtr(std::move(payloads)))}, 2);
        auto replicated = makeChunk(*header, {1u, 4u}, 300);
        auto columns = replicated.detachColumns();
        columns[0] = ColumnReplicated::create(columns[0]);
        chunks.emplace_back(std::move(columns), 2);
        MergeSorter sorter(header, std::move(chunks), ascending(), 2, 0, mode);
        const std::vector<UInt64> expected = mode == MergeSorter::Mode::PreserveRows
            ? std::vector<UInt64>{100, 300, 200, 201, 301} : std::vector<UInt64>{100, 200, 201, 301};
        EXPECT_EQ(readPayloads(sorter, 2), expected);
    }
}
