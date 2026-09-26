#include <gtest/gtest.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/SortCursor.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/FinishSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/SortingTransform.h>
#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

namespace
{

/// A dense UInt16 column with a single repeated value.
ColumnPtr denseUInt16(UInt16 value, size_t n)
{
    auto col = ColumnUInt16::create();
    col->getData().assign(n, value);
    return col;
}

/// A ColumnSparse over UInt16 where every row holds `value` (fully default when value == 0,
/// but the representation is ColumnSparse regardless).
ColumnPtr sparseUInt16(UInt16 value, size_t n)
{
    auto values = ColumnUInt16::create();
    auto offsets = ColumnUInt64::create();
    /// values[0] is the default; extra values live at the given offsets.
    values->getData().push_back(static_cast<UInt16>(0));
    if (value != 0)
    {
        for (size_t i = 0; i < n; ++i)
        {
            values->getData().push_back(value);
            offsets->getData().push_back(i);
        }
    }
    return ColumnSparse::create(std::move(values), std::move(offsets), n);
}

/// A Replicated over a dense UInt16 where every row holds `value` (single stored row, replicated
/// n times by the indexes). `convertToFullColumnIfReplicated` expands it to a dense column.
ColumnPtr replicatedUInt16(UInt16 value, size_t n)
{
    ColumnPtr nested = denseUInt16(value, 1);
    auto indexes = ColumnUInt8::create();
    indexes->getData().assign(n, static_cast<UInt8>(0));
    return ColumnReplicated::create(nested, std::move(indexes));
}

/// A single-element `Tuple(UInt16)` wrapping `element`. `FunctionTuple` builds tuples without
/// materializing replicated/sparse children, so a tuple sort key can carry a replicated child.
ColumnPtr tupleOf(ColumnPtr element)
{
    return ColumnTuple::create(Columns{std::move(element)});
}

/// A non-null `Nullable(Tuple(UInt16))` wrapping a single-element tuple built from `element`.
/// `Tuple` (unlike `Sparse`) can be inside `Nullable`, and a tuple can carry sparse/replicated
/// children, so `Nullable(Tuple(Sparse))` / `Nullable(Tuple(Replicated))` are constructible sort
/// keys whose sparse/replicated column sits two wrappers deep.
ColumnPtr nullableTupleOf(ColumnPtr element, size_t n)
{
    auto null_map = ColumnUInt8::create();
    null_map->getData().assign(n, static_cast<UInt8>(0));
    return ColumnNullable::create(tupleOf(std::move(element)), std::move(null_map));
}

/// The `y` payload values of every output block, in output order.
std::vector<UInt64> collectPayload(const Block & block)
{
    std::vector<UInt64> result;
    if (!block.has("y"))
        return result;
    const auto & column = *block.getByName("y").column;
    result.reserve(column.size());
    for (size_t i = 0; i < column.size(); ++i)
        result.push_back(column.getUInt(i));
    return result;
}

/// Sorting two n-row inputs by (`x`, `y`) where every `x` is equal and each input carries
/// `y` = 0..n-1 puts the two copies of each `y` next to each other: 0, 0, 1, 1, ... n-1, n-1.
std::vector<UInt64> payloadOfBothInputsPaired(size_t n)
{
    std::vector<UInt64> expected;
    expected.reserve(2 * n);
    for (size_t i = 0; i < n; ++i)
    {
        expected.push_back(i);
        expected.push_back(i);
    }
    return expected;
}

ColumnPtr denseUInt64Iota(size_t n)
{
    auto col = ColumnUInt64::create();
    auto & data = col->getData();
    data.resize(n);
    for (size_t i = 0; i < n; ++i)
        data[i] = i;
    return col;
}

/// Feed two chunks into FinishSortingTransform whose already-sorted prefix `x` holds the SAME key
/// value, so consume runs its cross-chunk binary-search `less`. The stored chunk uses `first_prefix`,
/// the current one `second_prefix`. Returns the `y` payload in output order: `compareAt` checks its
/// operand type only in debug and sanitizer builds, so the sort result pins the compare in a release
/// build too.
std::vector<UInt64> runFinishSorting(DataTypePtr key_type, ColumnPtr first_prefix, ColumnPtr second_prefix, size_t n)
{
    auto type_u64 = std::make_shared<DataTypeUInt64>();

    Block header({
        ColumnWithTypeAndName(key_type->createColumn(), key_type, "x"),
        ColumnWithTypeAndName(type_u64->createColumn(), type_u64, "y"),
    });
    auto shared_header = std::make_shared<const Block>(header);

    /// First chunk: prefix `x` is a DENSE column (all the same key value), sorted.
    Chunk chunk_first;
    {
        Columns cols;
        cols.push_back(std::move(first_prefix));
        cols.push_back(denseUInt64Iota(n));
        chunk_first.setColumns(std::move(cols), n);
    }

    /// Second chunk: prefix `x` holds the SAME key value as `first_prefix`.
    /// The equal prefix forces the cross-chunk `less` comparison in consume.
    Chunk chunk_second;
    {
        Columns cols;
        cols.push_back(std::move(second_prefix));
        cols.push_back(denseUInt64Iota(n));
        chunk_second.setColumns(std::move(cols), n);
    }

    Chunks chunks;
    chunks.emplace_back(std::move(chunk_first));
    chunks.emplace_back(std::move(chunk_second));

    SortDescription description_sorted;    // already sorted by prefix `x`
    description_sorted.emplace_back("x");
    SortDescription description_to_sort;    // finish sorting by (`x`, `y`)
    description_to_sort.emplace_back("x");
    description_to_sort.emplace_back("y");

    auto source = std::make_shared<SourceFromChunks>(shared_header, std::move(chunks));
    auto finish_sorting = std::make_shared<FinishSortingTransform>(
        shared_header, description_sorted, description_to_sort, /*max_merged_block_size=*/8192, /*limit=*/0, false);

    connect(source->getPort(), finish_sorting->getInputs().front());
    auto * output_port = &finish_sorting->getOutputs().front();

    auto processors = std::make_shared<Processors>();
    processors->push_back(source);
    processors->push_back(finish_sorting);

    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);

    PullingPipelineExecutor executor(pipeline);
    std::vector<UInt64> payload;
    Block block;
    while (executor.pull(block))
    {
        auto block_payload = collectPayload(block);
        payload.insert(payload.end(), block_payload.begin(), block_payload.end());
    }
    return payload;
}

/// Shorthand: a plain `UInt16` sort key, dense in the stored chunk and `second_prefix` in the next.
std::vector<UInt64> runFinishSortingUInt16(ColumnPtr second_prefix, size_t n)
{
    return runFinishSorting(std::make_shared<DataTypeUInt16>(), denseUInt16(0, n), std::move(second_prefix), n);
}

/// Drive `MergeSorter` (the merge-sort path behind `MergeSortingTransform`) with two already-sorted
/// chunks whose sort key `x` holds the SAME value, so building and draining the merging queue
/// exercises the cross-cursor `compareAt`.
std::vector<UInt64> runMergeSorter(DataTypePtr key_type, ColumnPtr first_key, ColumnPtr second_key, size_t n)
{
    auto type_u64 = std::make_shared<DataTypeUInt64>();

    Block header({
        ColumnWithTypeAndName(key_type->createColumn(), key_type, "x"),
        ColumnWithTypeAndName(type_u64->createColumn(), type_u64, "y"),
    });
    auto shared_header = std::make_shared<const Block>(header);

    Chunk chunk_first;
    {
        Columns cols;
        cols.push_back(std::move(first_key));
        cols.push_back(denseUInt64Iota(n));
        chunk_first.setColumns(std::move(cols), n);
    }
    Chunk chunk_second;
    {
        Columns cols;
        cols.push_back(std::move(second_key));
        cols.push_back(denseUInt64Iota(n));
        chunk_second.setColumns(std::move(cols), n);
    }

    Chunks chunks;
    chunks.emplace_back(std::move(chunk_first));
    chunks.emplace_back(std::move(chunk_second));

    SortDescription description;
    description.emplace_back("x");
    description.emplace_back("y");

    MergeSorter merge_sorter(shared_header, std::move(chunks), description, /*max_merged_block_size=*/8192, /*limit=*/0);
    std::vector<UInt64> payload;
    while (Chunk chunk = merge_sorter.read())
    {
        auto block_payload = collectPayload(shared_header->cloneWithColumns(chunk.detachColumns()));
        payload.insert(payload.end(), block_payload.begin(), block_payload.end());
    }
    return payload;
}

}

/// Regression test for STID 1499-2393 (`Bad cast from type DB::ColumnSparse to
/// DB::ColumnVector<unsigned short>`): a dense sort key in the stored chunk against the same key as
/// `ColumnSparse` in the next. Captured by the AST fuzzer on 02149_read_in_order_fixed_prefix over
/// amd_msan, whose read-in-order prefix `toStartOfMonth(date)` is a Date == UInt16. Pinned here
/// because every SQL plan reaching this compare densifies both chunks symmetrically.
TEST(FinishSortingTransform, SparseSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    std::vector<UInt64> payload;
    ASSERT_NO_THROW(payload = runFinishSortingUInt16(sparseUInt16(0, n), n));
    EXPECT_EQ(payload, payloadOfBothInputsPaired(n));
}

/// `Nullable` can hide a `ColumnTuple`, and a tuple keeps sparse/replicated children, so
/// `Nullable(Tuple(Sparse(UInt16)))` is a constructible sort key whose wrapped column sits two levels
/// down, reached by `ColumnNullable::compareAt` delegating to `ColumnTuple::compareAt`. A top-level
/// strip does not see it.
TEST(FinishSortingTransform, NullableTupleSparseChildSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    auto key_type = std::make_shared<DataTypeNullable>(
        std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt16>()}));
    std::vector<UInt64> payload;
    /// Stored chunk: dense `Nullable(Tuple(UInt16))`. Next chunk: same key with a sparse tuple child.
    ASSERT_NO_THROW(payload = runFinishSorting(
        key_type, nullableTupleOf(denseUInt16(0, n), n), nullableTupleOf(sparseUInt16(0, n), n), n));
    EXPECT_EQ(payload, payloadOfBothInputsPaired(n));
}

/// The same nesting on the ordinary merge-sort path: a `Tuple(Replicated(UInt16))` sort key reaches
/// the merging cursors and `ColumnTuple::compareAt` delegates to the replicated child.
TEST(MergeSorter, TupleReplicatedChildSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    auto key_type = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt16>()});
    std::vector<UInt64> payload;
    /// Two sorted chunks with the SAME tuple key: one dense, one carrying a replicated tuple child.
    ASSERT_NO_THROW(payload = runMergeSorter(
        key_type, tupleOf(denseUInt16(0, n)), tupleOf(replicatedUInt16(0, n)), n));
    EXPECT_EQ(payload, payloadOfBothInputsPaired(n));
}

namespace
{

/// Drive `MergingSortedTransform` (the k-way merge behind `MergingSortedAlgorithm` and the
/// `*SortedAlgorithm` family) with two already-sorted single-chunk inputs whose sort key `x` holds
/// the SAME value, so the merging cursors compare across sources.
std::vector<UInt64> runMergingSorted(DataTypePtr key_type, ColumnPtr first_key, ColumnPtr second_key, size_t n)
{
    auto type_u64 = std::make_shared<DataTypeUInt64>();

    Block header({
        ColumnWithTypeAndName(key_type->createColumn(), key_type, "x"),
        ColumnWithTypeAndName(type_u64->createColumn(), type_u64, "y"),
    });
    auto shared_header = std::make_shared<const Block>(header);

    auto make_source = [&](ColumnPtr key)
    {
        Columns cols;
        cols.push_back(std::move(key));
        cols.push_back(denseUInt64Iota(n));
        Chunk chunk;
        chunk.setColumns(std::move(cols), n);
        Chunks chunks;
        chunks.emplace_back(std::move(chunk));
        return std::make_shared<SourceFromChunks>(shared_header, std::move(chunks));
    };

    auto source_first = make_source(std::move(first_key));
    auto source_second = make_source(std::move(second_key));

    SortDescription description;
    description.emplace_back("x");
    description.emplace_back("y");

    auto merging = std::make_shared<MergingSortedTransform>(
        shared_header, /*num_inputs=*/2, description, /*max_block_size_rows=*/8192, /*max_block_size_bytes=*/0,
        /*max_dynamic_subcolumns=*/std::nullopt, SortingQueueStrategy::Default, /*limit=*/0);

    auto inputs_it = merging->getInputs().begin();
    connect(source_first->getPort(), *inputs_it++);
    connect(source_second->getPort(), *inputs_it);
    auto * output_port = &merging->getOutputs().front();

    auto processors = std::make_shared<Processors>();
    processors->push_back(source_first);
    processors->push_back(source_second);
    processors->push_back(merging);

    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);

    PullingPipelineExecutor executor(pipeline);
    std::vector<UInt64> payload;
    Block block;
    while (executor.pull(block))
    {
        auto block_payload = collectPayload(block);
        payload.insert(payload.end(), block_payload.begin(), block_payload.end());
    }
    return payload;
}

/// Drive `PartialSortingTransform` (the TopK path) with two blocks: the first sets the saved
/// threshold row, the second is compared against it (`getFilterMask` / `compareWithThreshold`). The
/// limit is >= `min_limit_for_partial_sort_optimization` so the threshold optimization is active.
std::vector<std::vector<UInt64>> runPartialSorting(DataTypePtr key_type, ColumnPtr first_key, ColumnPtr second_key, size_t n)
{
    auto type_u64 = std::make_shared<DataTypeUInt64>();

    Block header({
        ColumnWithTypeAndName(key_type->createColumn(), key_type, "x"),
        ColumnWithTypeAndName(type_u64->createColumn(), type_u64, "y"),
    });
    auto shared_header = std::make_shared<const Block>(header);

    auto make_chunk = [&](ColumnPtr key)
    {
        Columns cols;
        cols.push_back(std::move(key));
        cols.push_back(denseUInt64Iota(n));
        Chunk chunk;
        chunk.setColumns(std::move(cols), n);
        return chunk;
    };

    Chunks chunks;
    chunks.emplace_back(make_chunk(std::move(first_key)));
    chunks.emplace_back(make_chunk(std::move(second_key)));

    SortDescription description;
    description.emplace_back("x");
    description.emplace_back("y");

    auto source = std::make_shared<SourceFromChunks>(shared_header, std::move(chunks));
    /// limit >= min_limit_for_partial_sort_optimization (1500) so the threshold optimization runs.
    auto partial_sorting = std::make_shared<PartialSortingTransform>(shared_header, description, /*limit=*/1500);

    connect(source->getPort(), partial_sorting->getInputPort());
    auto * output_port = &partial_sorting->getOutputPort();

    auto processors = std::make_shared<Processors>();
    processors->push_back(source);
    processors->push_back(partial_sorting);

    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);

    PullingPipelineExecutor executor(pipeline);
    std::vector<std::vector<UInt64>> blocks;
    Block block;
    while (executor.pull(block))
    {
        auto block_payload = collectPayload(block);
        if (!block_payload.empty())
            blocks.push_back(std::move(block_payload));
    }
    return blocks;
}

/// The TopK path emits one sorted block per input block truncated to the LIMIT, and the last row of
/// the first block becomes the threshold the next block's rows must stay strictly below. The count is
/// therefore a live threshold comparison, not merely the absence of an exception: 2999 of 3000.
void expectThresholdFiltered(const std::vector<std::vector<UInt64>> & blocks)
{
    constexpr size_t limit = 1500;

    ASSERT_EQ(blocks.size(), 2u);
    /// Every `x` is equal, so sorting by (`x`, `y`) orders the payload.
    EXPECT_EQ(blocks[0].size(), limit);
    EXPECT_TRUE(std::is_sorted(blocks[0].begin(), blocks[0].end()));

    const UInt64 threshold = blocks[0].back();
    EXPECT_EQ(threshold, limit - 1);
    EXPECT_EQ(blocks[1].size(), threshold);
    EXPECT_TRUE(std::is_sorted(blocks[1].begin(), blocks[1].end()));
    EXPECT_LT(blocks[1].back(), threshold);
}

}

/// The sparse sibling under a nullable on the merge path: `recursiveRemoveSparse` does not recurse
/// through `Nullable`, so a `Nullable(Tuple(Sparse(UInt16)))` child survives `removeConstAndSparse`
/// and reaches the cursors.
TEST(MergingSortedAlgorithm, NullableTupleSparseChildSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    auto key_type = std::make_shared<DataTypeNullable>(
        std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt16>()}));
    std::vector<UInt64> payload;
    ASSERT_NO_THROW(payload = runMergingSorted(
        key_type, nullableTupleOf(denseUInt16(0, n), n), nullableTupleOf(sparseUInt16(0, n), n), n));
    EXPECT_EQ(payload, payloadOfBothInputsPaired(n));
}

/// the TopK/threshold path. `removeSpecialRepresentations` keeps a sparse/replicated child under
/// `Nullable`/`Array`/`Map`, so a `Nullable(Tuple(Sparse(UInt16)))` threshold is compared raw
/// against the next block's dense live keys.
TEST(PartialSortingTransform, NullableTupleSparseThresholdDoesNotBadCast)
{
    /// n must be >= the transform's limit so the first block fills the LIMIT and the threshold is
    /// actually saved (`limit <= block.rows()` in `transform`); the limit itself must be >=
    /// `min_limit_for_partial_sort_optimization` (1500) for the optimization to run.
    constexpr size_t n = 1600;
    auto key_type = std::make_shared<DataTypeNullable>(
        std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt16>()}));
    std::vector<std::vector<UInt64>> blocks;
    /// First block sets the threshold as a sparse-child key; second block (dense) is compared to it.
    ASSERT_NO_THROW(blocks = runPartialSorting(
        key_type, nullableTupleOf(sparseUInt16(0, n), n), nullableTupleOf(denseUInt16(0, n), n), n));
    expectThresholdFiltered(blocks);
}
