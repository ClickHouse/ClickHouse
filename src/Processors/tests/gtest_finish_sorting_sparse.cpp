#include <gtest/gtest.h>

#include <algorithm>

#include <Columns/ColumnConst.h>
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

/// A ColumnConst over UInt16 where every row holds `value`.
ColumnPtr constUInt16(UInt16 value, size_t n)
{
    return ColumnConst::create(denseUInt16(value, 1), n);
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

/// A Replicated(Sparse) over UInt16 where every row holds `value`. Expanding the replicated
/// wrapper (`convertToFullColumnIfReplicated`) calls `ColumnSparse::index`, which keeps the
/// sparse wrapper, so the result is still a plain ColumnSparse: exactly the case only materializing
/// the replicated wrapper fails to densify.
ColumnPtr replicatedSparseUInt16(UInt16 value, size_t n)
{
    /// Single-row sparse nested column holding `value`, replicated n times by the indexes.
    ColumnPtr nested = sparseUInt16(value, 1);
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

/// A dense `Nullable(UInt16)` where every row holds a non-null `value`.
ColumnPtr nullableUInt16(UInt16 value, size_t n)
{
    auto null_map = ColumnUInt8::create();
    null_map->getData().assign(n, static_cast<UInt8>(0));
    return ColumnNullable::create(denseUInt16(value, n), std::move(null_map));
}

/// A sparse `Nullable(UInt16)` sort key where every row holds a non-null `value`. Sparse is the
/// OUTER layer -- `ColumnSparse(ColumnNullable(...))` -- because `ISerialization`'s SPARSE kind is
/// applied outermost (`IDataType::createColumn`), and `ColumnNullable` rejects a sparse nested column
/// (`ColumnSparse::canBeInsideNullable` is false).
///
/// Slot zero of a sparse column is its implicit default, which for `Nullable(UInt16)` is `NULL`, so
/// the non-null rows are the ones stored at the offsets.
ColumnPtr sparseNullableUInt16(UInt16 value, size_t n)
{
    auto values = ColumnUInt16::create();
    auto null_map = ColumnUInt8::create();
    auto offsets = ColumnUInt64::create();

    values->getData().push_back(static_cast<UInt16>(0));
    null_map->getData().push_back(static_cast<UInt8>(1));
    for (size_t i = 0; i < n; ++i)
    {
        values->getData().push_back(value);
        null_map->getData().push_back(static_cast<UInt8>(0));
        offsets->getData().push_back(i);
    }

    return ColumnSparse::create(
        ColumnNullable::create(std::move(values), std::move(null_map)), std::move(offsets), n);
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

/// Feed two chunks into FinishSortingTransform where the already-sorted prefix `x` holds the
/// SAME key value in both, so consume runs its cross-chunk binary-search `less`. `key_type`
/// is the declared type of `x`; the stored chunk uses `first_prefix` and the current chunk uses
/// `second_prefix`. Before the fix, a `second_prefix` carrying a sparse or replicated column
/// (possibly nested in a tuple) makes the raw `compareAt` in `less` bad-cast its rhs
/// (`Bad cast from type DB::ColumnSparse to DB::ColumnVector<unsigned short>`). Returns the `y`
/// payload in output order: `compareAt` only checks its operand type in debug and sanitizer builds,
/// so the sort result is what pins the comparison in a release build too.
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

/// Drive `MergeSorter` (the ordinary merge-sort path used by `MergeSortingTransform`) directly with
/// two already-sorted chunks whose sort key `x` holds the SAME value, so building and draining the
/// merging queue exercises the cross-cursor `compareAt`. `first_key`/`second_key` are the `x` column
/// of each chunk; before the fix a `second_key` carrying a replicated column nested in a tuple made
/// `ColumnTuple::compareAt` delegate to `ColumnVector::compareAt(..., ColumnReplicated)` and bad-cast.
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

/// Regression test for STID 1499-2393: a dense sort-key column in the stored chunk and the same
/// key as `ColumnSparse` in the next chunk made the cross-chunk `less` in consume bad-cast
/// (`Bad cast from type DB::ColumnSparse to DB::ColumnVector<unsigned short>`).
///
/// Originally captured by the AST fuzzer on 02149_read_in_order_fixed_prefix over amd_msan: the
/// read-in-order sort prefix `toStartOfMonth(date)` (a Date == UInt16) reached the compare dense in
/// one chunk and ColumnSparse in the next. That state is not reliably reachable from SQL (the
/// standard pipeline densifies both chunks symmetrically), so the deterministic proof lives here.
TEST(FinishSortingTransform, SparseSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    std::vector<UInt64> payload;
    /// Before the fix this aborts with `Bad cast from type DB::ColumnSparse to DB::ColumnVector<unsigned short>`.
    ASSERT_NO_THROW(payload = runFinishSortingUInt16(sparseUInt16(0, n), n));
    EXPECT_EQ(payload, payloadOfBothInputsPaired(n));
}

/// The `Replicated(Sparse)` variant of the same bug: expanding the replicated wrapper leaves a
/// plain ColumnSparse, so only materializing the replicated wrapper is not enough and `less`
/// bad-casts again. The fix strips replicated then sparse.
TEST(FinishSortingTransform, ReplicatedSparseSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    std::vector<UInt64> payload;
    /// Before the fix this aborts with the same `Bad cast ... ColumnSparse to ColumnVector<unsigned short>`.
    ASSERT_NO_THROW(payload = runFinishSortingUInt16(replicatedSparseUInt16(0, n), n));
    EXPECT_EQ(payload, payloadOfBothInputsPaired(n));
}

/// A tuple sort key whose child stays replicated in the next chunk: only the
/// top-level column was materialized, so a `Tuple(Replicated(UInt16))` reached `less` and
/// `ColumnTuple::compareAt` delegated to `ColumnVector::compareAt(..., ColumnReplicated)` -- the
/// same bad cast one level deeper. The fix recurses into tuple children.
TEST(FinishSortingTransform, TupleReplicatedChildSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    auto key_type = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt16>()});
    /// Stored chunk: dense `Tuple(UInt16)`. Next chunk: same key as `Tuple(Replicated(UInt16))`.
    std::vector<UInt64> payload;
    /// Before the fix this aborts with the same `Bad cast ... ColumnReplicated ... ColumnVector<unsigned short>`.
    ASSERT_NO_THROW(payload = runFinishSorting(
        key_type, tupleOf(denseUInt16(0, n)), tupleOf(replicatedUInt16(0, n)), n));
    EXPECT_EQ(payload, payloadOfBothInputsPaired(n));
}

/// A `Nullable(UInt16)` sort key that is dense in the stored chunk and sparse in the next. A sparse
/// nullable column is `ColumnSparse(ColumnNullable(...))`, sparse being the OUTER wrapper (see
/// `sparseNullableUInt16`), so the top-level sparse strip densifies it before `less` and
/// `ColumnNullable::compareAt` sees a dense nested rhs. Without the strip its own `assert_cast` of
/// the rhs fails first: `Bad cast from type DB::ColumnSparse to DB::ColumnNullable`.
TEST(FinishSortingTransform, NullableSparseSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    auto key_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt16>());
    std::vector<UInt64> payload;
    ASSERT_NO_THROW(payload = runFinishSorting(key_type, nullableUInt16(0, n), sparseNullableUInt16(0, n), n));
    EXPECT_EQ(payload, payloadOfBothInputsPaired(n));
}

/// `ColumnNullable` can hide a `ColumnTuple` that carries a sparse/replicated child: `Tuple` can be
/// inside `Nullable`, and a tuple keeps sparse/replicated children, so
/// `Nullable(Tuple(Sparse(UInt16)))` is a constructible sort key. `ColumnNullable::compareAt`
/// delegates to the nested tuple, which delegates to its child, reaching
/// `ColumnVector::compareAt(..., ColumnSparse)` -- the same bad cast two wrappers deep. The generic
/// subcolumn walk materializes children at every level, so this case must not bad-cast.
TEST(FinishSortingTransform, NullableTupleSparseChildSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    auto key_type = std::make_shared<DataTypeNullable>(
        std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt16>()}));
    std::vector<UInt64> payload;
    /// Stored chunk: dense `Nullable(Tuple(UInt16))`. Next chunk: same key with a sparse tuple child.
    /// Before the fix this aborts with `Bad cast ... ColumnSparse to ColumnVector<unsigned short>`.
    ASSERT_NO_THROW(payload = runFinishSorting(
        key_type, nullableTupleOf(denseUInt16(0, n), n), nullableTupleOf(sparseUInt16(0, n), n), n));
    EXPECT_EQ(payload, payloadOfBothInputsPaired(n));
}

/// The replicated sibling of the case above: `Nullable(Tuple(Replicated(UInt16)))`. Peeling only the
/// top-level wrapper leaves the replicated column reachable through the nullable+tuple delegation.
/// The generic walk expands the replicated child, so `less` compares dense columns.
TEST(FinishSortingTransform, NullableTupleReplicatedChildSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    auto key_type = std::make_shared<DataTypeNullable>(
        std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt16>()}));
    std::vector<UInt64> payload;
    /// Stored chunk: dense `Nullable(Tuple(UInt16))`. Next chunk: same key with a replicated tuple child.
    /// Before the fix this aborts with `Bad cast ... ColumnReplicated to ColumnVector<unsigned short>`.
    ASSERT_NO_THROW(payload = runFinishSorting(
        key_type, nullableTupleOf(denseUInt16(0, n), n), nullableTupleOf(replicatedUInt16(0, n), n), n));
    EXPECT_EQ(payload, payloadOfBothInputsPaired(n));
}

/// The same nested-replicated hazard on the ordinary merge-sort path (`MergeSortingTransform` ->
/// `MergeSorter`), not just the read-in-order `FinishSortingTransform`. Before
/// the fix `MergeSorter` peeled only a top-level `ColumnReplicated`, so a `Tuple(Replicated(UInt16))`
/// sort key reached the merging cursors and `ColumnTuple::compareAt` delegated to
/// `ColumnVector::compareAt(..., ColumnReplicated)` -- the same bad cast one level deeper. Both sort
/// sites now share `IColumn::convertToFullIfWrapped`, which recurses through composite children.
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

/// Drive `MergingSortedTransform` (the k-way merge used by `MergingSortedAlgorithm`, the base of the
/// *SortedAlgorithm family) with two already-sorted single-chunk inputs whose sort key `x` holds the
/// SAME value, so the merging cursors compare across sources. `first_key`/`second_key` are the `x`
/// column of each input. Before the fix `IMergingAlgorithm::removeReplicatedFromSortingColumns` only
/// stripped a top-level `ColumnReplicated` and the following `removeConstAndSparse`'s
/// `recursiveRemoveSparse` recursed only through `Replicated`/`Tuple`, so a composite key like
/// `Tuple(Replicated(UInt16))` or `Nullable(Tuple(Sparse(UInt16)))` reached `SortCursorImpl` with one
/// side dense and the other wrapped, and `compareAt` bad-cast one level deeper.
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
/// Before the fix the saved threshold was normalized with `removeSpecialRepresentations`, which
/// (like the merge path) recurses only through `Replicated`/`Tuple`, so a composite key like
/// `Nullable(Tuple(Sparse(UInt16)))` kept a sparse/replicated child in the threshold and the raw
/// comparison against the dense live keys bad-cast one level deeper.
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

/// The TopK path emits one sorted block per input block, truncated to the LIMIT, and the last row of
/// the first block becomes the threshold the next block's rows are compared against, keeping only
/// those strictly below it. So a live threshold comparison is visible in the output rather than only
/// in the absence of an exception: 2999 rows of a possible 3000, the second block one row short of
/// the limit and bounded by the threshold value.
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

/// the merge-algorithm path (`MergingSortedAlgorithm` and its `*SortedAlgorithm` siblings),
/// which strip sort keys via `IMergingAlgorithm::removeReplicatedFromSortingColumns` +
/// `removeConstAndSparse`. A `Tuple(Replicated(UInt16))` key reached the merging cursors with one
/// side dense and the other replicated. `ColumnTuple::compareAt` delegated to
/// `ColumnVector::compareAt(..., ColumnReplicated)` -- the same bad cast one level deeper. The fix
/// materializes the sort keys recursively there too.
TEST(MergingSortedAlgorithm, TupleReplicatedChildSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    auto key_type = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt16>()});
    std::vector<UInt64> payload;
    ASSERT_NO_THROW(payload = runMergingSorted(
        key_type, tupleOf(denseUInt16(0, n)), tupleOf(replicatedUInt16(0, n)), n));
    EXPECT_EQ(payload, payloadOfBothInputsPaired(n));
}

/// The sparse sibling nested under a nullable on the merge path: `Nullable(Tuple(Sparse(UInt16)))`.
/// `recursiveRemoveSparse` does not recurse through `Nullable`, so before the fix the sparse child
/// survived `removeConstAndSparse` and reached the cursors. The recursive materializer densifies it.
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

/// the TopK/threshold path (`PartialSortingTransform`). The saved threshold row was normalized
/// with `removeSpecialRepresentations`, which keeps a sparse/replicated child under
/// `Nullable`/`Array`/`Map`. On the next block the raw comparison against the dense live keys
/// (`compareWithThreshold` / `getFilterMask`) bad-cast one level deeper for a
/// `Nullable(Tuple(Sparse(UInt16)))` threshold. The fix materializes the threshold recursively.
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

/// The replicated sibling of the TopK case: a `Tuple(Replicated(UInt16))` threshold.
TEST(PartialSortingTransform, TupleReplicatedThresholdDoesNotBadCast)
{
    constexpr size_t n = 1600;
    auto key_type = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt16>()});
    std::vector<std::vector<UInt64>> blocks;
    ASSERT_NO_THROW(blocks = runPartialSorting(
        key_type, tupleOf(replicatedUInt16(0, n)), tupleOf(denseUInt16(0, n)), n));
    expectThresholdFiltered(blocks);
}

/// The const key goes the other way: only the threshold is materialized here, so unwrapping it
/// leaves the live key column const, and `ColumnConst::compareAt` casts its rhs to `ColumnConst`.
/// `02427_column_nullable_ubsan` (`SELECT 0 AS a, ... ORDER BY a DESC, b DESC, c ASC LIMIT 1500`)
/// is this shape, and segfaults where `assert_cast` compiles to `static_cast`.
TEST(PartialSortingTransform, ConstThresholdDoesNotBadCast)
{
    constexpr size_t n = 1600;
    auto key_type = std::make_shared<DataTypeUInt16>();
    std::vector<std::vector<UInt64>> blocks;
    ASSERT_NO_THROW(blocks = runPartialSorting(
        key_type, constUInt16(0, n), constUInt16(0, n), n));
    expectThresholdFiltered(blocks);
}
