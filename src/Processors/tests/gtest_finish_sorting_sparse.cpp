#include <gtest/gtest.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/FinishSortingTransform.h>
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

/// A dense `Nullable(UInt16)` where every row holds a non-null `value`.
ColumnPtr nullableUInt16(UInt16 value, size_t n)
{
    auto null_map = ColumnUInt8::create();
    null_map->getData().assign(n, static_cast<UInt8>(0));
    return ColumnNullable::create(denseUInt16(value, n), std::move(null_map));
}

/// A sparse `Nullable(UInt16)` sort key where every row holds the default (non-null `value`). The
/// canonical representation wraps the nullable column in the sparse layer --
/// `ColumnSparse(ColumnNullable(...))` -- because `ISerialization`'s SPARSE kind is applied outermost
/// (`IDataType::createColumn`), and `ColumnNullable` rejects a sparse nested column
/// (`ColumnSparse::canBeInsideNullable()` is false). So this is the shape a `Nullable(UInt16)` key
/// actually takes on the read-in-order path when it uses sparse serialization. Built as a fully
/// default sparse column: a one-row nullable "values" column holding the default and empty offsets.
ColumnPtr sparseNullableUInt16(UInt16 value, size_t n)
{
    auto offsets = ColumnUInt64::create();
    return ColumnSparse::create(nullableUInt16(value, 1)->assumeMutable(), std::move(offsets), n);
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
/// SAME key value in both, so consume() runs its cross-chunk binary-search `less()`. `key_type`
/// is the declared type of `x`; the stored chunk uses `first_prefix` and the current chunk uses
/// `second_prefix`. Before the fix, a `second_prefix` carrying a sparse or replicated column
/// (possibly nested in a tuple) makes the raw `compareAt` in `less()` bad-cast its rhs
/// (`Bad cast from type DB::ColumnSparse to DB::ColumnVector<unsigned short>`).
void runFinishSorting(DataTypePtr key_type, ColumnPtr first_prefix, ColumnPtr second_prefix, size_t n)
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
    /// The equal prefix forces the cross-chunk `less()` comparison in consume().
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
    Block block;
    while (executor.pull(block))
        ;
}

/// Shorthand: a plain `UInt16` sort key, dense in the stored chunk and `second_prefix` in the next.
void runFinishSortingUInt16(ColumnPtr second_prefix, size_t n)
{
    runFinishSorting(std::make_shared<DataTypeUInt16>(), denseUInt16(0, n), std::move(second_prefix), n);
}

}

/// Regression test for STID 1499-2393: a dense sort-key column in the stored chunk and the same
/// key as `ColumnSparse` in the next chunk made the cross-chunk `less()` in consume() bad-cast
/// (`Bad cast from type DB::ColumnSparse to DB::ColumnVector<unsigned short>`).
TEST(FinishSortingTransform, SparseSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    /// Before the fix this aborts with `Bad cast from type DB::ColumnSparse to DB::ColumnVector<unsigned short>`.
    EXPECT_NO_THROW(runFinishSortingUInt16(sparseUInt16(0, n), n));
}

/// The `Replicated(Sparse)` variant of the same bug: expanding the replicated wrapper leaves a
/// plain ColumnSparse, so only materializing the replicated wrapper is not enough and `less()`
/// bad-casts again. The fix strips replicated then sparse.
TEST(FinishSortingTransform, ReplicatedSparseSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    /// Before the fix this aborts with the same `Bad cast ... ColumnSparse to ColumnVector<unsigned short>`.
    EXPECT_NO_THROW(runFinishSortingUInt16(replicatedSparseUInt16(0, n), n));
}

/// A tuple sort key whose child stays replicated in the next chunk (raised in review): only the
/// top-level column was materialized, so a `Tuple(Replicated(UInt16))` reached `less()` and
/// `ColumnTuple::compareAt` delegated to `ColumnVector::compareAt(..., ColumnReplicated)` -- the
/// same bad cast one level deeper. The fix recurses into tuple children.
TEST(FinishSortingTransform, TupleReplicatedChildSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    auto key_type = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt16>()});
    /// Stored chunk: dense `Tuple(UInt16)`. Next chunk: same key as `Tuple(Replicated(UInt16))`.
    /// Before the fix this aborts with the same `Bad cast ... ColumnReplicated ... ColumnVector<unsigned short>`.
    EXPECT_NO_THROW(runFinishSorting(
        key_type, tupleOf(denseUInt16(0, n)), tupleOf(replicatedUInt16(0, n)), n));
}

/// A `Nullable(UInt16)` sort key that is dense in the stored chunk and sparse in the next (raised in
/// review). A sparse nullable column is `ColumnSparse(ColumnNullable(...))` (sparse is the OUTER
/// wrapper -- see `sparseNullableUInt16`), so the top-level `convertToFullColumnIfSparse` in the fix
/// densifies it before `less()`; `ColumnNullable::compareAt` then sees a dense nested rhs. Confirms
/// the reachable sparse-nullable shape does not bad-cast.
TEST(FinishSortingTransform, NullableSparseSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    auto key_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt16>());
    EXPECT_NO_THROW(runFinishSorting(key_type, nullableUInt16(0, n), sparseNullableUInt16(0, n), n));
}

/// The nesting the review worried about -- `ColumnNullable(ColumnSparse(...))`, a dense null map over
/// a sparse nested values column -- is unconstructible: `ColumnNullable`'s constructor rejects a
/// nested column whose `canBeInsideNullable()` is false, and `ColumnSparse` inherits the `false`
/// default. So a `Nullable` sort key can never reach `less()` with a sparse nested column; the sparse
/// layer is always outside the nullable one (`NullableSparseSortKeyDoesNotBadCast`). This pins that
/// invariant so a future change that makes sparse nullable-able would fail here loudly.
TEST(FinishSortingTransform, NullableCannotWrapSparse)
{
    constexpr size_t n = 8;
    auto null_map = ColumnUInt8::create();
    null_map->getData().assign(n, static_cast<UInt8>(0));
    EXPECT_ANY_THROW(ColumnNullable::create(sparseUInt16(0, n)->assumeMutable(), std::move(null_map)));
}
