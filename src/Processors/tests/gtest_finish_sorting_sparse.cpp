#include <gtest/gtest.h>

#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/SortDescription.h>
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

/// A Replicated(Sparse) over UInt16 where every row holds `value`. Expanding the replicated
/// wrapper (`convertToFullColumnIfReplicated`) calls `ColumnSparse::index`, which keeps the
/// sparse wrapper, so the result is still a plain ColumnSparse: exactly the case the plain
/// `convertToFullColumnIfSparse()->convertToFullColumnIfReplicated()` chain fails to densify.
ColumnPtr replicatedSparseUInt16(UInt16 value, size_t n)
{
    /// Single-row sparse nested column holding `value`, replicated n times by the indexes.
    ColumnPtr nested = sparseUInt16(value, 1);
    auto indexes = ColumnUInt8::create();
    indexes->getData().assign(n, static_cast<UInt8>(0));
    return ColumnReplicated::create(nested, std::move(indexes));
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
/// SAME key value in both, so consume() runs its cross-chunk binary-search `less()`. The stored
/// chunk's prefix is a dense UInt16 column; the current chunk's prefix uses `second_prefix`.
/// Before the fix, a non-dense `second_prefix` makes `ColumnVector<UInt16>::doCompareAt` bad-cast
/// its rhs (`Bad cast from type DB::ColumnSparse to DB::ColumnVector<unsigned short>`).
void runFinishSorting(ColumnPtr second_prefix, size_t n)
{
    auto type_u16 = std::make_shared<DataTypeUInt16>();
    auto type_u64 = std::make_shared<DataTypeUInt64>();

    Block header({
        ColumnWithTypeAndName(type_u16->createColumn(), type_u16, "x"),
        ColumnWithTypeAndName(type_u64->createColumn(), type_u64, "y"),
    });
    auto shared_header = std::make_shared<const Block>(header);

    /// First chunk: prefix `x` is a DENSE column (all zeros), sorted.
    Chunk chunk_first;
    {
        Columns cols;
        cols.push_back(denseUInt16(0, n));
        cols.push_back(denseUInt64Iota(n));
        chunk_first.setColumns(std::move(cols), n);
    }

    /// Second chunk: prefix `x` holds the SAME key value (all zeros) as `second_prefix`.
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

}

/// Regression test for STID 1499-2393: a dense sort-key column in the stored chunk and the same
/// key as `ColumnSparse` in the next chunk made the cross-chunk `less()` in consume() bad-cast
/// (`Bad cast from type DB::ColumnSparse to DB::ColumnVector<unsigned short>`).
TEST(FinishSortingTransform, SparseSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    /// Before the fix this aborts with `Bad cast from type DB::ColumnSparse to DB::ColumnVector<unsigned short>`.
    EXPECT_NO_THROW(runFinishSorting(sparseUInt16(0, n), n));
}

/// The `Replicated(Sparse)` variant of the same bug (raised in review): expanding the replicated
/// wrapper leaves a plain ColumnSparse, so the plain `convertToFullColumnIfSparse()->
/// convertToFullColumnIfReplicated()` chain did NOT densify it and `less()` bad-cast again.
/// `removeSpecialRepresentations` (replicated first, then recursive sparse) fixes both.
TEST(FinishSortingTransform, ReplicatedSparseSortKeyDoesNotBadCast)
{
    constexpr size_t n = 8;
    /// Before the fix this aborts with the same `Bad cast ... ColumnSparse to ColumnVector<unsigned short>`.
    EXPECT_NO_THROW(runFinishSorting(replicatedSparseUInt16(0, n), n));
}
