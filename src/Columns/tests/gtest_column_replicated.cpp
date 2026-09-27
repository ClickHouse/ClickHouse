#include <Columns/ColumnArray.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <gtest/gtest.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

static MutableColumnPtr createNestedColumn(const VectorWithMemoryTracking<String> & values)
{
    MutableColumnPtr nested_column = ColumnString::create();
    for (const auto & value : values)
        nested_column->insert(value);
    return nested_column;
}

static ColumnReplicated::MutablePtr createColumn(const VectorWithMemoryTracking<String> & values, const VectorWithMemoryTracking<size_t> & indexes)
{
    MutableColumnPtr nested_column = createNestedColumn(values);
    MutableColumnPtr indexes_column = ColumnUInt8::create();
    for (const auto & index : indexes)
        indexes_column->insert(index);

    return ColumnReplicated::create(std::move(nested_column), std::move(indexes_column));
}

static void checkColumn(const ColumnReplicated & column, const VectorWithMemoryTracking<String> & expected_values, const VectorWithMemoryTracking<size_t> & expected_indexes)
{
    const auto & nested_column = column.getNestedColumn();
    ASSERT_EQ(nested_column->size(), expected_values.size());
    for (size_t i = 0; i < expected_values.size(); ++i)
        ASSERT_EQ((*nested_column)[i], Field(expected_values[i]));

    const auto & indexes = column.getIndexes().getIndexes();
    ASSERT_EQ(indexes->size(), expected_indexes.size());
    for (size_t i = 0; i < expected_indexes.size(); ++i)
        ASSERT_EQ((*indexes)[i], Field(expected_indexes[i]));
}

static void checkColumn(const IColumn & column, const VectorWithMemoryTracking<String> & expected_values, const VectorWithMemoryTracking<size_t> & expected_indexes)
{
    checkColumn(assert_cast<const ColumnReplicated &>(column), expected_values, expected_indexes);
}

TEST(ColumnReplicated, CloneResized)
{
    auto column = createColumn({"s1", "s2", "s3"}, {0, 1, 1, 2, 0, 0, 1, 2, 0});
    auto resized_column = column->cloneResized(6);
    checkColumn(assert_cast<const ColumnReplicated &>(*resized_column), {"s1", "s2", "s3"}, {0, 1, 1, 2, 0, 0});
    resized_column = resized_column->cloneResized(3);
    checkColumn(assert_cast<const ColumnReplicated &>(*resized_column), {"s1", "s2"}, {0, 1, 1});
    resized_column = resized_column->cloneResized(1);
    checkColumn(assert_cast<const ColumnReplicated &>(*resized_column), {"s1"}, {0});
    resized_column = resized_column->cloneResized(3);
    checkColumn(assert_cast<const ColumnReplicated &>(*resized_column), {"s1", ""}, {0, 1, 1});
}


TEST(ColumnReplicated, PopBack)
{
    auto column = createColumn({"s1", "s2", "s3"}, {2, 1, 1, 2, 0, 0, 1, 2, 0});
    column->popBack(3);
    checkColumn(*column, {"s1", "s2", "s3"}, {2, 1, 1, 2, 0, 0});
    column->popBack(3);
    checkColumn(*column, {"s1", "s2", "s3"}, {2, 1, 1});
    column->popBack(2);
    checkColumn(*column, {"s1", "s2", "s3"}, {2});
}

TEST(ColumnReplicated, Filter)
{
    auto column = createColumn({"s1", "s2", "s3"}, {2, 1, 1, 2, 0, 0, 1, 2, 0});
    IColumnFilter filter = {0, 0, 0, 1, 1, 0, 0, 0, 0};
    auto filtered_column = column->filter(filter, 2);
    checkColumn(*filtered_column, {"s1", "s2", "s3"}, {{2, 0}});
}

TEST(ColumnReplicated, Index)
{
    auto column = createColumn({"s1", "s2", "s3"}, {2, 1, 1, 2, 0, 0, 1, 2, 0});
    auto index_column = ColumnUInt64::create();
    index_column->getData() = {3, 4};
    auto filtered_column = column->index(*index_column, 0);
    checkColumn(*filtered_column, {"s1", "s2", "s3"}, {2, 0});
}

TEST(ColumnReplicated, Permute)
{
    auto column = createColumn({"s1", "s2", "s3"}, {2, 1, 1, 2, 0, 0, 1, 2, 0});
    IColumnPermutation permutation = {3, 4, 0, 1, 2, 5, 6, 7, 8};
    auto filtered_column = column->permute(permutation, 2);
    checkColumn(*filtered_column, {"s1", "s2", "s3"}, {2, 0});
}

TEST(ColumnReplicated, PermuteShorterThanColumnWithLimit)
{
    /// Relaxed contract: a permutation shorter than the column is legal when limit == perm.size()
    /// (see IColumn::permute / getLimitForPermutation). This is the case ORDER BY ... LIMIT
    /// produces for a replicated payload column. permute must succeed and take the first
    /// `limit` rows.
    auto column = createColumn({"s1", "s2", "s3"}, {2, 1, 1, 2, 0, 0, 1, 2, 0});
    IColumnPermutation permutation = {3, 4};
    auto permuted_column = column->permute(permutation, 2);
    checkColumn(*permuted_column, {"s1", "s2", "s3"}, {2, 0});
}

TEST(ColumnReplicated, PermuteTooShortForLimitThrows)
{
    /// Kept negative contract: perm.size() < min(size(), limit) is invalid and must throw.
    auto column = createColumn({"s1", "s2", "s3"}, {2, 1, 1, 2, 0, 0, 1, 2, 0});
    IColumnPermutation permutation = {3, 4};
    EXPECT_THROW(column->permute(permutation, 5), DB::Exception);
}

TEST(ColumnReplicated, InsertRangeFrom)
{
    auto column_to = createColumn({"s0", "s1"}, {0, 1});
    auto column_from = createColumn({"s2", "s3", "s4"}, {0, 1, 1, 2, 0, 0, 1, 2, 0});
    column_to->insertRangeFrom(*column_from, 3, 4);
    checkColumn(*column_to, {"s0", "s1", "s4", "s2", "s3"}, {0, 1, 2, 3, 3, 4});
}

TEST(ColumnReplicated, RollbackClearsInsertionCache)
{
    /// insertion_cache memoizes absolute indexes into nested_column keyed by the source id.
    /// rollback() shrinks nested_column and indexes, so it must invalidate the cache (like filter() does);
    /// otherwise a re-insert from the same source hits a stale entry, skips the real insert and stores a
    /// dangling index >= nested_column->size().
    MutableColumnPtr src_nested = ColumnUInt64::create();
    src_nested->insert(10);
    src_nested->insert(11);
    src_nested->insert(12);
    MutableColumnPtr src_indexes = ColumnUInt8::create();
    src_indexes->insert(0);
    src_indexes->insert(1);
    src_indexes->insert(2);
    auto src = ColumnReplicated::create(std::move(src_nested), std::move(src_indexes));

    MutableColumnPtr dest_nested = ColumnUInt64::create();
    auto dest = ColumnReplicated::create(std::move(dest_nested));
    auto checkpoint = dest->getCheckpoint();

    dest->insertFrom(*src, 0);
    dest->insertFrom(*src, 1);
    dest->insertFrom(*src, 2);
    ASSERT_EQ(dest->size(), 3);
    ASSERT_EQ(dest->getNestedColumn()->size(), 3);

    dest->rollback(*checkpoint);
    ASSERT_EQ(dest->size(), 0);
    ASSERT_EQ(dest->getNestedColumn()->size(), 0);

    /// Re-insert the source value that was cached before the rollback.
    dest->insertFrom(*src, 2);
    ASSERT_EQ(dest->size(), 1);
    /// The value must have been re-inserted into nested_column (cache miss), not skipped via a stale entry.
    ASSERT_EQ(dest->getNestedColumn()->size(), 1)
        << "rollback() left a stale insertion_cache; the re-insert was skipped and the stored index is dangling";
    ASSERT_LT(assert_cast<const ColumnReplicated &>(*dest).getIndexes().getIndexAt(0), dest->getNestedColumn()->size());
    auto full = assert_cast<const ColumnReplicated &>(*dest).convertToFullColumnIfReplicated();
    ASSERT_EQ(full->size(), 1u);
    ASSERT_EQ(full->getUInt(0), 12u);
}

TEST(ColumnReplicated, CopyShortRowsFrom)
{
    const String long_value(200, 'l');
    auto source = createColumn({"a", "b", long_value}, {0, 2, 0, 2});
    auto other_source = createColumn({long_value, "z"}, {0, 1, 1});
    MutableColumnPtr dest_nested = ColumnString::create();
    auto dest = ColumnReplicated::create(std::move(dest_nested));
    dest->copyShortRowsFrom(*source);
    /// Short rows of `source` are copied, its long row is shared; rows of an unregistered source are shared.
    dest->insertFrom(*source, 0);
    dest->insertFrom(*source, 2);
    dest->insertRangeFrom(*source, 1, 3);
    dest->insertManyFrom(*source, 1, 2);
    dest->insertFrom(*other_source, 0);
    dest->insertRangeFrom(*other_source, 1, 2);
    checkColumn(*dest, {"a", "a", long_value, "a", long_value, "z"}, {0, 1, 2, 3, 2, 2, 2, 4, 5, 5});

    /// Rows of other column families are shared however short.
    MutableColumnPtr array_nested = ColumnArray::create(ColumnUInt64::create());
    array_nested->insert(Array{UInt64(1)});
    MutableColumnPtr array_indexes = ColumnUInt8::create();
    array_indexes->insert(0);
    array_indexes->insert(0);
    auto array_source = ColumnReplicated::create(std::move(array_nested), std::move(array_indexes));
    MutableColumnPtr array_dest_nested = ColumnArray::create(ColumnUInt64::create());
    auto array_dest = ColumnReplicated::create(std::move(array_dest_nested));
    array_dest->copyShortRowsFrom(*array_source);
    array_dest->insertFrom(*array_source, 0);
    array_dest->insertFrom(*array_source, 1);
    ASSERT_EQ(array_dest->getNestedColumn()->size(), 1);
    ASSERT_EQ(array_dest->size(), 2);
    ASSERT_EQ((*array_dest)[1], Field(Array{UInt64(1)}));
}

TEST(ColumnReplicated, IndicesOfNonDefaultRows)
{
    auto column = createColumn({"s1", "s2", "", "s3", ""}, {0, 1, 1, 3, 2, 0, 0, 3, 1, 2, 0, 3, 4});
    IColumn::Offsets offsets;
    column->getIndicesOfNonDefaultRows(offsets, 0, column->size());
    ASSERT_EQ(offsets.size(), 10);
    ASSERT_EQ(offsets, IColumn::Offsets({0, 1, 2, 3, 5, 6, 7, 8, 10, 11}));
}
