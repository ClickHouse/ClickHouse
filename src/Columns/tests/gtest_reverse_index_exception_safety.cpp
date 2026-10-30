#include <Columns/ColumnString.h>
#include <Columns/ColumnUnique.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <DataTypes/DataTypeString.h>

#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace DB::FailPoints
{
    extern const char reverse_index_build_index_throw[];
}

using namespace DB;

/// Mimics what happens during an asynchronous insert flush under memory pressure: the first insert into a
/// `LowCardinality` dictionary throws `MEMORY_LIMIT_EXCEEDED` while the reverse index is being built, the error
/// is handled by the caller, and the same dictionary is reused for the next entry. The index must either be
/// fully built or absent, never a non-null hash table paired with a null saved-hash array.
TEST(ReverseIndex, ExceptionWhileBuildingIndexLeavesNoHalfBuiltIndex)
{
    auto column = ColumnUnique<ColumnString>::create(DataTypeString());

    FailPointInjection::enableFailPoint(FailPoints::reverse_index_build_index_throw);
    try
    {
        column->uniqueInsertData("a", 1);
        FAIL() << "Expected an exception from the failpoint";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }
    FailPointInjection::disableFailPoint(FailPoints::reverse_index_build_index_throw);

    /// Index 0 is the default value (empty string).
    EXPECT_EQ(column->size(), 1u);
    EXPECT_EQ(column->uniqueInsertData("a", 1), 1u);
    EXPECT_EQ(column->uniqueInsertData("b", 1), 2u);
    EXPECT_EQ(column->uniqueInsertData("a", 1), 1u);
    EXPECT_EQ(column->uniqueInsertData("", 0), 0u);
    EXPECT_EQ(column->size(), 3u);
    EXPECT_EQ(column->getDataAt(1), "a");
    EXPECT_EQ(column->getDataAt(2), "b");
}
