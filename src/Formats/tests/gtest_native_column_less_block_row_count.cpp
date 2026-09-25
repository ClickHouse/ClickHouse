#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

using namespace DB;

namespace
{

Block makeColumnLessBlock(size_t num_rows)
{
    Block block;
    block.info.num_rows_without_columns = num_rows;
    return block;
}

String writeToString(const Block & block, UInt64 revision)
{
    WriteBufferFromOwnString out;
    NativeWriter writer(out, revision, std::make_shared<const Block>());
    writer.write(block);
    out.finalize();
    return out.str();
}

Block readFromString(const String & data, UInt64 revision)
{
    ReadBufferFromString in(data);
    NativeReader reader(in, revision);
    return reader.read();
}

}

/// At the revision that introduced it, the row count of a column-less block survives the round trip.
TEST(NativeColumnLessBlock, RowCountRoundTrips)
{
    constexpr UInt64 revision = DBMS_MIN_REVISION_WITH_COLUMN_LESS_BLOCK_ROW_COUNT;
    auto result = readFromString(writeToString(makeColumnLessBlock(42), revision), revision);
    ASSERT_EQ(result.columns(), 0);
    ASSERT_EQ(result.info.num_rows_without_columns, 42);
}

/// A peer below that revision rejects a column-less block that declares rows, so the writer sends zero rows to it.
TEST(NativeColumnLessBlock, OlderPeerGetsZeroRows)
{
    constexpr UInt64 revision = DBMS_MIN_REVISION_WITH_COLUMN_LESS_BLOCK_ROW_COUNT - 1;
    auto result = readFromString(writeToString(makeColumnLessBlock(42), revision), revision);
    ASSERT_EQ(result.columns(), 0);
    ASSERT_EQ(result.info.num_rows_without_columns, 0);
}

/// The new layout read below the revision is malformed data, as it was before the revision existed.
TEST(NativeColumnLessBlock, NewLayoutRejectedBelowRevision)
{
    auto data = writeToString(makeColumnLessBlock(42), DBMS_MIN_REVISION_WITH_COLUMN_LESS_BLOCK_ROW_COUNT);
    try
    {
        readFromString(data, DBMS_MIN_REVISION_WITH_COLUMN_LESS_BLOCK_ROW_COUNT - 1);
        FAIL() << "Expected INCORRECT_DATA";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }
}
