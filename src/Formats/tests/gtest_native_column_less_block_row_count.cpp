#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
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

String writeWithOutputFormat(const Block & block, UInt64 revision)
{
    tryRegisterFormats();
    FormatSettings settings;
    settings.client_protocol_version = revision;
    WriteBufferFromOwnString out;
    auto format = FormatFactory::instance().getOutputFormat("Native", out, Block{}, getContext().context, settings);
    format->write(block);
    format->finalize();
    out.finalize();
    return out.str();
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

/// A peer below that revision rejects a column-less block that declares rows, and sending zero rows to it
/// would lose them, so the writer refuses.
TEST(NativeColumnLessBlock, OlderPeerIsRefused)
{
    constexpr UInt64 revision = DBMS_MIN_REVISION_WITH_COLUMN_LESS_BLOCK_ROW_COUNT - 1;
    try
    {
        writeToString(makeColumnLessBlock(42), revision);
        FAIL() << "Expected NOT_IMPLEMENTED";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED);
    }
}

/// A column-less block with no rows is still written to an older peer as before.
TEST(NativeColumnLessBlock, OlderPeerGetsEmptyBlock)
{
    constexpr UInt64 revision = DBMS_MIN_REVISION_WITH_COLUMN_LESS_BLOCK_ROW_COUNT - 1;
    auto result = readFromString(writeToString(makeColumnLessBlock(0), revision), revision);
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

/// The `Native` output format passes the row count of a column-less block to the writer, at the revision
/// that introduced it (e.g. HTTP with `client_protocol_version`).
TEST(NativeColumnLessBlock, OutputFormatRowCountRoundTrips)
{
    constexpr UInt64 revision = DBMS_MIN_REVISION_WITH_COLUMN_LESS_BLOCK_ROW_COUNT;
    auto result = readFromString(writeWithOutputFormat(makeColumnLessBlock(42), revision), revision);
    ASSERT_EQ(result.columns(), 0);
    ASSERT_EQ(result.info.num_rows_without_columns, 42);
}

/// Below that revision (a `Native` file, for one) the `Native` output format refuses the rows instead of
/// silently writing an empty block.
TEST(NativeColumnLessBlock, OutputFormatOlderRevisionIsRefused)
{
    try
    {
        writeWithOutputFormat(makeColumnLessBlock(42), 0);
        FAIL() << "Expected NOT_IMPLEMENTED";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED);
    }
}

/// The `Native` input format reads at revision 0, where a block of no columns and some rows is malformed
/// data, not the end of the data.
TEST(NativeColumnLessBlock, InputFormatRejectsColumnLessRows)
{
    tryRegisterFormats();
    /// The layout of a block at revision 0: the number of columns, then the number of rows.
    const String data("\x00\x2a", 2);
    ReadBufferFromString in(data);
    auto format = FormatFactory::instance().getInput("Native", in, Block{}, getContext().context, 1000);
    try
    {
        format->read();
        FAIL() << "Expected INCORRECT_DATA";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }
}
