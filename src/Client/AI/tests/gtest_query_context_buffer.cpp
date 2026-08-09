#include <gtest/gtest.h>
#include "config.h"

#if USE_CLIENT_AI

#include <Client/AI/QueryContextBuffer.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

using namespace DB;

namespace
{

Block makeBlock(size_t start, size_t rows)
{
    auto column = ColumnUInt64::create();
    for (size_t i = 0; i < rows; ++i)
        column->insertValue(start + i);
    return Block{ColumnWithTypeAndName{std::move(column), std::make_shared<DataTypeUInt64>(), "n"}};
}

}

TEST(QueryContextBuffer, SmallResult)
{
    QueryContextBuffer buffer;
    buffer.startQuery("SELECT 1", false);
    buffer.addBlock(makeBlock(0, 3));
    buffer.finishQuery(0.5, false);

    String text = buffer.format(0, false);
    EXPECT_NE(text.find("Query: SELECT 1"), String::npos);
    EXPECT_NE(text.find("Result: 3 rows"), String::npos);
    EXPECT_NE(text.find("n:UInt64"), String::npos);
    EXPECT_NE(text.find("\n0\n1\n2\n"), String::npos);
    /// A small result has no truncation marker.
    EXPECT_EQ(text.find("…"), String::npos);
}

TEST(QueryContextBuffer, LargeResultIsTruncatedToHeadAndTail)
{
    QueryContextBuffer buffer;
    buffer.startQuery("SELECT big", false);
    for (size_t block = 0; block < 10; ++block)
        buffer.addBlock(makeBlock(block * 100, 100));
    buffer.finishQuery(1.0, false);

    String text = buffer.format(0, false);
    EXPECT_NE(text.find("Result: 1000 rows"), String::npos);
    /// The head sample.
    EXPECT_NE(text.find("\n0\n1\n"), String::npos);
    /// The gap.
    EXPECT_NE(text.find("…"), String::npos);
    /// The tail sample.
    EXPECT_NE(text.find("\n998\n999\n"), String::npos);
    /// No middle rows.
    EXPECT_EQ(text.find("\n500\n"), String::npos);
}

TEST(QueryContextBuffer, ErrorsAndStandaloneErrors)
{
    QueryContextBuffer buffer;

    buffer.startQuery("SELECT bad", false);
    buffer.recordError("SELECT bad", "Missing columns: 'bad'");
    buffer.finishQuery(0.1, false);

    /// A query that failed before start (e.g. a parse error).
    buffer.recordError("SELEC 1", "Syntax error");

    String text = buffer.format(0, false);
    EXPECT_NE(text.find("Error: Missing columns: 'bad'"), String::npos);
    EXPECT_NE(text.find("Query: SELEC 1"), String::npos);
    EXPECT_NE(text.find("Error: Syntax error"), String::npos);

    /// The first recorded error wins.
    buffer.startQuery("SELECT worse", false);
    buffer.recordError("SELECT worse", "first");
    buffer.recordError("SELECT worse", "second");
    buffer.finishQuery(0.1, false);
    text = buffer.format(0, false);
    EXPECT_NE(text.find("Error: first"), String::npos);
    EXPECT_EQ(text.find("Error: second"), String::npos);
}

TEST(QueryContextBuffer, SeqnoFilteringAndAISkipping)
{
    QueryContextBuffer buffer;

    buffer.startQuery("SELECT 1", false);
    buffer.finishQuery(0.1, false);
    UInt64 seen = buffer.latestSeqno();

    buffer.startQuery("SELECT 2", false);
    buffer.finishQuery(0.1, false);
    buffer.startQuery("SELECT 3", true); /// from the AI agent
    buffer.finishQuery(0.1, false);

    String text = buffer.format(seen, true);
    EXPECT_EQ(text.find("SELECT 1"), String::npos);
    EXPECT_NE(text.find("SELECT 2"), String::npos);
    EXPECT_EQ(text.find("SELECT 3"), String::npos);

    /// Without skipping, the agent's queries are included (used for tool result summaries).
    text = buffer.format(seen, false);
    EXPECT_NE(text.find("SELECT 3"), String::npos);
}

TEST(QueryContextBuffer, EntryCountIsBounded)
{
    QueryContextBuffer buffer;
    for (size_t i = 0; i < QueryContextBuffer::max_entries + 5; ++i)
    {
        buffer.startQuery("SELECT " + std::to_string(i), false);
        buffer.finishQuery(0.1, false);
    }

    String text = buffer.format(0, false);
    EXPECT_EQ(text.find("Query: SELECT 0\n"), String::npos);
    EXPECT_NE(text.find("Query: SELECT " + std::to_string(QueryContextBuffer::max_entries + 4)), String::npos);
}

TEST(QueryContextBuffer, Cancellation)
{
    QueryContextBuffer buffer;
    buffer.startQuery("SELECT sleep(100)", false);
    buffer.finishQuery(5.0, true);

    String text = buffer.format(0, false);
    EXPECT_NE(text.find("cancelled"), String::npos);
}

TEST(QueryContextBuffer, FormatBlockAsText)
{
    String text = formatBlockAsTextForAI(makeBlock(0, 3));
    EXPECT_EQ(text, "n:UInt64\n0\n1\n2\n");

    /// Row cap with a truncation notice.
    text = formatBlockAsTextForAI(makeBlock(0, 500));
    EXPECT_NE(text.find("truncated: 200 of 500 rows"), String::npos);
}

#endif
