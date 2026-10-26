#include <gtest/gtest.h>
#include "config.h"

#if USE_CLIENT_AI

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Client/AI/QueryContextBuffer.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <Common/tests/gtest_global_register.h>

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
    buffer.recordError("SELECT bad", "Missing columns: 'bad'", false);
    buffer.finishQuery(0.1, false);

    /// A query that failed before start (e.g. a parse error).
    buffer.recordError("SELEC 1", "Syntax error", false);

    String text = buffer.format(0, false);
    EXPECT_NE(text.find("Error: Missing columns: 'bad'"), String::npos);
    EXPECT_NE(text.find("Query: SELEC 1"), String::npos);
    EXPECT_NE(text.find("Error: Syntax error"), String::npos);

    /// The first recorded error wins.
    buffer.startQuery("SELECT worse", false);
    buffer.recordError("SELECT worse", "first", false);
    buffer.recordError("SELECT worse", "second", false);
    buffer.finishQuery(0.1, false);
    text = buffer.format(0, false);
    EXPECT_NE(text.find("Error: first"), String::npos);
    EXPECT_EQ(text.find("Error: second"), String::npos);
}

TEST(QueryContextBuffer, StandaloneAIErrorsAreHiddenFromTheConversation)
{
    QueryContextBuffer buffer;

    /// A query of the AI agent that failed before an entry was opened (e.g. a parse error)
    /// must keep the AI-initiated flag: the model already saw the error as the tool result,
    /// and it must not be replayed into the conversation as if the user had typed it.
    buffer.recordError("SELEC 1", "Syntax error", /*from_ai=*/ true);

    EXPECT_EQ(buffer.format(0, /*skip_ai_initiated=*/ true), "");
    EXPECT_NE(buffer.format(0, /*skip_ai_initiated=*/ false).find("Query: SELEC 1"), String::npos);
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

TEST(QueryContextBuffer, DropFinishedEntriesKeepsTheRunningQuery)
{
    /// On a reconnect the queries of the previous session are forgotten, but the query that is
    /// running - the one that reconnected - stays, and a newer entry is still told from the old ones.
    QueryContextBuffer buffer;
    buffer.startQuery("SELECT old", false);
    buffer.finishQuery(0.1, false);
    const UInt64 seen = buffer.latestSeqno();
    buffer.startQuery("SELECT running", false);

    buffer.dropFinishedEntries();
    buffer.addBlock(makeBlock(0, 1));
    buffer.finishQuery(0.1, false);

    const String text = buffer.format(0, false);
    EXPECT_EQ(text.find("SELECT old"), String::npos);
    EXPECT_NE(text.find("SELECT running"), String::npos);
    EXPECT_GT(buffer.latestSeqno(), seen);

    buffer.dropFinishedEntries();
    EXPECT_TRUE(buffer.empty());
    EXPECT_GT(buffer.latestSeqno(), seen);
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


namespace
{

bool isValidUTF8(const String & text)
{
    size_t i = 0;
    while (i < text.size())
    {
        const auto c = static_cast<unsigned char>(text[i]);
        size_t length = 0;
        if (c < 0x80)
            length = 1;
        else if ((c & 0xE0) == 0xC0)
            length = 2;
        else if ((c & 0xF0) == 0xE0)
            length = 3;
        else if ((c & 0xF8) == 0xF0)
            length = 4;
        else
            return false;
        if (i + length > text.size())
            return false;
        for (size_t j = 1; j < length; ++j)
            if ((static_cast<unsigned char>(text[i + j]) & 0xC0) != 0x80)
                return false;
        i += length;
    }
    return true;
}

}

TEST(QueryContextBuffer, TruncationKeepsValidUTF8)
{
    /// The buffer is rendered into the prompt of the model, so a truncated query, row or error
    /// must not end in the middle of a multi-byte sequence.
    for (size_t extra = 0; extra < 4; ++extra)
    {
        /// "ё" is two bytes, so shifting the start by one byte moves the cut into the middle
        /// of a sequence; the same for the three-byte "…" below.
        String query(extra, 'x');
        while (query.size() < QueryContextBuffer::max_query_bytes + 16)
            query += extra % 2 ? "ё" : "…";

        QueryContextBuffer buffer;
        buffer.startQuery(query, false);
        buffer.finishQuery(0.1, false);
        const String text = buffer.format(0, false);
        EXPECT_NE(text.find("…"), String::npos);
        EXPECT_TRUE(isValidUTF8(text)) << "extra = " << extra;
    }
}

TEST(QueryContextBuffer, SampleOfAHugeValueIsBounded)
{
    /// One value of a million elements: the sample keeps the beginning of the line only, and the
    /// rest of the value is not serialized at all.
    auto nested = ColumnUInt64::create();
    for (size_t i = 0; i < 1'000'000; ++i)
        nested->insertValue(i);
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insertValue(1'000'000);
    auto array = ColumnArray::create(std::move(nested), std::move(offsets));
    auto string = ColumnString::create();
    string->insert(Field(String("after")));

    QueryContextBuffer buffer;
    buffer.startQuery("SELECT range(1000000), 'after'", false);
    buffer.addBlock(Block{
        ColumnWithTypeAndName{std::move(array), std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "a"},
        ColumnWithTypeAndName{std::move(string), std::make_shared<DataTypeString>(), "s"}});
    buffer.finishQuery(0.1, false);

    const String text = buffer.format(0, false);
    const size_t line_start = text.find("\n[0,1,2,3,");
    ASSERT_NE(line_start, String::npos);
    const size_t line_end = text.find('\n', line_start + 1);
    const String line = text.substr(line_start + 1, line_end == String::npos ? String::npos : line_end - line_start - 1);
    EXPECT_EQ(line.size(), QueryContextBuffer::max_line_bytes + String("…").size());
    EXPECT_TRUE(line.ends_with("…"));
    EXPECT_EQ(text.find("after\n"), String::npos);
}

TEST(QueryContextBuffer, AggregateFunctionStateIsNotSampled)
{
    /// The text of an aggregate function state is built whole before any of it is written, so the
    /// bounded sample cannot stop it: such a value is not serialized at all, and the columns after it
    /// are still sampled.
    tryRegisterAggregateFunctions();

    const DataTypes argument_types{std::make_shared<DataTypeUInt64>()};
    AggregateFunctionProperties properties;
    const auto function = AggregateFunctionFactory::instance().get("groupArray", NullsAction::EMPTY, argument_types, {}, properties);

    auto state = ColumnAggregateFunction::create(function);
    state->insertDefault();
    auto values = ColumnUInt64::create();
    for (size_t i = 0; i < 100'000; ++i)
        values->insertValue(i);
    const IColumn * arguments[] = {values.get()};
    for (size_t i = 0; i < values->size(); ++i)
        function->add(state->getData()[0], arguments, i, &state->createOrGetArena());

    auto string = ColumnString::create();
    string->insert(Field(String("after")));

    QueryContextBuffer buffer;
    buffer.startQuery("SELECT groupArrayState(number), 'after' FROM numbers(100000)", false);
    buffer.addBlock(Block{
        ColumnWithTypeAndName{std::move(state), std::make_shared<DataTypeAggregateFunction>(function, argument_types, Array{}), "a"},
        ColumnWithTypeAndName{std::move(string), std::make_shared<DataTypeString>(), "s"}});
    buffer.finishQuery(0.1, false);

    const String text = buffer.format(0, false);
    EXPECT_NE(text.find("[aggregate function state]\tafter"), String::npos) << text;
}

TEST(QueryContextBuffer, SampleOfAHugeJSONIsBounded)
{
    /// The escaped serialization of `JSON` renders the whole object before writing any of it, so a
    /// `JSON` column is sampled through its plain serialization, which the bounded sample stops, and
    /// a `JSON` value nested in another type is not sampled at all. The columns after it are.
    const auto type = DataTypeFactory::instance().get("JSON");
    auto json = type->createColumn();
    const String object = R"({"x":")" + String(1'000'000, 'x') + R"("})";
    ReadBufferFromString in(object);
    type->getDefaultSerialization()->deserializeWholeText(*json, in, FormatSettings{});

    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insertValue(1);
    auto array = ColumnArray::create(json->cloneResized(1), std::move(offsets));

    auto string = ColumnString::create();
    string->insert(Field(String("after")));

    QueryContextBuffer buffer;
    buffer.startQuery("SELECT j, [j], 'after'", false);
    buffer.addBlock(Block{
        ColumnWithTypeAndName{std::move(json), type, "j"},
        ColumnWithTypeAndName{std::move(array), std::make_shared<DataTypeArray>(type), "a"},
        ColumnWithTypeAndName{std::move(string), std::make_shared<DataTypeString>(), "s"}});
    buffer.finishQuery(0.1, false);

    const String text = buffer.format(0, false);
    const size_t line_start = text.find("\n{\"x\":\"xxx");
    ASSERT_NE(line_start, String::npos) << text;
    const size_t line_end = text.find('\n', line_start + 1);
    const String line = text.substr(line_start + 1, line_end == String::npos ? String::npos : line_end - line_start - 1);
    EXPECT_EQ(line.size(), QueryContextBuffer::max_line_bytes + String("…").size());
    EXPECT_EQ(text.find("after\n"), String::npos);

    /// A short object is sampled whole, and a nested one is not sampled.
    auto short_json = type->createColumn();
    const String short_object = R"({"a":1})";
    ReadBufferFromString short_in(short_object);
    type->getDefaultSerialization()->deserializeWholeText(*short_json, short_in, FormatSettings{});
    auto short_offsets = ColumnArray::ColumnOffsets::create();
    short_offsets->insertValue(1);
    auto short_array = ColumnArray::create(short_json->cloneResized(1), std::move(short_offsets));
    auto short_string = ColumnString::create();
    short_string->insert(Field(String("after")));

    QueryContextBuffer short_buffer;
    short_buffer.startQuery("SELECT j, [j], 'after'", false);
    short_buffer.addBlock(Block{
        ColumnWithTypeAndName{std::move(short_json), type, "j"},
        ColumnWithTypeAndName{std::move(short_array), std::make_shared<DataTypeArray>(type), "a"},
        ColumnWithTypeAndName{std::move(short_string), std::make_shared<DataTypeString>(), "s"}});
    short_buffer.finishQuery(0.1, false);

    const String short_text = short_buffer.format(0, false);
    EXPECT_NE(short_text.find("{\"a\":1}\t[value not sampled]\tafter"), String::npos) << short_text;
}

TEST(QueryContextBuffer, JSONWithSharedDataIsNotSampled)
{
    /// The plain serialization of `JSON` decodes a value of the shared data of the object (the paths
    /// over `max_dynamic_paths`) whole before writing any of it, so a `JSON` column that uses its
    /// shared data is not sampled. The columns after it are.
    const auto type = DataTypeFactory::instance().get("JSON(max_dynamic_paths=0)");
    auto json = type->createColumn();
    const String object = R"({"x":")" + String(1'000'000, 'x') + R"("})";
    ReadBufferFromString in(object);
    type->getDefaultSerialization()->deserializeWholeText(*json, in, FormatSettings{});
    ASSERT_FALSE(assert_cast<const ColumnObject &>(*json).getSharedDataNestedColumn().getData().empty());

    auto string = ColumnString::create();
    string->insert(Field(String("after")));

    QueryContextBuffer buffer;
    buffer.startQuery("SELECT j, 'after'", false);
    buffer.addBlock(Block{
        ColumnWithTypeAndName{std::move(json), type, "j"},
        ColumnWithTypeAndName{std::move(string), std::make_shared<DataTypeString>(), "s"}});
    buffer.finishQuery(0.1, false);

    const String text = buffer.format(0, false);
    EXPECT_NE(text.find("[value not sampled]\tafter"), String::npos) << text;
    EXPECT_EQ(text.find("xxx"), String::npos) << text;
}

TEST(QueryContextBuffer, SampleOfALineAtTheLimitIsNotCut)
{
    for (size_t size : {QueryContextBuffer::max_line_bytes, QueryContextBuffer::max_line_bytes + 1})
    {
        auto string = ColumnString::create();
        string->insert(Field(String(size, 'x')));

        QueryContextBuffer buffer;
        buffer.startQuery("SELECT x", false);
        buffer.addBlock(Block{ColumnWithTypeAndName{std::move(string), std::make_shared<DataTypeString>(), "s"}});
        buffer.finishQuery(0.1, false);

        const String text = buffer.format(0, false);
        const String expected = String(QueryContextBuffer::max_line_bytes, 'x') + (size > QueryContextBuffer::max_line_bytes ? "…\n" : "\n");
        EXPECT_NE(text.find("\n" + expected), String::npos) << "size = " << size;
    }
}

#endif
