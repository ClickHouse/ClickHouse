#include <Core/Settings.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/parseQuery.h>
#include <gtest/gtest.h>

using namespace DB;

TEST(AsyncInsertKey, SettingsChanges)
{
    String query_str = "INSERT INTO test (a, b, c) VALUES (1, 2, 3)";
    ParserInsertQuery parser(query_str.data() + query_str.size(), false);
    ASTPtr query = parseQuery(parser, query_str, DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    Settings settings1;
    Settings settings2;
    Settings settings3;
    Settings settings4;

    settings1.set("async_insert", 1);
    settings1.set("log_comment", "test1");
    settings1.set("insert_deduplication_token", "token1");

    settings2.set("async_insert", 1);
    settings2.set("log_comment", "test2");
    settings2.set("insert_deduplication_token", "token2");

    settings3.set("async_insert", 1);
    settings3.set("async_insert_busy_timeout", 100);
    settings3.set("log_comment", "test3");
    settings3.set("insert_deduplication_token", "token3");

    settings4.set("async_insert", 1);
    settings4.set("async_insert_busy_timeout", 200);
    settings4.set("log_comment", "test3");
    settings4.set("insert_deduplication_token", "token3");

    auto kind = AsynchronousInsertQueueDataKind::Parsed;

    AsynchronousInsertQueue::InsertQuery key1(query, {}, {}, settings1, kind);
    AsynchronousInsertQueue::InsertQuery key2(query, {}, {}, settings2, kind);
    AsynchronousInsertQueue::InsertQuery key3(query, {}, {}, settings3, kind);
    AsynchronousInsertQueue::InsertQuery key4(query, {}, {}, settings4, kind);

    EXPECT_EQ(key1, key2);
    EXPECT_NE(key1, key3);
    EXPECT_NE(key2, key3);
    EXPECT_NE(key3, key4);
}
