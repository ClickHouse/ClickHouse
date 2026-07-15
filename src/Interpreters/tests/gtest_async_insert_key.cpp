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

    AsynchronousInsertQueue::InsertQuery key1(query, {}, {}, {}, {}, {}, 0, settings1, kind);
    AsynchronousInsertQueue::InsertQuery key2(query, {}, {}, {}, {}, {}, 0, settings2, kind);
    AsynchronousInsertQueue::InsertQuery key3(query, {}, {}, {}, {}, {}, 0, settings3, kind);
    AsynchronousInsertQueue::InsertQuery key4(query, {}, {}, {}, {}, {}, 0, settings4, kind);

    EXPECT_EQ(key1, key2);
    EXPECT_NE(key1, key3);
    EXPECT_NE(key2, key3);
    EXPECT_NE(key3, key4);
}

TEST(AsyncInsertKey, IdentityHashIsNotAmbiguous)
{
    String query_str = "INSERT INTO test (a, b, c) VALUES (1, 2, 3)";
    ParserInsertQuery parser(query_str.data() + query_str.size(), false);
    ASTPtr query = parseQuery(parser, query_str, DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    Settings settings;
    settings.set("async_insert", 1);

    auto kind = AsynchronousInsertQueueDataKind::Parsed;

    auto make_key = [&](const String & current_user, const String & initial_user, const String & authenticated_user)
    {
        return AsynchronousInsertQueue::InsertQuery(
            query, {}, {}, current_user, initial_user, authenticated_user, 0, settings, kind);
    };

    /// The three identity fields are variable-length strings folded into the queue key hash,
    /// and the queue is indexed by hash alone. Without a length delimiter "a"/"a"/"aaa" and
    /// "aa"/"aa"/"a" stream the same bytes ("aaaaa") and collide, which would route a second
    /// user's insert into the first user's pending batch and flush it under the wrong
    /// ClientInfo. The keys must differ both by hash and by equality.
    auto key_a = make_key("a", "a", "aaa");
    auto key_b = make_key("aa", "aa", "a");
    EXPECT_NE(key_a.hash, key_b.hash);
    EXPECT_NE(key_a, key_b);

    /// A field boundary shift across the current/initial split must also be distinguished.
    auto key_c = make_key("a", "b", "");
    auto key_d = make_key("a", "", "b");
    EXPECT_NE(key_c.hash, key_d.hash);
    EXPECT_NE(key_c, key_d);

    /// Same identity still hashes the same and compares equal (batching preserved).
    auto key_e = make_key("alice", "alice", "alice");
    auto key_f = make_key("alice", "alice", "alice");
    EXPECT_EQ(key_e.hash, key_f.hash);
    EXPECT_EQ(key_e, key_f);

    /// Distinct identities never coalesce.
    auto key_g = make_key("alice", "alice", "alice");
    auto key_h = make_key("bob", "bob", "bob");
    EXPECT_NE(key_g.hash, key_h.hash);
    EXPECT_NE(key_g, key_h);
}

TEST(AsyncInsertKey, ClientProtocolVersion)
{
    auto parse = [](const String & query_str)
    {
        ParserInsertQuery parser(query_str.data() + query_str.size(), false);
        return parseQuery(parser, query_str, DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    };

    Settings settings;
    settings.set("async_insert", 1);

    using Kind = AsynchronousInsertQueueDataKind;
    auto make_key = [&](const ASTPtr & query, UInt64 version, Kind kind)
    {
        return AsynchronousInsertQueue::InsertQuery(query, {}, {}, {}, {}, {}, version, settings, kind);
    };

    ASTPtr native = parse("INSERT INTO test FORMAT Native");
    ASTPtr values = parse("INSERT INTO test (a, b, c) VALUES (1, 2, 3)");

    /// Parsed + revision-sensitive format (Native): the buffered bytes are reparsed at the request's
    /// protocol version on flush, so the version is part of the key and different revisions do not batch.
    {
        auto old_rev = make_key(native, 0, Kind::Parsed);
        auto new_rev = make_key(native, 54487, Kind::Parsed);
        EXPECT_NE(old_rev.hash, new_rev.hash);
        EXPECT_NE(old_rev, new_rev);
        EXPECT_EQ(new_rev, make_key(native, 54487, Kind::Parsed));
    }

    /// Preprocessed entries are already-materialized Blocks (for example the native TCP path) and are
    /// never reparsed, so the version must not fragment the batch even for a Native query.
    {
        auto old_rev = make_key(native, 0, Kind::Preprocessed);
        auto new_rev = make_key(native, 54487, Kind::Preprocessed);
        EXPECT_EQ(old_rev.hash, new_rev.hash);
        EXPECT_EQ(old_rev, new_rev);
    }

    /// Parsed but revision-insensitive format (VALUES): the parser ignores the version, so different
    /// revisions must still batch together.
    {
        auto old_rev = make_key(values, 0, Kind::Parsed);
        auto new_rev = make_key(values, 54487, Kind::Parsed);
        EXPECT_EQ(old_rev.hash, new_rev.hash);
        EXPECT_EQ(old_rev, new_rev);
    }
}
