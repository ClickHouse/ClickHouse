#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessRightsElement.h>
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

    AsynchronousInsertQueue::InsertQuery key1(query, {}, {}, {}, {}, {}, {}, {}, settings1, kind);
    AsynchronousInsertQueue::InsertQuery key2(query, {}, {}, {}, {}, {}, {}, {}, settings2, kind);
    AsynchronousInsertQueue::InsertQuery key3(query, {}, {}, {}, {}, {}, {}, {}, settings3, kind);
    AsynchronousInsertQueue::InsertQuery key4(query, {}, {}, {}, {}, {}, {}, {}, settings4, kind);

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
            query, {}, {}, {}, {}, current_user, initial_user, authenticated_user, settings, kind);
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

TEST(AsyncInsertKey, AuthenticationGrantsAreDistinguished)
{
    String query_str = "INSERT INTO test (a, b, c) VALUES (1, 2, 3)";
    ParserInsertQuery parser(query_str.data() + query_str.size(), false);
    ASTPtr query = parseQuery(parser, query_str, DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    Settings settings;
    settings.set("async_insert", 1);

    auto kind = AsynchronousInsertQueueDataKind::Parsed;

    /// A fresh shared_ptr each call, so equal-content limits are always distinct instances: the key must
    /// compare them by content (like the hash), never by pointer identity.
    auto make_grants = [](std::string_view database, std::string_view table)
    {
        auto grants = std::make_shared<AccessRightsElements>();
        grants->emplace_back(AccessFlags(AccessType::SELECT), database, table);
        return std::shared_ptr<const AccessRightsElements>(std::move(grants));
    };

    auto make_key = [&](const std::shared_ptr<const AccessRightsElements> & grants)
    {
        return AsynchronousInsertQueue::InsertQuery(query, {}, {}, {}, grants, {}, {}, {}, settings, kind);
    };

    /// The credential grant limit (from the GRANTS clause of an authentication method) is part of the
    /// queue key, so inserts made under different limits are never coalesced into one flush and replayed
    /// under the wrong (possibly broader) rights.

    /// A limited credential (non-null grants) must never coalesce with an unrestricted insert (null grants).
    auto key_none = make_key(nullptr);
    auto key_t1 = make_key(make_grants("db", "t1"));
    EXPECT_NE(key_none.hash, key_t1.hash);
    EXPECT_NE(key_none, key_t1);

    /// Two different limits must never coalesce with each other.
    auto key_t2 = make_key(make_grants("db", "t2"));
    EXPECT_NE(key_t1.hash, key_t2.hash);
    EXPECT_NE(key_t1, key_t2);

    /// The same limit still coalesces (batching preserved for a single token), even though the two
    /// AccessRightsElements are distinct objects — the comparison is by content, not by pointer.
    auto key_t1_again = make_key(make_grants("db", "t1"));
    EXPECT_EQ(key_t1.hash, key_t1_again.hash);
    EXPECT_EQ(key_t1, key_t1_again);
}
