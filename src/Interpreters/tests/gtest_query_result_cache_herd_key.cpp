#include <Interpreters/Cache/QueryResultCache.h>

#include <gtest/gtest.h>

using namespace DB;

/// `QueryResultCache::CoalescingKey` decides which concurrent queries coalesce on one in-flight computation for the
/// query result cache. Unlike the cache map key (`QueryResultCache::Key`, AST-only), it must separate
/// users when `query_cache_share_between_users` is off, mirroring the read-side access rules: a waiter
/// coalescing on another user's executor could never read that user's cache entry and would just block
/// for nothing. A stateless test cannot prove this contract via `query_cache_usage` counts (with sharing
/// off, only one user can own the single AST-keyed cache slot, so both a correct and an AST-only
/// regressed key produce the same observable counts), hence this direct unit test.

namespace
{

const IASTHash ast_hash_1{123456789, 987654321};
const IASTHash ast_hash_2{111111111, 222222222};

const UUID user_a = UUID(UInt128(1));
const UUID user_b = UUID(UInt128(2));
const UUID role_1 = UUID(UInt128(3));
const UUID role_2 = UUID(UInt128(4));

size_t hashOf(const QueryResultCache::CoalescingKey & key)
{
    return QueryResultCache::CoalescingKeyHasher()(key);
}

}

TEST(QueryResultCacheCoalescingKey, SeparatesUsersWhenSharingIsOff)
{
    /// Same AST, different users, sharing off: must not coalesce.
    QueryResultCache::CoalescingKey key_user_a{ast_hash_1, user_a, {}, /*share_between_users=*/ false};
    QueryResultCache::CoalescingKey key_user_b{ast_hash_1, user_b, {}, /*share_between_users=*/ false};
    EXPECT_FALSE(key_user_a == key_user_b);
    EXPECT_NE(hashOf(key_user_a), hashOf(key_user_b));

    /// Same AST and user but different roles: the reader checks roles too, so keys must differ.
    QueryResultCache::CoalescingKey key_role_1{ast_hash_1, user_a, {role_1}, /*share_between_users=*/ false};
    QueryResultCache::CoalescingKey key_role_2{ast_hash_1, user_a, {role_2}, /*share_between_users=*/ false};
    EXPECT_FALSE(key_role_1 == key_role_2);
    EXPECT_NE(hashOf(key_role_1), hashOf(key_role_2));

    /// A user-less (internal) query must not coalesce with a real user's query.
    QueryResultCache::CoalescingKey key_no_user{ast_hash_1, std::nullopt, {}, /*share_between_users=*/ false};
    EXPECT_FALSE(key_no_user == key_user_a);
    EXPECT_NE(hashOf(key_no_user), hashOf(key_user_a));

    /// Same AST, same user, same roles: must coalesce.
    QueryResultCache::CoalescingKey key_same_1{ast_hash_1, user_a, {role_1}, /*share_between_users=*/ false};
    QueryResultCache::CoalescingKey key_same_2{ast_hash_1, user_a, {role_1}, /*share_between_users=*/ false};
    EXPECT_TRUE(key_same_1 == key_same_2);
    EXPECT_EQ(hashOf(key_same_1), hashOf(key_same_2));
}

TEST(QueryResultCacheCoalescingKey, IgnoresUserWhenSharingIsOn)
{
    /// Same AST, different users, sharing on: must coalesce.
    QueryResultCache::CoalescingKey key_user_a{ast_hash_1, user_a, {role_1}, /*share_between_users=*/ true};
    QueryResultCache::CoalescingKey key_user_b{ast_hash_1, user_b, {role_2}, /*share_between_users=*/ true};
    EXPECT_TRUE(key_user_a == key_user_b);
    EXPECT_EQ(hashOf(key_user_a), hashOf(key_user_b));

    /// Both sides must agree on the sharing mode: a sharing key never matches a non-sharing key,
    /// even for the same AST and user.
    QueryResultCache::CoalescingKey key_shared{ast_hash_1, user_a, {}, /*share_between_users=*/ true};
    QueryResultCache::CoalescingKey key_not_shared{ast_hash_1, user_a, {}, /*share_between_users=*/ false};
    EXPECT_FALSE(key_shared == key_not_shared);
    EXPECT_NE(hashOf(key_shared), hashOf(key_not_shared));
}

TEST(QueryResultCacheCoalescingKey, SeparatesDifferentQueries)
{
    /// Different ASTs never coalesce, regardless of the sharing mode.
    QueryResultCache::CoalescingKey key_ast_1{ast_hash_1, user_a, {}, /*share_between_users=*/ true};
    QueryResultCache::CoalescingKey key_ast_2{ast_hash_2, user_a, {}, /*share_between_users=*/ true};
    EXPECT_FALSE(key_ast_1 == key_ast_2);
    EXPECT_NE(hashOf(key_ast_1), hashOf(key_ast_2));
}

TEST(QueryResultCacheCoalescingKey, SeparatesTopLevelAndSubqueryHerds)
{
    /// The top-level (`executeQuery`) cache and the Planner-level subquery cache store separate entries for the same
    /// AST, so their herds must be separate too: a top-level query coalescing on a subquery executor would wait for a
    /// computation that never fills the entry it is going to read (and vice versa).
    QueryResultCache::CoalescingKey key_top_level{ast_hash_1, user_a, {}, /*share_between_users=*/ false, /*is_subquery=*/ false};
    QueryResultCache::CoalescingKey key_subquery{ast_hash_1, user_a, {}, /*share_between_users=*/ false, /*is_subquery=*/ true};
    EXPECT_FALSE(key_top_level == key_subquery);
    EXPECT_NE(hashOf(key_top_level), hashOf(key_subquery));

    /// The separation also holds when users are shared.
    QueryResultCache::CoalescingKey key_top_level_shared{ast_hash_1, user_a, {}, /*share_between_users=*/ true, /*is_subquery=*/ false};
    QueryResultCache::CoalescingKey key_subquery_shared{ast_hash_1, user_b, {}, /*share_between_users=*/ true, /*is_subquery=*/ true};
    EXPECT_FALSE(key_top_level_shared == key_subquery_shared);
    EXPECT_NE(hashOf(key_top_level_shared), hashOf(key_subquery_shared));

    /// Two subqueries of the same shape still coalesce.
    QueryResultCache::CoalescingKey key_subquery_2{ast_hash_1, user_a, {}, /*share_between_users=*/ false, /*is_subquery=*/ true};
    EXPECT_TRUE(key_subquery == key_subquery_2);
    EXPECT_EQ(hashOf(key_subquery), hashOf(key_subquery_2));
}
