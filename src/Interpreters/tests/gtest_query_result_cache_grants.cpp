#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Core/Settings.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/Context.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/tests/gtest_global_context.h>
#include <base/UUID.h>
#include <gtest/gtest.h>

using namespace DB;

/// The query-result cache isolates entries by the requesting session's privilege identity, which includes the
/// per-authentication-method GRANTS clause (a token-style credential limit). On a cache hit the reader only
/// re-compares the serialized `authentication_grants` string, so that string must be *precise*: the
/// backward-compatibility widening that `toString` applies (dropping the source filter when
/// `enable_read_write_grants` is off) would make two distinct source limits share a cache entry, letting the
/// narrower token read rows produced under the broader one. `QueryResultCache::Key` therefore serializes the
/// clause via `AccessRightsElements::toStringPrecise`.
TEST(QueryResultCacheGrants, KeyUsesPreciseAuthGrantSerialization)
{
    /// Force the shared global context to exist so the (non-precise) `toString` observes the server toggles;
    /// `enable_read_write_grants` defaults to false, which activates the widening that drops the filter.
    auto & access_control = getMutableContext().context->getAccessControl();
    ASSERT_FALSE(access_control.isEnabledReadWriteGrants());

    String query_str = "SELECT 1";
    ParserQuery parser(query_str.data() + query_str.size());
    ASTPtr ast = parseQuery(parser, query_str, DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    Settings settings;
    std::optional<UUID> user_id = UUID{};
    std::vector<UUID> roles;

    auto make_source_grant = [](AccessType type, std::string_view source, std::string_view filter)
    {
        auto grants = std::make_shared<AccessRightsElements>();
        AccessRightsElement element(AccessFlags(type), source);
        element.filter = String(filter);
        grants->emplace_back(std::move(element));
        return std::shared_ptr<const AccessRightsElements>(std::move(grants));
    };

    auto make_key = [&](const std::shared_ptr<const AccessRightsElements> & grants)
    {
        return QueryResultCache::Key(ast, /*current_database=*/ "default", settings, /*query_id=*/ "", user_id, roles, grants, /*is_subquery=*/ false);
    };

    /// Two credentials that differ only by the source filter: the widening `toString` drops the filter, so
    /// both stringify identically (the collision that would let the narrower token read the other's cache).
    auto read_a = make_source_grant(AccessType::READ, "S3", "bucket-a/.*");
    auto read_b = make_source_grant(AccessType::READ, "S3", "bucket-b/.*");
    ASSERT_EQ(read_a->toString(), read_b->toString());

    /// The cache key stores the precise serialization, so the two identities stay distinct and never share an
    /// entry (the reader compares `Key::authentication_grants`).
    auto key_a = make_key(read_a);
    auto key_b = make_key(read_b);
    EXPECT_EQ(key_a.authentication_grants, read_a->toStringPrecise());
    EXPECT_NE(key_a.authentication_grants, key_b.authentication_grants);

    /// A session with no clause has an empty identity, unchanged from before the feature, and never collides
    /// with a limited credential.
    auto key_none = make_key(nullptr);
    EXPECT_TRUE(key_none.authentication_grants.empty());
    EXPECT_NE(key_none.authentication_grants, key_a.authentication_grants);
}
