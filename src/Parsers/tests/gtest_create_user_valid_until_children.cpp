#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ParserCreateUserQuery.h>
#include <Parsers/parseQuery.h>

#include <algorithm>

using namespace DB;

namespace
{

boost::intrusive_ptr<ASTCreateUserQuery> parseCreateUser(const String & query)
{
    ParserCreateUserQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, 0, 0);
    return boost::static_pointer_cast<ASTCreateUserQuery>(ast);
}

bool isChild(const IAST & parent, const ASTPtr & node)
{
    return std::any_of(
        parent.children.begin(), parent.children.end(), [&](const ASTPtr & child) { return child.get() == node.get(); });
}

}

/// The `VALID UNTIL` / `VALID FOR` deadline is stored both in a named member and in `children`,
/// so that the generic AST machinery (depth/size limits, clone-based visitors) sees the subtree.

TEST(CreateUserValidUntilAST, MethodLevelDeadlineIsRegisteredInChildren)
{
    auto query = parseCreateUser("CREATE USER u IDENTIFIED WITH no_password VALID FOR INTERVAL 1 DAY");
    const auto & method = query->authentication_methods.at(0);

    ASSERT_TRUE(method->valid_until);
    EXPECT_TRUE(isChild(*method, method->valid_until));
}

TEST(CreateUserValidUntilAST, CloneDeepCopiesMethodLevelDeadline)
{
    auto query = parseCreateUser("CREATE USER u IDENTIFIED WITH plaintext_password BY 'a' VALID FOR INTERVAL 1 DAY");
    auto cloned = boost::static_pointer_cast<ASTCreateUserQuery>(query->clone());
    const auto & original_method = query->authentication_methods.at(0);
    const auto & cloned_method = cloned->authentication_methods.at(0);

    ASSERT_TRUE(cloned_method->valid_until);
    /// The clone must own a deep copy of the deadline subtree, not share the original's.
    EXPECT_NE(cloned_method->valid_until.get(), original_method->valid_until.get());
    /// And the member must be rebound to the clone's own child.
    EXPECT_TRUE(isChild(*cloned_method, cloned_method->valid_until));
    EXPECT_FALSE(isChild(*cloned_method, original_method->valid_until));
}

TEST(CreateUserValidUntilAST, CloneDeepCopiesGlobalDeadline)
{
    auto query = parseCreateUser("CREATE USER u VALID FOR INTERVAL 1 DAY");
    auto cloned = boost::static_pointer_cast<ASTCreateUserQuery>(query->clone());

    ASSERT_TRUE(cloned->global_valid_until);
    EXPECT_NE(cloned->global_valid_until.get(), query->global_valid_until.get());
    EXPECT_TRUE(isChild(*cloned, cloned->global_valid_until));
    EXPECT_FALSE(isChild(*cloned, query->global_valid_until));
}

TEST(CreateUserValidUntilAST, MethodLevelDeadlineCountsTowardsASTDepthLimit)
{
    auto shallow = parseCreateUser("CREATE USER u IDENTIFIED WITH no_password VALID UNTIL '2035-01-01 00:00:00'");
    EXPECT_NO_THROW(shallow->checkDepth(6));

    auto deep = parseCreateUser(
        "CREATE USER u IDENTIFIED WITH no_password"
        " VALID FOR INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY + INTERVAL 1 DAY");
    /// Without the deadline registered in `children`, the whole expression subtree was
    /// invisible to `checkDepth` and a deeply nested `VALID FOR` bypassed `max_ast_depth`.
    EXPECT_THROW(deep->checkDepth(6), Exception);
}
