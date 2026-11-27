#include <gtest/gtest.h>

#include <Access/AuthenticationData.h>
#include <Common/Exception.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ParserCreateUserQuery.h>
#include <Parsers/parseQuery.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int INVALID_GRANT;
}

namespace
{

/// Extracts the authentication method of a parsed CREATE USER query and converts it with the given `validate`
/// flag. `validate = false` is the `ATTACH USER` path, taken when a user is loaded from an access storage.
AuthenticationData authenticationDataFromQuery(const String & query, bool validate)
{
    ParserCreateUserQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, 0, 0);
    const auto & create_query = ast->as<const ASTCreateUserQuery &>();
    return AuthenticationData::fromAST(*create_query.authentication_methods.front(), nullptr, validate);
}

}

/// An element of the auth-method GRANTS clause that the regular GRANT statement rejects as not grantable
/// must be rejected even on the ATTACH path (validate = false): the check is compatibility-insensitive
/// (the clause is a new feature, so no older server could have stored a non-grantable element), and
/// `AccessRights` would otherwise silently mask the flags out of the applied session limit, making the
/// stored clause diverge from the enforced one.
TEST(AuthenticationDataGrants, NonGrantableElementIsRejectedOnAttachPath)
{
    /// A global-level privilege listed on a table.
    try
    {
        authenticationDataFromQuery(
            "CREATE USER u IDENTIFIED WITH plaintext_password BY 'p' GRANTS (CREATE TEMPORARY TABLE ON db.t)",
            /*validate=*/ false);
        FAIL() << "Expected INVALID_GRANT";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INVALID_GRANT);
    }

    /// A global-level privilege listed on a database.
    try
    {
        authenticationDataFromQuery(
            "CREATE USER u IDENTIFIED WITH plaintext_password BY 'p' GRANTS (KILL QUERY ON db.*)",
            /*validate=*/ false);
        FAIL() << "Expected INVALID_GRANT";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INVALID_GRANT);
    }
}

TEST(AuthenticationDataGrants, GrantableElementIsAcceptedOnAttachPath)
{
    auto auth_data = authenticationDataFromQuery(
        "CREATE USER u IDENTIFIED WITH plaintext_password BY 'p' GRANTS (SELECT ON db.t)",
        /*validate=*/ false);
    EXPECT_FALSE(auth_data.getGrants().structurallyEmpty());

    /// The explicit deny-all clause has no flags to check and must stay accepted.
    auto deny_all = authenticationDataFromQuery(
        "CREATE USER u IDENTIFIED WITH plaintext_password BY 'p' GRANTS (USAGE ON *.*)",
        /*validate=*/ false);
    EXPECT_FALSE(deny_all.getGrants().structurallyEmpty());
}
