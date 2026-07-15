#include <Access/User.h>
#include <Interpreters/Access/InterpreterCreateUserQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/Access/ASTPublicSSHKey.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <gtest/gtest.h>

#include "config.h"

#if USE_SSH

using namespace DB;

namespace
{
/// Build an ATTACH USER query AST with the given authentication methods.
/// attach == true makes InterpreterCreateUserQuery use validate == false (the reload/ATTACH path),
/// which is where an all-Ed25519 ssh_key method is filtered to empty under FIPS mode.
std::shared_ptr<ASTCreateUserQuery> makeAttachUserQuery(std::vector<boost::intrusive_ptr<ASTAuthenticationData>> methods)
{
    auto query = std::make_shared<ASTCreateUserQuery>();
    query->attach = true;
    query->names = make_intrusive<ASTUserNamesWithHost>("u");
    query->authentication_methods = std::move(methods);
    return query;
}

boost::intrusive_ptr<ASTAuthenticationData> makeEmptySSHKeyMethod()
{
    /// An SSH_KEY method with no ASTPublicSSHKey children models the state left after FIPS filtering
    /// removes every (Ed25519) key: fromAST(validate == false) drops it (returns nullopt). This is
    /// FIPS-independent, so the test exercises the same skip path on a non-FIPS build.
    auto data = make_intrusive<ASTAuthenticationData>();
    data->type = AuthenticationType::SSH_KEY;
    return data;
}

boost::intrusive_ptr<ASTAuthenticationData> makeSha256HashMethod()
{
    auto data = make_intrusive<ASTAuthenticationData>();
    data->type = AuthenticationType::SHA256_PASSWORD;
    data->contains_hash = true;
    /// A valid 64-hex-char SHA-256 hash (sha256 of "x").
    data->children.push_back(make_intrusive<ASTLiteral>(Field(String("2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"))));
    return data;
}
}

/// An ATTACH USER whose only authentication method is an ssh_key that filters to empty (all Ed25519
/// keys under FIPS) must be rejected as a whole (throw) rather than silently becoming a zero-key
/// SSH_KEY method or an implicit NO_PASSWORD user. The throw is caught per-entity by
/// deserializeAccessEntity so only this user is dropped during reload.
TEST(AttachUserMixedAuthFIPS, AllMethodsDroppedThrows)
{
    auto query = makeAttachUserQuery({makeEmptySSHKeyMethod()});
    User user;
    EXPECT_THROW(
        InterpreterCreateUserQuery::updateUserFromQuery(user, *query, /* allow_no_password= */ true, /* allow_plaintext_password= */ true, /* max_number_of_authentication_methods= */ 0),
        Exception);
}

/// An ATTACH USER with a still-valid sha256_password method AND an ssh_key method that filters to
/// empty must keep the password method: dropping only the offending method preserves the user.
TEST(AttachUserMixedAuthFIPS, SiblingMethodSurvivesDroppedSSHKey)
{
    auto query = makeAttachUserQuery({makeSha256HashMethod(), makeEmptySSHKeyMethod()});
    User user;
    ASSERT_NO_THROW(
        InterpreterCreateUserQuery::updateUserFromQuery(user, *query, /* allow_no_password= */ true, /* allow_plaintext_password= */ true, /* max_number_of_authentication_methods= */ 0));

    ASSERT_EQ(user.authentication_methods.size(), 1u);
    EXPECT_EQ(user.authentication_methods.front().getType(), AuthenticationType::SHA256_PASSWORD);
}

#endif
