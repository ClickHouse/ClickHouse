#include <Access/AuthenticationData.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/Access/ASTPublicSSHKey.h>
#include <Common/Exception.h>
#include <gtest/gtest.h>

#include "config.h"

#if USE_SSH

using namespace DB;

/// An SSH_KEY authentication method with an empty key list is not round-trippable:
/// AuthenticationData::toAST emits "ssh_key BY" with zero keys, which ParserCreateUserQuery
/// rejects. Under FIPS mode every unsupported key can be filtered out, which would otherwise
/// leave exactly this state. fromAST must never materialize such a method:
///  - on the interactive path (validate == true) it throws;
///  - on the reload/ATTACH path (validate == false) it returns std::nullopt so the caller drops
///    only this method and keeps the user's remaining authentication methods.
TEST(SSHKeyEmptyAuth, FromASTRejectsEmptyKeyList)
{
    ASTAuthenticationData ast;
    ast.type = AuthenticationType::SSH_KEY;
    /// No ASTPublicSSHKey children: the resulting key list is empty.

    EXPECT_THROW(AuthenticationData::fromAST(ast, nullptr, /* validate= */ true), Exception);
    EXPECT_EQ(AuthenticationData::fromAST(ast, nullptr, /* validate= */ false), std::nullopt);
}

/// A non-empty SSH_KEY method still parses and stores its keys as before.
/// Use an RSA key: it is usable in both FIPS and non-FIPS builds, so the method is never
/// filtered down to empty and this positive expectation holds regardless of FIPS mode.
TEST(SSHKeyEmptyAuth, FromASTAcceptsNonEmptyKeyList)
{
    ASTAuthenticationData ast;
    ast.type = AuthenticationType::SSH_KEY;
    ast.children.push_back(make_intrusive<ASTPublicSSHKey>(
        "AAAAB3NzaC1yc2EAAAADAQABAAABAQCYtXyCeWwmjokz6sCT5zg/TGbk4FEZF4huwXwPtXtnBlqw8/K/fIUjpfKmT8tgSziag"
        "qEfOClBfpHje8HvtQj8rSYsw6+OSV+qpEEVQy75GwTax3kH62+kZ/SxucWVLgYx6uZVZS6XnpK3H0i+azWU07keNRqIdvu9mcJ"
        "jnljNZAdm+OEgqrUYyCR1Bii1azI95rLvGdXJ/ZJz8WSgbCfSr4ME8GPEpIp8BSD097IrKtW0PuePrwH8BR+0rzLwKlxSKT1K1"
        "B3ktWO45bZ3QwHFo4BcdGkR6VpjUzugZJKK6uqnKFZ9e02EYidy6SKHPM1ifksWOprZdrtLYGkpW0tf",
        "ssh-rsa"));

    auto auth_data = AuthenticationData::fromAST(ast, nullptr, /* validate= */ false);
    ASSERT_TRUE(auth_data.has_value());
    EXPECT_EQ(auth_data->getType(), AuthenticationType::SSH_KEY);
    EXPECT_EQ(auth_data->getSSHKeys().size(), 1u);
}

#endif
