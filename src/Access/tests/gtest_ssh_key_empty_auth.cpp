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
/// leave exactly this state. fromAST must never materialize such a method; it rejects it on
/// both the interactive (validate == true) and reload/ATTACH (validate == false) paths.
TEST(SSHKeyEmptyAuth, FromASTRejectsEmptyKeyList)
{
    ASTAuthenticationData ast;
    ast.type = AuthenticationType::SSH_KEY;
    /// No ASTPublicSSHKey children: the resulting key list is empty.

    EXPECT_THROW(AuthenticationData::fromAST(ast, nullptr, /* validate= */ true), Exception);
    EXPECT_THROW(AuthenticationData::fromAST(ast, nullptr, /* validate= */ false), Exception);
}

/// A non-empty SSH_KEY method still parses and stores its keys as before.
TEST(SSHKeyEmptyAuth, FromASTAcceptsNonEmptyKeyList)
{
    ASTAuthenticationData ast;
    ast.type = AuthenticationType::SSH_KEY;
    /// A real ed25519 public key (base64 of the 32-byte key blob).
    ast.children.push_back(make_intrusive<ASTPublicSSHKey>(
        "AAAAC3NzaC1lZDI1NTE5AAAAICWLVGm05y2+pXSwkw4NNRoVKJtOflnwuYsuUhFx7VNs",
        "ssh-ed25519"));

    auto auth_data = AuthenticationData::fromAST(ast, nullptr, /* validate= */ false);
    EXPECT_EQ(auth_data.getType(), AuthenticationType::SSH_KEY);
    EXPECT_EQ(auth_data.getSSHKeys().size(), 1u);
}

#endif
