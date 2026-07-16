#include <Access/AuthenticationData.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/Access/ASTPublicSSHKey.h>
#include <Common/Exception.h>
#include <gtest/gtest.h>

#include "config.h"

#if USE_SSH

using namespace DB;

namespace
{
/// A syntactically valid RSA public key: usable in both FIPS and non-FIPS builds.
constexpr auto rsa_key_base64 =
    "AAAAB3NzaC1yc2EAAAADAQABAAABAQCYtXyCeWwmjokz6sCT5zg/TGbk4FEZF4huwXwPtXtnBlqw8/K/fIUjpfKmT8tgSziag"
    "qEfOClBfpHje8HvtQj8rSYsw6+OSV+qpEEVQy75GwTax3kH62+kZ/SxucWVLgYx6uZVZS6XnpK3H0i+azWU07keNRqIdvu9mcJ"
    "jnljNZAdm+OEgqrUYyCR1Bii1azI95rLvGdXJ/ZJz8WSgbCfSr4ME8GPEpIp8BSD097IrKtW0PuePrwH8BR+0rzLwKlxSKT1K1"
    "B3ktWO45bZ3QwHFo4BcdGkR6VpjUzugZJKK6uqnKFZ9e02EYidy6SKHPM1ifksWOprZdrtLYGkpW0tf";
constexpr auto ed25519_key_base64 = "AAAAC3NzaC1lZDI1NTE5AAAAIB0hPtHM5g5nB6+Yt6a6Txo1a6t7wYQ2v8b0eZQ2Z1kf";
}

/// toAST must re-emit keys preserved as "unusable" (Ed25519 under FIPS) alongside the usable ones, so a
/// persisted disk / ZooKeeper entity is rewritten without silently dropping keys. This exercises the
/// preserve path directly and is FIPS-independent.
TEST(SSHKeyPreserveUnusable, ToASTReEmitsPreservedKeys)
{
    AuthenticationData auth_data(AuthenticationType::SSH_KEY);
    std::vector<std::pair<String, String>> unusable{{ed25519_key_base64, "ssh-ed25519"}};
    auth_data.setUnusableSSHKeys(std::move(unusable));

    auto ast = auth_data.toAST();

    /// Even with no usable keys, the preserved key must still appear as a child so the method
    /// round-trips (it is not a zero-key, non-parseable "ssh_key BY").
    ASSERT_EQ(ast->children.size(), 1u);
    const auto & child = ast->children[0]->as<ASTPublicSSHKey &>();
    EXPECT_EQ(child.type, "ssh-ed25519");
    EXPECT_EQ(child.key_base64, ed25519_key_base64);
}

/// A mixed method (one preserved Ed25519 key + one usable RSA key) round-trips both keys through toAST,
/// so a FIPS rewrite of an ATTACH USER / disk / ZooKeeper entity keeps the full definition.
TEST(SSHKeyPreserveUnusable, ToASTReEmitsUsableAndPreservedKeys)
{
    ASTAuthenticationData source;
    source.type = AuthenticationType::SSH_KEY;
    source.children.push_back(make_intrusive<ASTPublicSSHKey>(rsa_key_base64, "ssh-rsa"));
    /// Materialize the RSA key through fromAST so it is a real, importable SSHKey.
    auto parsed = AuthenticationData::fromAST(source, nullptr, /* validate= */ false);
    ASSERT_TRUE(parsed.has_value());
    ASSERT_EQ(parsed->getSSHKeys().size(), 1u);

    std::vector<std::pair<String, String>> unusable{{ed25519_key_base64, "ssh-ed25519"}};
    parsed->setUnusableSSHKeys(std::move(unusable));

    auto ast = parsed->toAST();
    ASSERT_EQ(ast->children.size(), 2u);

    bool saw_rsa = false;
    bool saw_ed25519 = false;
    for (const auto & child_ast : ast->children)
    {
        const auto & key = child_ast->as<ASTPublicSSHKey &>();
        if (key.type == "ssh-rsa")
            saw_rsa = true;
        else if (key.type == "ssh-ed25519" && key.key_base64 == ed25519_key_base64)
            saw_ed25519 = true;
    }
    EXPECT_TRUE(saw_rsa);
    EXPECT_TRUE(saw_ed25519);
}

/// Preserved-but-unusable keys never take part in authentication: getSSHKeys() (the set actually used to
/// verify signatures / match public keys) excludes them.
TEST(SSHKeyPreserveUnusable, UnusableKeysAreNotUsedForAuthentication)
{
    AuthenticationData auth_data(AuthenticationType::SSH_KEY);
    std::vector<std::pair<String, String>> unusable{{ed25519_key_base64, "ssh-ed25519"}};
    auth_data.setUnusableSSHKeys(std::move(unusable));

    EXPECT_TRUE(auth_data.getSSHKeys().empty());
    EXPECT_EQ(auth_data.getUnusableSSHKeys().size(), 1u);
}

/// operator== must account for preserved keys, so an entity that lost its preserved Ed25519 key on
/// rewrite would not compare equal to the original (guards against a silent-drop regression).
TEST(SSHKeyPreserveUnusable, EqualityAccountsForPreservedKeys)
{
    AuthenticationData with_preserved(AuthenticationType::SSH_KEY);
    with_preserved.setUnusableSSHKeys({{ed25519_key_base64, "ssh-ed25519"}});

    AuthenticationData without_preserved(AuthenticationType::SSH_KEY);

    EXPECT_NE(with_preserved, without_preserved);

    AuthenticationData same(AuthenticationType::SSH_KEY);
    same.setUnusableSSHKeys({{ed25519_key_base64, "ssh-ed25519"}});
    EXPECT_EQ(with_preserved, same);
}

#endif
