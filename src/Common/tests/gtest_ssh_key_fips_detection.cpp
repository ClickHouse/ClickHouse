#include <Common/SSHWrapper.h>
#include <gtest/gtest.h>

#include "config.h"

#if USE_SSH

#include <cstdio>
#include <fstream>
#include <string>

using namespace DB;

namespace
{
/// A real, freshly generated Ed25519 OpenSSH private key.
constexpr const char * ED25519_PRIVATE_KEY =
    "-----BEGIN OPENSSH PRIVATE KEY-----\n"
    "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\n"
    "QyNTUxOQAAACDN8Q939MFECh/m7JF/6ArtghcrRTpMaE5f3U7q6vEQngAAAJAiz269Is9u\n"
    "vQAAAAtzc2gtZWQyNTUxOQAAACDN8Q939MFECh/m7JF/6ArtghcrRTpMaE5f3U7q6vEQng\n"
    "AAAEAarNFAn/YihGfTPYc890uAcWzENmlFTDV6GBov+RZxPs3xD3f0wUQKH+bskX/oCu2C\n"
    "FytFOkxoTl/dTurq8RCeAAAAB2VkLXRlc3QBAgMEBQY=\n"
    "-----END OPENSSH PRIVATE KEY-----\n";

/// A real, freshly generated RSA OpenSSH private key.
constexpr const char * RSA_PUBLIC_KEY =
    "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCYtXyCeWwmjokz6sCT5zg/TGbk4FEZF4huwXwPtXtnBlqw8/K/fIUjpfKmT8tgSziag"
    "qEfOClBfpHje8HvtQj8rSYsw6+OSV+qpEEVQy75GwTax3kH62+kZ/SxucWVLgYx6uZVZS6XnpK3H0i+azWU07keNRqIdvu9mcJjnljNZ"
    "Adm+OEgqrUYyCR1Bii1azI95rLvGdXJ/ZJz8WSgbCfSr4ME8GPEpIp8BSD097IrKtW0PuePrwH8BR+0rzLwKlxSKT1K1B3ktWO45bZ3Q"
    "wHFo4BcdGkR6VpjUzugZJKK6uqnKFZ9e02EYidy6SKHPM1ifksWOprZdrtLYGkpW0tf rsa-test\n";

constexpr const char * ED25519_PUBLIC_KEY =
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIM3xD3f0wUQKH+bskX/oCu2CFytFOkxoTl/dTurq8RCe ed-test\n";

std::string writeTempFile(const std::string & suffix, const char * contents)
{
    std::string path = std::string("/tmp/ch_gtest_ssh_") + suffix + "_" + std::to_string(::getpid());
    std::ofstream out(path);
    out << contents;
    out.close();
    return path;
}
}

/// The FIPS key-type detection runs BEFORE any libssh import, so it can be exercised on non-FIPS
/// builds too: it just parses the file to determine the key type. This guards the pre-import
/// rejection added to makePrivateKeyFromFile / makePublicKeyFromFile that keeps an Ed25519 key from
/// ever reaching libssh under FIPS (the crash path via clickhouse-client --ssh-key-file).
TEST(SSHKeyFIPSDetection, PrivateKeyFileEd25519IsNotUsable)
{
    auto path = writeTempFile("ed_priv", ED25519_PRIVATE_KEY);
    EXPECT_FALSE(SSHKeyFactory::isPrivateKeyFileUsableInFIPSBuilds(path));
    std::remove(path.c_str());
}

TEST(SSHKeyFIPSDetection, PublicKeyFileEd25519IsNotUsable)
{
    auto path = writeTempFile("ed_pub", ED25519_PUBLIC_KEY);
    EXPECT_FALSE(SSHKeyFactory::isPublicKeyFileUsableInFIPSBuilds(path));
    std::remove(path.c_str());
}

TEST(SSHKeyFIPSDetection, PublicKeyFileRSAIsUsable)
{
    auto path = writeTempFile("rsa_pub", RSA_PUBLIC_KEY);
    EXPECT_TRUE(SSHKeyFactory::isPublicKeyFileUsableInFIPSBuilds(path));
    std::remove(path.c_str());
}

/// A non-OpenSSH / unparsable file is reported as UNKNOWN, which is treated as usable
/// (not Ed25519): the subsequent libssh import decides validity for such keys.
TEST(SSHKeyFIPSDetection, NonOpenSSHFileIsUsable)
{
    auto path = writeTempFile("garbage", "not a key at all\n");
    EXPECT_TRUE(SSHKeyFactory::isPrivateKeyFileUsableInFIPSBuilds(path));
    std::remove(path.c_str());
}

#endif
