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

/// A PKCS#8 Ed25519 private key ("-----BEGIN PRIVATE KEY-----"), as produced by
/// `openssl genpkey -algorithm ED25519`. This carrier is NOT the OpenSSH container, so it must be
/// recognized via the id-Ed25519 OID in its AlgorithmIdentifier, not the OpenSSH header.
constexpr const char * ED25519_PKCS8_PRIVATE_KEY =
    "-----BEGIN PRIVATE KEY-----\n"
    "MC4CAQAwBQYDK2VwBCIEIMr2v6c+E0irtD2Qn+PtwYpgp6wbdYQFpboLcIfSjb8L\n"
    "-----END PRIVATE KEY-----\n";

/// An X.509 SubjectPublicKeyInfo Ed25519 public key ("-----BEGIN PUBLIC KEY-----"), as produced by
/// `openssl pkey -pubout`. Recognized via the id-Ed25519 OID, not the OpenSSH one-line format.
constexpr const char * ED25519_SPKI_PUBLIC_KEY =
    "-----BEGIN PUBLIC KEY-----\n"
    "MCowBQYDK2VwAyEAWTSDnELFOI3QfcsHITzMiajbo9mhOm/9Ev8/uf3eN14=\n"
    "-----END PUBLIC KEY-----\n";

/// A PKCS#8 RSA private key: usable in FIPS mode, and must not be mistaken for Ed25519.
constexpr const char * RSA_PKCS8_PRIVATE_KEY =
    "-----BEGIN PRIVATE KEY-----\n"
    "MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC8IywOSoJ21rv2\n"
    "9H50gjOWDDaB0Itn+DNKBpK0SgTMUspHI38iXT2uuFdmhnc+CT1VWsCdjXpb9MJn\n"
    "ArumnPRsqw/RTNUNn/5KuoztAOqA9VMqF0NOg3hw8PaRhW+PDmArTY7Wh/rdS4+b\n"
    "lIhvbGE4WX+71Ycx3m92PjX+wJYzINVoEd7AuJIQdNARcHdZNU8VkcQqkHH1nA9k\n"
    "h+5On4rW7BQc1RAnN6g8US/FyWrPreyJwFiWpgxb/1QjGDy1awOuz8HcQiMlZiR7\n"
    "gTvD+vybZTXiGagout07qrGJiOJtC29fEJ+76NO+CowkaGscGikkqzbsyXPYiyzo\n"
    "h847FEz7AgMBAAECggEAHBVop/t3AnP8cUPpine9vpS1IDyin/2nSPy2h84SwvNK\n"
    "zu1dYXenU6j1zE/0hctNn1ZMMViN9CMS+oXTFW8Rd2qI58Ep09OsWw2s0rC+e0s4\n"
    "7la0TTMygTLJrkr2WQBC2m6Z4gFAsSmXLBizIxAmal8bF6KrVduH1OU6xyaKuo0s\n"
    "XTXXM0jPuLQfzeu/1oisExYSkbUsjV45/pQXjL7exmy66QvaaKeyWV/e64p7nJS2\n"
    "gQ7c9mhho6tbej1LueSc/vVP75TeX69vqSNdYTe4giHnHlf2M6z9NRwEDBnt/0O4\n"
    "3zTvft/JLjsDQuSOtwF6hDIyPIZ8+Yy7cXPrn8YjxQKBgQDocg6c9FSzy5VAaaMe\n"
    "MASsrz2TyhJ7BJTJbO8mGvbAHoZDoe4SDZBCS2nB2gOoeKiVbVZoT7H/KjQRYigA\n"
    "jpntMBWEwBHkT9rHZgxAKEFqQhqAuf3uU8DpwTKmdPxUaeNTDoqBjpEnzhyF9l1q\n"
    "bOK0yViwEsfeo5gczs06n47f9QKBgQDPM7QWU0LbVnm6IBwENzCTKJ2nAgSmfYk9\n"
    "4KdXYagUlwvMvm5w2RJ5cFwRGriP3S55f2zNsE1d9its5SlIYV9b6cuzOTSah+HD\n"
    "IkLlcirBtn8y8y0t0J5kLIVVKftB0XqrANwNuOUa/Zs7dOO/Vy+Qb2IueJs2Zewf\n"
    "xlsZfhITLwKBgQCCyLnkIa0OxHsjlMxMjePzbV4OK2IeiUNV2c7s97rh+cq9fvy+\n"
    "d5c1Vc0ZXxszzT+9Y5l8c2rJEOve5AVx2spMpANXf+IRPcTw8D/wUTYNHUhgKUXw\n"
    "tHsUirl+4s+ehz16W/IeVyLg7J0r2qRQ3xbndWpLFyYR5KYPf2QFmgT/iQKBgQCY\n"
    "siiwHzHxBFx2rv0WmjmA6XitQA5/R5phcHLCMaS1b87xVxF6tm4n8obW0BNdagm6\n"
    "3g4n0xpg+W5tNMBkp+WnjbdhX/IFQ8g7jr8v6h/aXAyHVrBfyBNCDZFA/sw8aAz3\n"
    "go2oyy6D4ouImI7SToUt8veu+ZCg2SHXFM3EDqVxXwKBgQCvB7X/mYvXhQe2HK+l\n"
    "Ph9OkEf4klba10qEqIOhaBA5Ex9qwwtVFaWAYh9BZZfhSOc8ContOUNhc5lW3/GI\n"
    "+41em2hwE9a+BEiuSHXUsObpG3RosqA69/UpXlUiTdWrHtEmg7XmyrDUUwMCVMcU\n"
    "3tRLAwO3JmBvy9R82mxOeaGHdg==\n"
    "-----END PRIVATE KEY-----\n";

/// An X.509 SubjectPublicKeyInfo RSA public key: usable in FIPS mode.
constexpr const char * RSA_SPKI_PUBLIC_KEY =
    "-----BEGIN PUBLIC KEY-----\n"
    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvCMsDkqCdta79vR+dIIz\n"
    "lgw2gdCLZ/gzSgaStEoEzFLKRyN/Il09rrhXZoZ3Pgk9VVrAnY16W/TCZwK7ppz0\n"
    "bKsP0UzVDZ/+SrqM7QDqgPVTKhdDToN4cPD2kYVvjw5gK02O1of63UuPm5SIb2xh\n"
    "OFl/u9WHMd5vdj41/sCWMyDVaBHewLiSEHTQEXB3WTVPFZHEKpBx9ZwPZIfuTp+K\n"
    "1uwUHNUQJzeoPFEvxclqz63sicBYlqYMW/9UIxg8tWsDrs/B3EIjJWYke4E7w/r8\n"
    "m2U14hmoKLrdO6qxiYjibQtvXxCfu+jTvgqMJGhrHBopJKs27Mlz2Iss6IfOOxRM\n"
    "+wIDAQAB\n"
    "-----END PUBLIC KEY-----\n";

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
    (void)std::remove(path.c_str());
}

TEST(SSHKeyFIPSDetection, PublicKeyFileEd25519IsNotUsable)
{
    auto path = writeTempFile("ed_pub", ED25519_PUBLIC_KEY);
    EXPECT_FALSE(SSHKeyFactory::isPublicKeyFileUsableInFIPSBuilds(path));
    (void)std::remove(path.c_str());
}

TEST(SSHKeyFIPSDetection, PublicKeyFileRSAIsUsable)
{
    auto path = writeTempFile("rsa_pub", RSA_PUBLIC_KEY);
    EXPECT_TRUE(SSHKeyFactory::isPublicKeyFileUsableInFIPSBuilds(path));
    (void)std::remove(path.c_str());
}

/// A non-OpenSSH / unparsable file is reported as UNKNOWN, which is treated as usable
/// (not Ed25519): the subsequent libssh import decides validity for such keys.
TEST(SSHKeyFIPSDetection, NonOpenSSHFileIsUsable)
{
    auto path = writeTempFile("garbage", "not a key at all\n");
    EXPECT_TRUE(SSHKeyFactory::isPrivateKeyFileUsableInFIPSBuilds(path));
    (void)std::remove(path.c_str());
}

/// A PKCS#8 Ed25519 private key ("-----BEGIN PRIVATE KEY-----") is not the OpenSSH container, so it
/// must be recognized via the id-Ed25519 OID. This is the `openssl genpkey -algorithm ED25519`
/// carrier reachable through clickhouse-client --ssh-key-file.
TEST(SSHKeyFIPSDetection, PrivateKeyFileEd25519PKCS8IsNotUsable)
{
    auto path = writeTempFile("ed_pkcs8_priv", ED25519_PKCS8_PRIVATE_KEY);
    EXPECT_FALSE(SSHKeyFactory::isPrivateKeyFileUsableInFIPSBuilds(path));
    (void)std::remove(path.c_str());
}

/// An X.509 SPKI Ed25519 public key ("-----BEGIN PUBLIC KEY-----") is recognized via the OID.
TEST(SSHKeyFIPSDetection, PublicKeyFileEd25519SPKIIsNotUsable)
{
    auto path = writeTempFile("ed_spki_pub", ED25519_SPKI_PUBLIC_KEY);
    EXPECT_FALSE(SSHKeyFactory::isPublicKeyFileUsableInFIPSBuilds(path));
    (void)std::remove(path.c_str());
}

/// PKCS#8 / SPKI RSA keys carry a different OID and must remain usable in FIPS mode (they must not
/// be misclassified as Ed25519 by the OID scan).
TEST(SSHKeyFIPSDetection, PrivateKeyFileRSAPKCS8IsUsable)
{
    auto path = writeTempFile("rsa_pkcs8_priv", RSA_PKCS8_PRIVATE_KEY);
    EXPECT_TRUE(SSHKeyFactory::isPrivateKeyFileUsableInFIPSBuilds(path));
    (void)std::remove(path.c_str());
}

TEST(SSHKeyFIPSDetection, PublicKeyFileRSASPKIIsUsable)
{
    auto path = writeTempFile("rsa_spki_pub", RSA_SPKI_PUBLIC_KEY);
    EXPECT_TRUE(SSHKeyFactory::isPublicKeyFileUsableInFIPSBuilds(path));
    (void)std::remove(path.c_str());
}

#endif
