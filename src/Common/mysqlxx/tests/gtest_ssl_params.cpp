#include <gtest/gtest.h>

#include "config.h"

#if USE_MYSQL

#include <fstream>

#include <mysqlxx/SSLParams.h>
#include <Poco/Exception.h>

/// The SQL surfaces validate that a credential has exactly one source where the arguments are
/// parsed, but the raw configuration of a dictionary source reaches `ResolvedSSLPaths` unvalidated,
/// so it rejects both forms of the same credential itself.
TEST(MySQLResolvedSSLPaths, PathAndContentsTogetherAreRejected)
{
    {
        mysqlxx::SSLParams params;
        params.ca_path = "/etc/ssl/certs/ca.crt";
        params.ca_pem = "not a certificate";
        EXPECT_THROW(mysqlxx::ResolvedSSLPaths{params}, Poco::Exception);
    }
    {
        mysqlxx::SSLParams params;
        params.cert_path = "/etc/ssl/certs/client.crt";
        params.cert_pem = "not a certificate";
        EXPECT_THROW(mysqlxx::ResolvedSSLPaths{params}, Poco::Exception);
    }
    {
        mysqlxx::SSLParams params;
        params.key_path = "/etc/ssl/private/client.key";
        params.key_pem = "not a key";
        EXPECT_THROW(mysqlxx::ResolvedSSLPaths{params}, Poco::Exception);
    }
}

/// A conflict of one credential is not excused by the other credentials being fine.
TEST(MySQLResolvedSSLPaths, ConflictAmongValidCredentialsIsRejected)
{
    mysqlxx::SSLParams params;
    params.ca_path = "/etc/ssl/certs/ca.crt";
    params.cert_pem = "not a certificate";
    params.key_path = "/etc/ssl/private/client.key";
    params.key_pem = "not a key";
    EXPECT_THROW(mysqlxx::ResolvedSSLPaths{params}, Poco::Exception);
}

TEST(MySQLResolvedSSLPaths, PathsPassThrough)
{
    mysqlxx::SSLParams params;
    params.ca_path = "/etc/ssl/certs/ca.crt";
    params.key_path = "/etc/ssl/private/client.key";

    mysqlxx::ResolvedSSLPaths resolved(params);
    EXPECT_EQ(resolved.getCA(), "/etc/ssl/certs/ca.crt");
    EXPECT_EQ(resolved.getCert(), "");
    EXPECT_EQ(resolved.getKey(), "/etc/ssl/private/client.key");
}

/// The passphrase is not a file form of the key: it must not change which path is handed to the
/// client library, and it must not be treated as a second source of the same credential.
TEST(MySQLResolvedSSLPaths, PassphraseDoesNotReplaceTheKey)
{
    mysqlxx::SSLParams params;
    params.key_path = "/etc/ssl/private/client.key";
    params.key_passphrase = "testpass-a";

    mysqlxx::ResolvedSSLPaths resolved(params);
    EXPECT_EQ(resolved.getKey(), "/etc/ssl/private/client.key");
}

/// An empty passphrase is the unencrypted-key case: it must not be treated as a second key source.
TEST(MySQLResolvedSSLPaths, EmptyPassphraseLeavesTheKeyUnchanged)
{
    mysqlxx::SSLParams params;
    params.key_pem = "not a key";
    params.key_passphrase = "";

    mysqlxx::ResolvedSSLPaths resolved(params);
    ASSERT_NE(resolved.getKey(), "");

    std::ifstream file(resolved.getKey());
    std::string contents((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    EXPECT_EQ(contents, params.key_pem);
}

TEST(MySQLResolvedSSLPaths, ContentsAreMaterialized)
{
    mysqlxx::SSLParams params;
    params.ca_pem = "the contents of the certificate";

    mysqlxx::ResolvedSSLPaths resolved(params);
    ASSERT_NE(resolved.getCA(), "");

    std::ifstream file(resolved.getCA());
    std::string contents((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    EXPECT_EQ(contents, params.ca_pem);
}

#endif
