#include "config.h"

#if USE_LIBPQXX

#include <gtest/gtest.h>

#include <Core/PostgreSQL/Utils.h>

using postgres::ConnectionSSLParams;

namespace
{
String connectionString(const ConnectionSSLParams & ssl_params)
{
    return postgres::formatConnectionString("db", "host", 5432, "user", "", 10, ssl_params).connection_string;
}

bool disablesClientCert(const ConnectionSSLParams & ssl_params)
{
    return connectionString(ssl_params).find("sslcertmode=disable") != String::npos;
}
}

/// Without an explicitly bound client credential, the connection string must stop libpq
/// from presenting the service account's ambient default certificate and key (~/.postgresql/postgresql.*),
/// so a user-chosen PostgreSQL connection cannot authenticate as the certificate role.
TEST(PostgresConnectionString, NoCredentialDisablesAmbientDefault)
{
    EXPECT_TRUE(disablesClientCert({}));

    /// A CA-only / sslmode configuration still has no client credential of its own.
    ConnectionSSLParams ca_only;
    ca_only.ssl_mode = "verify-full";
    ca_only.ssl_root_cert = "/etc/clickhouse/root.crt";
    EXPECT_TRUE(disablesClientCert(ca_only));
}

/// A certificate is public material: supplying it without the matching key must not let libpq pair it
/// with the ambient default key, and a key without a certificate is incomplete too. Either half missing
/// disables client-certificate authentication.
TEST(PostgresConnectionString, IncompleteClientCredentialIsNotUsed)
{
    ConnectionSSLParams cert_path_only;
    cert_path_only.ssl_cert = "/etc/clickhouse/client.crt";
    EXPECT_TRUE(disablesClientCert(cert_path_only));

    ConnectionSSLParams cert_pem_only;
    cert_pem_only.ssl_cert_pem = "-----BEGIN CERTIFICATE-----\nZHVtbXk=\n-----END CERTIFICATE-----\n";
    EXPECT_TRUE(disablesClientCert(cert_pem_only));

    ConnectionSSLParams key_only;
    key_only.ssl_key = "/etc/clickhouse/client.key";
    EXPECT_TRUE(disablesClientCert(key_only));
}

/// A complete, explicitly bound certificate and key (server-config paths, or PEM contents from anywhere)
/// is still presented, so operator-configured mTLS keeps working.
TEST(PostgresConnectionString, CompleteClientCredentialIsSent)
{
    ConnectionSSLParams from_paths;
    from_paths.ssl_cert = "/etc/clickhouse/client.crt";
    from_paths.ssl_key = "/etc/clickhouse/client.key";
    const auto with_paths = connectionString(from_paths);
    EXPECT_EQ(with_paths.find("sslcertmode=disable"), String::npos);
    EXPECT_NE(with_paths.find("sslcert='/etc/clickhouse/client.crt'"), String::npos);

    ConnectionSSLParams from_pems;
    from_pems.ssl_cert_pem = "-----BEGIN CERTIFICATE-----\nZHVtbXk=\n-----END CERTIFICATE-----\n";
    from_pems.ssl_key_pem = "-----BEGIN PRIVATE KEY-----\nZHVtbXk=\n-----END PRIVATE KEY-----\n";
    EXPECT_EQ(connectionString(from_pems).find("sslcertmode=disable"), String::npos);
}

#endif
