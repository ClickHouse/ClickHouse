#pragma once

#include <Core/Types.h>

namespace postgres
{

/// Optional TLS/SSL parameters passed to libpq via the connection string.
///
/// A certificate or a private key can be given in one of two forms:
///   - a path to a server-local file (`ssl_root_cert` / `ssl_cert` / `ssl_key`, the libpq
///     `sslrootcert` / `sslcert` / `sslkey` options). A path is only accepted from the server
///     configuration file (a named collection or a dictionary defined there): the server opens the
///     file with its own privileges, so a path taken from SQL would let anyone who can define a
///     PostgreSQL source probe the local filesystem and authenticate with a client certificate they
///     are not allowed to read themselves. See `StoragePostgreSQL::getSSLParams`.
///   - the literal contents of the file (`ssl_root_cert_pem` / `ssl_cert_pem` / `ssl_key_pem`),
///     acceptable from anywhere and masked in logs and `SHOW` queries like a password. libpq can
///     only load credentials from files, so the contents are materialized into a private temporary
///     file (see `TemporarySecretFile`) whose lifetime is tied to the `ConnectionInfo`.
///
/// For each credential at most one of the two forms may be used. Empty fields are omitted from the
/// connection string, so libpq keeps its own defaults (in particular, an empty `ssl_mode` leaves
/// libpq at `sslmode=prefer`).
struct ConnectionSSLParams
{
    String ssl_mode;           /// libpq `sslmode`: disable, allow, prefer, require, verify-ca or verify-full.

    String ssl_root_cert;      /// libpq `sslrootcert`: path to the CA certificate (or the special value `system`).
    String ssl_cert;           /// libpq `sslcert`: path to the client certificate.
    String ssl_key;            /// libpq `sslkey`: path to the client private key.

    String ssl_root_cert_pem;  /// Contents of the CA certificate file.
    String ssl_cert_pem;       /// Contents of the client certificate file.
    String ssl_key_pem;        /// Contents of the client private key file.
};

}
