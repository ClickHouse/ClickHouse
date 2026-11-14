#pragma once

#include <memory>
#include <string>


namespace DB { class TemporarySecretFile; }

namespace mysqlxx
{

/** TLS/SSL credentials of a MySQL connection.
  *
  * The `*_path` fields are paths to files on the server. They may only be specified in the server
  * configuration file, because the server opens them with its own privileges: accepting a path from
  * SQL would let anyone who is able to define a MySQL source probe the local filesystem and use
  * credentials they are not allowed to read themselves.
  *
  * The `*_pem` fields carry the literal contents of the same files. They can be specified anywhere,
  * including named collections created with SQL and query arguments, and are masked in logs and in
  * `SHOW` queries like passwords are.
  *
  * For each of the three credentials at most one of the two forms is used; the contents win.
  */
struct SSLParams
{
    std::string ca_path;
    std::string cert_path;
    std::string key_path;

    std::string ca_pem;
    std::string cert_pem;
    std::string key_pem;

    bool empty() const
    {
        return ca_path.empty() && cert_path.empty() && key_path.empty() && ca_pem.empty() && cert_pem.empty() && key_pem.empty();
    }
};

/** The same credentials as file paths that can be handed to mariadb-connector-c, keeping the
  * temporary files that back `SSLParams::*_pem` alive: the client library re-reads the files every
  * time it connects, so they have to live at least as long as the connection pool does.
  */
class ResolvedSSLPaths
{
public:
    ResolvedSSLPaths() = default;

    /// Materializes the PEM contents into temporary files. Throws if they cannot be written.
    explicit ResolvedSSLPaths(const SSLParams & params);

    const std::string & getCA() const { return ca; }
    const std::string & getCert() const { return cert; }
    const std::string & getKey() const { return key; }

private:
    std::string ca;
    std::string cert;
    std::string key;

    /// Set only for the credentials that were given as contents rather than as a path.
    /// `shared_ptr` because a Pool (and with it its credentials) can be copied.
    std::shared_ptr<DB::TemporarySecretFile> ca_file;
    std::shared_ptr<DB::TemporarySecretFile> cert_file;
    std::shared_ptr<DB::TemporarySecretFile> key_file;
};

}
