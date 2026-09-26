#pragma once

#include <string>


namespace DB
{

/** Materializes the contents of a certificate or a private key into a private temporary file.
  *
  * Some client libraries can only load TLS credentials from the filesystem: `libpq` takes the
  * `sslrootcert`/`sslcert`/`sslkey` paths, and mariadb-connector-c only has `MYSQL_OPT_SSL_CA`,
  * `MYSQL_OPT_SSL_CERT` and `MYSQL_OPT_SSL_KEY`. To let a user provide credentials as literal PEM
  * contents (in a named collection or in a query) instead of a path to a server-local file, the
  * contents are written to a temporary file whose path is given to the client library.
  *
  * The file is created exclusively with mode 0600 and removed in the destructor. Mode 0600 is also
  * what `libpq` requires of a private key file: it refuses to use one that is group- or
  * world-accessible.
  *
  * The object must outlive every connection attempt made with these credentials, because client
  * libraries re-read the files when they reconnect.
  */
class TemporarySecretFile
{
public:
    explicit TemporarySecretFile(const std::string & contents);
    ~TemporarySecretFile();

    TemporarySecretFile(const TemporarySecretFile &) = delete;
    TemporarySecretFile & operator=(const TemporarySecretFile &) = delete;

    const std::string & getPath() const { return path; }

private:
    std::string path;
};

}
