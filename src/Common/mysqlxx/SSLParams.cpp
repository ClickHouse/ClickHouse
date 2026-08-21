#include <mysqlxx/SSLParams.h>

#include <Common/TemporarySecretFile.h>

#include <Poco/Exception.h>


namespace mysqlxx
{

namespace
{
    /// Returns the path to use for one credential, materializing a temporary file when it was given
    /// as contents. A credential has exactly one source: the SQL surfaces validate that where the
    /// arguments are parsed, but the raw configuration of a dictionary source goes straight into
    /// `Pool` with no validation of its own, so the two forms together are rejected here — silently
    /// preferring one of them would leave an operator rotating the on-disk file while the pool stays
    /// pinned to the other credential.
    std::string resolve(
        const std::string & path,
        const std::string & pem,
        const char * path_key,
        const char * contents_key,
        std::shared_ptr<DB::TemporarySecretFile> & file)
    {
        if (pem.empty())
            return path;

        if (!path.empty())
            throw Poco::Exception(
                std::string("mysqlxx: `") + path_key + "` and `" + contents_key + "` cannot be specified at the same time");

        file = std::make_shared<DB::TemporarySecretFile>(pem);
        return file->getPath();
    }
}

ResolvedSSLPaths::ResolvedSSLPaths(const SSLParams & params)
{
    ca = resolve(params.ca_path, params.ca_pem, "ssl_ca", "ssl_ca_pem", ca_file);
    cert = resolve(params.cert_path, params.cert_pem, "ssl_cert", "ssl_cert_pem", cert_file);
    key = resolve(params.key_path, params.key_pem, "ssl_key", "ssl_key_pem", key_file);
}

}
