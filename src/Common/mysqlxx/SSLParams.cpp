#include <mysqlxx/SSLParams.h>

#include <Common/TemporarySecretFile.h>


namespace mysqlxx
{

namespace
{
    /// Returns the path to use for one credential, materializing a temporary file when it was given
    /// as contents. Only one of the two forms is set, this is validated where they are parsed.
    std::string resolve(const std::string & path, const std::string & pem, std::shared_ptr<DB::TemporarySecretFile> & file)
    {
        if (pem.empty())
            return path;

        file = std::make_shared<DB::TemporarySecretFile>(pem);
        return file->getPath();
    }
}

ResolvedSSLPaths::ResolvedSSLPaths(const SSLParams & params)
{
    ca = resolve(params.ca_path, params.ca_pem, ca_file);
    cert = resolve(params.cert_path, params.cert_pem, cert_file);
    key = resolve(params.key_path, params.key_pem, key_file);
}

}
