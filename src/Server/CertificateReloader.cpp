#include "CertificateReloader.h"

#if USE_SSL

#include <common/logger_useful.h>
#include <common/errnoToString.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/Utility.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_STAT;
}


/// This is callback for OpenSSL. It will be called on every connection to obtain a certificate and private key.
int CertificateReloader::setCertificate(SSL * ssl)
{
    auto current = data.get();
    if (!current)
        return -1;

    SSL_use_certificate(ssl, const_cast<X509 *>(current->cert.certificate()));
    SSL_use_RSAPrivateKey(ssl, current->key.impl()->getRSA());

    int err = SSL_check_private_key(ssl);
    if (err != 1)
    {
        std::string msg = Poco::Net::Utility::getLastError();
        LOG_ERROR(log, "Unusable key-pair {}", msg);
        return -1;
    }

    return 1;
}


void CertificateReloader::init(const Poco::Util::AbstractConfiguration & config)
{
    LOG_DEBUG(log, "Initializing certificate reloader.");

    reload(config);

    /// Set a callback for OpenSSL to allow get the updated cert and key.

    SSL_CTX_set_cert_cb(
        Poco::Net::SSLManager::instance().defaultClientContext()->sslContext(),
        [](SSL * ssl, void * arg) { return reinterpret_cast<CertificateReloader *>(arg)->setCertificate(ssl); },
        static_cast<void *>(this));
}


void CertificateReloader::reload(const Poco::Util::AbstractConfiguration & config)
{
    /// If at least one of the files is modified - recreate

    std::string new_cert_path = config.getString("openSSL.server.certificateFile", "");
    std::string new_key_path = config.getString("openSSL.server.privateKeyFile", "");

    if (cert_file.changeIfModified(std::move(new_cert_path), log)
        || key_file.changeIfModified(std::move(new_key_path), log))
    {
        LOG_DEBUG(log, "Reloading certificate ({}) and key ({}).", cert_file.path, key_file.path);
        data.set(std::make_unique<const Data>(cert_file.path, key_file.path));
        LOG_INFO(log, "Reloaded certificate ({}) and key ({}).", cert_file.path, key_file.path);
    }
}


CertificateReloader::Data::Data(std::string cert_path, std::string key_path)
    : cert(cert_path), key(/* public key */ "", /* private key */ key_path)
{
}


bool CertificateReloader::File::changeIfModified(std::string new_path, Poco::Logger * logger)
{
    std::error_code ec;
    std::filesystem::file_time_type new_modification_time = std::filesystem::last_write_time(new_path, ec);
    if (ec)
    {
        LOG_ERROR(logger, "Cannot obtain modification time for {} file {}, skipping update. {}",
            description, new_path, errnoToString(ErrorCodes::CANNOT_STAT, ec.value()));
        return false;
    }

    if (new_path != path || new_modification_time != modification_time)
    {
        path = new_path;
        modification_time = new_modification_time;
        return true;
    }

    return false;
}

}

#endif
