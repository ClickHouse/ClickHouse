#include <Server/CertificateReloader.h>

#if USE_SSL

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/errnoToString.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/Utility.h>


namespace DB
{

namespace
{
/// Call set process for certificate.
int callSetCertificate(SSL * ssl, [[maybe_unused]] void * arg)
{
    return CertificateReloader::instance().setCertificate(ssl);
}

}

/// This is callback for OpenSSL. It will be called on every connection to obtain a certificate and private key.
int CertificateReloader::setCertificate(SSL * ssl)
{
    auto current = data.get();
    if (!current)
        return -1;

    if (current->certs_chain.empty())
        return -1;

    if (auto err = SSL_clear_chain_certs(ssl); err != 1)
    {
        LOG_ERROR(log, "Clear certificates {}", Poco::Net::Utility::getLastError());
        return -1;
    }
    if (auto err = SSL_use_certificate(ssl, const_cast<X509 *>(current->certs_chain[0].certificate())); err != 1)
    {
        LOG_ERROR(log, "Use certificate {}", Poco::Net::Utility::getLastError());
        return -1;
    }
    for (auto cert = current->certs_chain.begin() + 1; cert != current->certs_chain.end(); cert++)
    {
        if (auto err = SSL_add1_chain_cert(ssl, const_cast<X509 *>(cert->certificate())); err != 1)
        {
            LOG_ERROR(log, "Add certificate to chain {}", Poco::Net::Utility::getLastError());
            return -1;
        }
    }
    if (auto err = SSL_use_PrivateKey(ssl, const_cast<EVP_PKEY *>(static_cast<const EVP_PKEY *>(current->key))); err != 1)
    {
        LOG_ERROR(log, "Use private key {}", Poco::Net::Utility::getLastError());
        return -1;
    }
    if (auto err = SSL_check_private_key(ssl); err != 1)
    {
        LOG_ERROR(log, "Unusable key-pair {}", Poco::Net::Utility::getLastError());
        return -1;
    }
    return 1;
}


void CertificateReloader::init()
{
    LOG_DEBUG(log, "Initializing certificate reloader.");

    /// Set a callback for OpenSSL to allow get the updated cert and key.

    auto* ctx = Poco::Net::SSLManager::instance().defaultServerContext()->sslContext();
    SSL_CTX_set_cert_cb(ctx, callSetCertificate, nullptr);
    init_was_not_made = false;
}


void CertificateReloader::tryLoad(const Poco::Util::AbstractConfiguration & config)
{
    /// If at least one of the files is modified - recreate

    std::string new_cert_path = config.getString("openSSL.server.certificateFile", "");
    std::string new_key_path = config.getString("openSSL.server.privateKeyFile", "");

    /// For empty paths (that means, that user doesn't want to use certificates)
    /// no processing required

    if (new_cert_path.empty() || new_key_path.empty())
    {
        LOG_INFO(log, "One of paths is empty. Cannot apply new configuration for certificates. Fill all paths and try again.");
    }
    else
    {
        bool cert_file_changed = cert_file.changeIfModified(std::move(new_cert_path), log);
        bool key_file_changed = key_file.changeIfModified(std::move(new_key_path), log);
        std::string pass_phrase = config.getString("openSSL.server.privateKeyPassphraseHandler.options.password", "");

        if (cert_file_changed || key_file_changed)
        {
            LOG_DEBUG(log, "Reloading certificate ({}) and key ({}).", cert_file.path, key_file.path);
            data.set(std::make_unique<const Data>(cert_file.path, key_file.path, pass_phrase));
            LOG_INFO(log, "Reloaded certificate ({}) and key ({}).", cert_file.path, key_file.path);
        }

        /// If callback is not set yet
        try
        {
            if (init_was_not_made)
                init();
        }
        catch (...)
        {
            init_was_not_made = true;
            LOG_ERROR(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ false));
        }
    }
}


CertificateReloader::Data::Data(std::string cert_path, std::string key_path, std::string pass_phrase)
    : certs_chain(Poco::Crypto::X509Certificate::readPEM(cert_path)), key(/* public key */ "", /* private key */ key_path, pass_phrase)
{
}


bool CertificateReloader::File::changeIfModified(std::string new_path, LoggerPtr logger)
{
    std::error_code ec;
    std::filesystem::file_time_type new_modification_time = std::filesystem::last_write_time(new_path, ec);
    if (ec)
    {
        LOG_ERROR(logger, "Cannot obtain modification time for {} file {}, skipping update. {}",
            description, new_path, errnoToString(ec.value()));
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
