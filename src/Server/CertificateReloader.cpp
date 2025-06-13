#include <Server/CertificateReloader.h>

#if USE_SSL

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/errnoToString.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/Utility.h>
#include <Server/ACME/Client.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{

/// Call set process for certificate.
int callSetCertificate(SSL * ssl, void * arg)
{
    if (!arg)
        return -1;

    const CertificateReloader::MultiData * pdata = reinterpret_cast<CertificateReloader::MultiData *>(arg);
    return CertificateReloader::instance().setCertificate(ssl, pdata);
}

}

/// This is callback for OpenSSL. It will be called on every connection to obtain a certificate and private key.
int CertificateReloader::setCertificate(SSL * ssl, const CertificateReloader::MultiData * pdata)
{
    auto current = pdata->data.get();

    if (!current)
        return -1;
    return setCertificateCallback(ssl, current.get(), log);
}

int setCertificateCallback(SSL * ssl, const CertificateReloader::Data * current_data, LoggerPtr log)
{
    if (current_data->certs_chain.empty())
        return -1;

    if (auto err = SSL_clear_chain_certs(ssl); err != 1)
    {
        LOG_ERROR(log, "Clear certificates {}", Poco::Net::Utility::getLastError());
        return -1;
    }

    const auto * root_certificate = static_cast<const X509 *>(current_data->certs_chain.front());
    if (auto err = SSL_use_certificate(ssl, const_cast<X509 *>(root_certificate)); err != 1)
    {
        LOG_ERROR(log, "Use certificate {}", Poco::Net::Utility::getLastError());
        return -1;
    }

    for (auto cert = current_data->certs_chain.begin() + 1; cert != current_data->certs_chain.end(); cert++)
    {
        const auto * certificate = static_cast<const X509 *>(*cert);
        if (auto err = SSL_add1_chain_cert(ssl, const_cast<X509 *>(certificate)); err != 1)
        {
            LOG_ERROR(log, "Add certificate to chain {}", Poco::Net::Utility::getLastError());
            return -1;
        }
    }

    if (auto err = SSL_use_PrivateKey(ssl, const_cast<EVP_PKEY *>(static_cast<const EVP_PKEY *>(current_data->key))); err != 1)
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


void CertificateReloader::init(MultiData * pdata)
{
    LOG_DEBUG(log, "Initializing certificate reloader.");

    /// Set a callback for OpenSSL to allow get the updated cert and key.
    SSL_CTX_set_cert_cb(pdata->ctx, callSetCertificate, reinterpret_cast<void *>(pdata));

    pdata->initialized = true;
}


void CertificateReloader::tryLoad(const Poco::Util::AbstractConfiguration & config)
{
    tryLoad(config, nullptr, Poco::Net::SSLManager::CFG_SERVER_PREFIX);
}


void CertificateReloader::tryLoadClient(const Poco::Util::AbstractConfiguration & config)
{
    tryLoad(config, nullptr, Poco::Net::SSLManager::CFG_CLIENT_PREFIX);
}


void CertificateReloader::tryLoad(const Poco::Util::AbstractConfiguration & config, SSL_CTX * ctx, const std::string & prefix)
{
    std::lock_guard lock{data_mutex};
    tryLoadImpl(config, ctx, prefix);
}


std::list<CertificateReloader::MultiData>::iterator CertificateReloader::findOrInsert(SSL_CTX * ctx, const std::string & prefix)
{
    auto it = data.end();
    auto i = data_index.find(prefix);
    if (i != data_index.end())
        it = i->second;
    else
    {
        if (!ctx)
        {
            if (prefix == Poco::Net::SSLManager::CFG_CLIENT_PREFIX)
                ctx = Poco::Net::SSLManager::instance().defaultClientContext()->sslContext();
            else
                ctx = Poco::Net::SSLManager::instance().defaultServerContext()->sslContext();
        }
        data.push_back(MultiData(ctx));
        --it;
        data_index[prefix] = it;
    }
    return it;
}

void CertificateReloader::tryLoadACMECertificate(SSL_CTX * ctx, const std::string & prefix)
{
    try
    {
        auto it = findOrInsert(ctx, prefix);
        if (!it->initialized)
            init(&*it);

        auto key_certificate_pair = ACME::Client::instance().requestCertificate();
        if (!key_certificate_pair)
        {
            LOG_WARNING(log, "ACME certificate is not ready yet.");
            return;
        }

        auto current_version = it->data.get();
        if (current_version && current_version->hash == key_certificate_pair->version)
            return;

        LOG_DEBUG(log, "Reloading ACME certificate and key.");
        it->data.set(std::make_unique<const Data>(
            std::move(key_certificate_pair->private_key),
            std::move(key_certificate_pair->certificate),
            key_certificate_pair->version
        ));
        LOG_INFO(log, "Reloaded ACME certificate and key.");
    }
    catch (...)
    {
        LOG_ERROR(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ false));
    }
}

void CertificateReloader::tryLoadImpl(const Poco::Util::AbstractConfiguration & config, SSL_CTX * ctx, const std::string & prefix)
{
    /// If at least one of the files is modified - recreate
    std::string new_cert_path = config.getString(prefix + "certificateFile", "");
    std::string new_key_path = config.getString(prefix + "privateKeyFile", "");

    if (config.has("acme") && prefix == Poco::Net::SSLManager::CFG_SERVER_PREFIX)
    {
        if (!new_cert_path.empty() || !new_key_path.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Static TLS keys and ACME provider are enabled at the same time.");

        tryLoadACMECertificate(ctx, prefix);

        return;
    }

    /// For empty paths (that means, that user doesn't want to use certificates)
    /// no processing required
    if (new_cert_path.empty() || new_key_path.empty())
    {
        LOG_INFO(log, "One of paths is empty. Cannot apply new configuration for certificates. Fill all paths and try again.");
        return;
    }

    try
    {
        auto it = findOrInsert(ctx, prefix);

        bool cert_file_changed = it->cert_file.changeIfModified(std::move(new_cert_path), log);
        bool key_file_changed = it->key_file.changeIfModified(std::move(new_key_path), log);

        if (cert_file_changed || key_file_changed)
        {
            LOG_DEBUG(log, "Reloading certificate ({}) and key ({}).", it->cert_file.path, it->key_file.path);

            std::string pass_phrase = config.getString(prefix + "privateKeyPassphraseHandler.options.password", "");
            it->data.set(std::make_unique<const Data>(it->cert_file.path, it->key_file.path, pass_phrase));

            LOG_INFO(log, "Reloaded certificate ({}) and key ({}).", it->cert_file.path, it->key_file.path);
        }

        /// If callback is not set yet
        if (!it->initialized)
            init(&*it);
    }
    catch (...)
    {
        LOG_ERROR(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ false));
    }
}


void CertificateReloader::tryReloadAll(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock{data_mutex};
    for (auto & item : data_index)
        tryLoadImpl(config, item.second->ctx, item.first);
}


CertificateReloader::Data::Data(std::string cert_path, std::string key_path, std::string pass_phrase)
    : certs_chain(X509Certificate::fromFile(cert_path)), key(KeyPair::fromFile(key_path, pass_phrase))
{
}

CertificateReloader::Data::Data(KeyPair _pkey, X509Certificate::List _certs_chain, std::string _hash)
    : certs_chain(std::move(_certs_chain)), key(std::move(_pkey)), hash(_hash)
{
}


bool CertificateReloader::File::changeIfModified(std::string new_path, LoggerPtr logger)
{
    std::error_code ec;
    std::filesystem::file_time_type new_modification_time = std::filesystem::last_write_time(new_path, ec);
    if (ec)
    {
        LOG_ERROR(
            logger,
            "Cannot obtain modification time for {} file {}, skipping update. {}",
            description,
            new_path,
            errnoToString(ec.value()));
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
