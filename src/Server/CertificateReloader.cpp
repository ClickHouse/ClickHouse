#include <Server/CertificateReloader.h>

#if USE_SSL

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <base/errnoToString.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/Utility.h>
#include <Server/ACME/Client.h>


namespace DB
{

CertificateReloader & CertificateReloader::instance()
{
    static CertificateReloader instance;
    return instance;
}

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int OPENSSL_ERROR;
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

/// Called by OpenSSL instead of `X509_verify_cert` to verify the peer's certificate.
int callVerifyCertificate(X509_STORE_CTX * store_ctx, void * arg)
{
    const MultiVersion<CertificateReloader::CAStore> * ca_store = reinterpret_cast<MultiVersion<CertificateReloader::CAStore> *>(arg);
    return CertificateReloader::instance().verifyCertificate(store_ctx, ca_store);
}

int defaultVerifyCertificate(X509_STORE_CTX * store_ctx)
{
    /// Same as libssl does when no callback is installed: an error is treated as a verification failure.
    int ok = X509_verify_cert(store_ctx);
    return ok < 0 ? 0 : ok;
}

/// Verify the certificate from `original_ctx` like `X509_verify_cert(original_ctx)` would, but against the trusted certificates in `store`.
/// libssl prepares `original_ctx` in `ssl_verify_internal` (ssl/ssl_cert.c) before calling the application callback.
/// The same preparation is carried over to a new `X509_STORE_CTX` that uses `store`, and the outcome is reported back
/// through `original_ctx`, because that is what libssl looks at after the callback returns.
int verifyCertificateWithStore(X509_STORE_CTX * original_ctx, X509 * certificate, X509_STORE * store)
{
    std::unique_ptr<X509_STORE_CTX, decltype(&X509_STORE_CTX_free)> verify_ctx(X509_STORE_CTX_new(), X509_STORE_CTX_free);
    if (!verify_ctx)
    {
        X509_STORE_CTX_set_error(original_ctx, X509_V_ERR_OUT_OF_MEM);
        return 0;
    }

    if (X509_STORE_CTX_init(verify_ctx.get(), store, certificate, X509_STORE_CTX_get0_untrusted(original_ctx)) != 1)
    {
        X509_STORE_CTX_set_error(original_ctx, X509_V_ERR_UNSPECIFIED);
        return 0;
    }

    /// The connection, for the per-connection verification callbacks (e.g. the ones of Poco and boost::asio) that look it up.
    int ssl_idx = SSL_get_ex_data_X509_STORE_CTX_idx();
    SSL * ssl = static_cast<SSL *>(X509_STORE_CTX_get_ex_data(original_ctx, ssl_idx));
    if (ssl)
    {
        if (X509_STORE_CTX_set_ex_data(verify_ctx.get(), ssl_idx, ssl) != 1)
        {
            X509_STORE_CTX_set_error(original_ctx, X509_V_ERR_UNSPECIFIED);
            return 0;
        }
        /// Has an effect only if DANE is enabled for the connection.
        X509_STORE_CTX_set0_dane(verify_ctx.get(), SSL_get0_dane(ssl));
    }

    /// The default purpose depends on which side is verified. Everything libssl derived from the connection and its `SSL_CTX`
    /// (verification depth, expected host name, security level, flags, ...) is already in the parameters of `original_ctx`.
    X509_STORE_CTX_set_default(verify_ctx.get(), (ssl && SSL_is_server(ssl)) ? "ssl_client" : "ssl_server");
    if (X509_VERIFY_PARAM_set1(X509_STORE_CTX_get0_param(verify_ctx.get()), X509_STORE_CTX_get0_param(original_ctx)) != 1)
    {
        X509_STORE_CTX_set_error(original_ctx, X509_V_ERR_UNSPECIFIED);
        return 0;
    }
    X509_STORE_CTX_set_verify_cb(verify_ctx.get(), X509_STORE_CTX_get_verify_cb(original_ctx));

    int ok = defaultVerifyCertificate(verify_ctx.get());

    X509_STORE_CTX_set_error(original_ctx, X509_STORE_CTX_get_error(verify_ctx.get()));
    X509_STORE_CTX_set_error_depth(original_ctx, X509_STORE_CTX_get_error_depth(verify_ctx.get()));
    if (STACK_OF(X509) * chain = X509_STORE_CTX_get1_chain(verify_ctx.get()))
        X509_STORE_CTX_set0_verified_chain(original_ctx, chain);
    X509_VERIFY_PARAM_move_peername(X509_STORE_CTX_get0_param(original_ctx), X509_STORE_CTX_get0_param(verify_ctx.get()));

    return ok;
}

/// Load the trusted CA certificates the same way `Poco::Net::Context` does it for the contexts created at startup,
/// so that a reload results in the same set of trusted certificates as a restart would.
std::unique_ptr<const CertificateReloader::CAStore> loadCAStore(const std::string & ca_path, bool load_default_cas)
{
    Poco::Net::Context::Params params;
    params.caLocation = ca_path;
    params.loadDefaultCAs = load_default_cas;
    /// Only the certificate store is taken from this context, so the usage does not matter.
    Poco::Net::Context context(Poco::Net::Context::CLIENT_USE, params);

    X509_STORE * store = SSL_CTX_get_cert_store(context.sslContext());
    if (!store || X509_STORE_up_ref(store) != 1)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Cannot get CA certificates from SSL context: {}", Poco::Net::Utility::getLastError());

    return std::make_unique<const CertificateReloader::CAStore>(store, context.getCAPaths());
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

/// This is callback for OpenSSL. It will be called on every connection that verifies the certificate of the peer.
int CertificateReloader::verifyCertificate(X509_STORE_CTX * store_ctx, const MultiVersion<CAStore> * ca_store) const
{
    try
    {
        auto current = ca_store->get();
        X509 * certificate = X509_STORE_CTX_get0_cert(store_ctx);

        /// Raw public keys (RFC 7250) are verified without CA certificates.
        if (!current || !certificate)
            return defaultVerifyCertificate(store_ctx);

        return verifyCertificateWithStore(store_ctx, certificate, current->store);
    }
    catch (...)
    {
        LOG_ERROR(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ false));
        X509_STORE_CTX_set_error(store_ctx, X509_V_ERR_UNSPECIFIED);
        return 0;
    }
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

        /// Verify peer certificates against the reloadable CA certificates of this prefix.
        /// Until (and unless) they are loaded, the callback does exactly what OpenSSL does without it.
        SSL_CTX_set_cert_verify_callback(ctx, callVerifyCertificate, reinterpret_cast<void *>(&it->ca.store));
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

void CertificateReloader::tryLoadCAImpl(const Poco::Util::AbstractConfiguration & config, SSL_CTX * ctx, const std::string & prefix)
{
    std::string new_ca_path = config.getString(prefix + Poco::Net::SSLManager::CFG_CA_LOCATION, "");

    /// Without `caConfig` the trusted certificates come only from the system locations (if at all), there is nothing to reload.
    /// But if `caConfig` was there before, keep following the configuration, like a restart would.
    if (new_ca_path.empty())
    {
        auto index_it = data_index.find(prefix);
        if (index_it == data_index.end() || index_it->second->ca_file.path.empty())
            return;
    }

    try
    {
        auto it = findOrInsert(ctx, prefix);
        bool ca_file_changed = it->ca_file.changeIfModified(std::move(new_ca_path), log);

        auto load = [&](CAData & ca, bool load_default_cas_default)
        {
            bool new_load_default_cas = config.getBool(prefix + Poco::Net::SSLManager::CFG_ENABLE_DEFAULT_CA, load_default_cas_default);
            if (!ca_file_changed && ca.store.get() && new_load_default_cas == ca.load_default_cas)
                return;

            LOG_DEBUG(log, "Reloading CA certificates ({}), load default CAs: {}.", it->ca_file.path, new_load_default_cas);
            ca.store.set(loadCAStore(it->ca_file.path, new_load_default_cas));
            ca.load_default_cas = new_load_default_cas;
            LOG_INFO(log, "Reloaded CA certificates ({}), load default CAs: {}.", it->ca_file.path, new_load_default_cas);
        };

        load(it->ca, Poco::Net::SSLManager::VAL_ENABLE_DEFAULT_CA);
        if (it->ca_with_other_default)
            load(*it->ca_with_other_default, it->other_load_default_cas_default);
    }
    catch (...)
    {
        LOG_ERROR(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ false));
    }
}

void CertificateReloader::tryLoadImpl(const Poco::Util::AbstractConfiguration & config, SSL_CTX * ctx, const std::string & prefix)
{
    /// Trusted CA certificates do not depend on how the own certificate is configured.
    tryLoadCAImpl(config, ctx, prefix);

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


bool CertificateReloader::registerAdditionalContext(SSL_CTX * ctx, const std::string & prefix, bool load_default_cas_default)
{
    if (!ctx)
        return false;

    std::lock_guard lock{data_mutex};

    auto it = data_index.find(prefix);
    if (it == data_index.end())
    {
        LOG_DEBUG(log, "Cannot register additional context for prefix '{}': prefix not found. "
            "This is expected when certificate/key paths are not configured for this prefix.", prefix);
        return false;
    }

    MultiData * pdata = &*(it->second);

    /// Share the reloadable CA certificates of this prefix (see `findOrInsert`). A context that assumes another default
    /// for `loadDefaultCAFile` may trust other CA certificates than the primary one, so it gets its own ones.
    /// They are loaded by the next `tryLoad`, until then the context keeps using the store it was created with.
    CAData * ca = &pdata->ca;
    if (load_default_cas_default != Poco::Net::SSLManager::VAL_ENABLE_DEFAULT_CA)
    {
        if (!pdata->ca_with_other_default)
        {
            pdata->ca_with_other_default.emplace();
            pdata->other_load_default_cas_default = load_default_cas_default;
        }
        ca = &*pdata->ca_with_other_default;
    }
    SSL_CTX_set_cert_verify_callback(ctx, callVerifyCertificate, reinterpret_cast<void *>(&ca->store));

    /// Verify that certificate data was actually loaded, not just the entry created.
    /// If data is null, return false so caller can use fallback (static cert loading).
    /// This can happen if initial cert parsing failed in tryLoadImpl or if only `caConfig` is set for the prefix.
    if (!pdata->data.get())
    {
        LOG_WARNING(log, "Cannot register additional context for prefix '{}': certificate data not loaded. "
            "Falling back to static certificate loading. Hot-reload will not work for this context.", prefix);
        return false;
    }

    SSL_CTX_set_cert_cb(ctx, callSetCertificate, reinterpret_cast<void *>(pdata));

    LOG_DEBUG(log, "Registered additional SSL context for prefix '{}'", prefix);
    return true;
}


std::optional<X509Certificate> CertificateReloader::getCertificate(const std::string & prefix) const
{
    std::lock_guard lock{data_mutex};

    auto it = data_index.find(prefix);
    if (it == data_index.end())
        return {};

    auto current = it->second->data.get();
    if (!current || current->certs_chain.empty())
        return {};

    /// `X509` is reference counted and immutable, so the certificate can be shared with the caller.
    X509 * leaf_certificate = static_cast<X509 *>(current->certs_chain.front());
    X509_up_ref(leaf_certificate);
    return X509Certificate(leaf_certificate);
}


std::optional<Poco::Net::Context::CAPaths> CertificateReloader::getCAPaths(const std::string & prefix) const
{
    std::lock_guard lock{data_mutex};

    auto it = data_index.find(prefix);
    if (it == data_index.end())
        return {};

    auto current = it->second->ca.store.get();
    if (!current)
        return {};

    return current->paths;
}


CertificateReloader::Data::Data(std::string cert_path, std::string key_path, std::string pass_phrase)
    : certs_chain(X509Certificate::fromFile(cert_path)), key(KeyPair::fromFile(key_path, pass_phrase))
{
}

CertificateReloader::Data::Data(KeyPair _pkey, X509Certificate::List _certs_chain, std::string _hash)
    : certs_chain(std::move(_certs_chain)), key(std::move(_pkey)), hash(std::move(_hash))
{
}


bool CertificateReloader::File::changeIfModified(std::string new_path, LoggerPtr logger)
{
    if (new_path.empty())
    {
        bool changed = !path.empty();
        path.clear();
        modification_time = {};
        return changed;
    }

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

    /// `caConfig` can be a directory with certificates, replacing one of them is a change too.
    UInt64 new_directory_contents_hash = 0;
    if (std::filesystem::is_directory(new_path, ec))
    {
        SipHash hash;
        for (const auto & entry : std::filesystem::directory_iterator(new_path, ec))
        {
            hash.update(entry.path().filename().string());
            hash.update(std::filesystem::last_write_time(entry.path(), ec).time_since_epoch().count());
        }
        new_directory_contents_hash = hash.get64();
    }

    if (new_path != path || new_modification_time != modification_time || new_directory_contents_hash != directory_contents_hash)
    {
        path = new_path;
        modification_time = new_modification_time;
        directory_contents_hash = new_directory_contents_hash;
        return true;
    }

    return false;
}

}

#endif
