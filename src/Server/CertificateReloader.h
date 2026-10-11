#pragma once

#include "config.h"

#if USE_SSL


#include <Common/MultiVersion.h>
#include <Common/Logger.h>
#include <Common/Crypto/KeyPair.h>
#include <Common/Crypto/X509Certificate.h>

#include <Poco/Logger.h>
#include <Poco/Net/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <openssl/x509v3.h>
#include <openssl/ssl.h>

#include <chrono>
#include <string>
#include <filesystem>
#include <list>
#include <optional>
#include <unordered_map>
#include <mutex>


namespace DB
{

/// The CertificateReloader singleton performs 3 functions:
/// 1. Dynamic reloading of TLS key-pair and of the trusted CA certificates (`caConfig`) when requested by server:
///   Server config reloader notifies CertificateReloader when the config changes.
///   On changed config, CertificateReloader reloads certs from disk.
/// 2. Implement `SSL_CTX_set_cert_cb` to set certificate for a new connection:
///   OpenSSL invokes a callback to setup a connection.
/// 3. Implement `SSL_CTX_set_cert_verify_callback` to verify the peer's certificate of a new connection
///   against the most recently loaded CA certificates.
///
/// An `SSL_CTX` that is shared between threads must not be modified, so instead of touching the contexts on reload,
/// both callbacks apply the current immutable snapshot (`MultiVersion`) to each new connection.
class CertificateReloader
{
public:
    using stat_t = struct stat;

    /// Owns a reference to a set of trusted CA certificates and remembers where they were loaded from.
    struct CAStore
    {
        CAStore(X509_STORE * store_, Poco::Net::Context::CAPaths paths_) : store(store_), paths(std::move(paths_)) {}
        CAStore(const CAStore &) = delete;
        CAStore & operator=(const CAStore &) = delete;
        ~CAStore() { X509_STORE_free(store); }

        X509_STORE * const store;
        const Poco::Net::Context::CAPaths paths;
    };

    struct Data
    {
        X509Certificate::List certs_chain;
        KeyPair key;

        const std::string hash;

        Data(std::string cert_path, std::string key_path, std::string pass_phrase);
        Data(KeyPair pkey, X509Certificate::List certs_chain, std::string hash);
    };

    struct File
    {
        const char * description;
        explicit File(const char * description_) : description(description_) {}

        std::string path;
        std::filesystem::file_time_type modification_time;
        /// For a directory: the names and modification times of the files in it.
        UInt64 directory_contents_hash = 0;

        bool changeIfModified(std::string new_path, LoggerPtr logger);
    };

    /// Trusted CA certificates from `caConfig` and, if `load_default_cas`, the default ones.
    struct CAData
    {
        MultiVersion<CAStore> store;
        bool load_default_cas = false;
    };

    struct MultiData
    {
        SSL_CTX * ctx = nullptr;
        MultiVersion<Data> data;
        bool initialized = false;

        File cert_file{"certificate"};
        File key_file{"key"};

        /// Empty if `caConfig` is not set for the prefix, then verification keeps using the store the context was created with.
        CAData ca;
        /// For additional contexts that assume another `loadDefaultCAFile` than Poco when it is not configured (the ones of Keeper).
        std::optional<CAData> ca_with_other_default;
        bool other_load_default_cas_default = false;

        File ca_file{"CA"};

        explicit MultiData(SSL_CTX * ctx_) : ctx(ctx_) {}
    };

    /// Singleton
    CertificateReloader(CertificateReloader const &) = delete;
    void operator=(CertificateReloader const &) = delete;
    /// Defined out of line: a static local in a header-defined function gives every shared
    /// object its own copy.
    static CertificateReloader & instance();

    /// Handle configuration reload for default path
    void tryLoad(const Poco::Util::AbstractConfiguration & config);

    /// Handle configuration reload client for default path
    void tryLoadClient(const Poco::Util::AbstractConfiguration & config);

    /// Handle configuration reload
    void tryLoad(const Poco::Util::AbstractConfiguration & config, SSL_CTX * ctx, const std::string & prefix);

    /// Register an additional SSL_CTX to share certificates and trusted CAs with the primary context of `prefix`.
    /// `load_default_cas_default` is what the caller assumes for `loadDefaultCAFile` when it is not configured.
    /// Returns true if the context will get its certificate and key from CertificateReloader,
    /// false if the caller has to configure them on the context itself.
    bool registerAdditionalContext(SSL_CTX * ctx, const std::string & prefix, bool load_default_cas_default);

    /// Handle configuration reload for all contexts
    void tryReloadAll(const Poco::Util::AbstractConfiguration & config);

    /// A callback for OpenSSL
    int setCertificate(SSL * ssl, const MultiData * pdata);

    /// A callback for OpenSSL: verify the peer certificate in `store_ctx` against the current CA certificates in `ca_store`.
    int verifyCertificate(X509_STORE_CTX * store_ctx, const MultiVersion<CAStore> * ca_store) const;

    /// The leaf certificate that is currently served for `prefix` connections, if there is one.
    /// It is not necessarily the certificate of the corresponding `SSL_CTX`: certificates are installed
    /// per connection, and with `<acme>` the context itself never gets a certificate at all.
    std::optional<X509Certificate> getCertificate(const std::string & prefix) const;

    /// Where the CA certificates that are currently used to verify peers of `prefix` connections were loaded from,
    /// if they are managed by CertificateReloader (i.e. `caConfig` is set for the prefix).
    std::optional<Poco::Net::Context::CAPaths> getCAPaths(const std::string & prefix) const;

private:
    CertificateReloader() = default;

    /// Initialize the callback and perform the initial cert loading
    void init(MultiData * pdata) TSA_REQUIRES(data_mutex);

    /// Unsafe implementation
    void tryLoadImpl(const Poco::Util::AbstractConfiguration & config, SSL_CTX * ctx, const std::string & prefix) TSA_REQUIRES(data_mutex);
    void tryLoadACMECertificate(SSL_CTX * ctx, const std::string & prefix) TSA_REQUIRES(data_mutex);
    void tryLoadCAImpl(const Poco::Util::AbstractConfiguration & config, SSL_CTX * ctx, const std::string & prefix) TSA_REQUIRES(data_mutex);

    std::list<MultiData>::iterator findOrInsert(SSL_CTX * ctx, const std::string & prefix) TSA_REQUIRES(data_mutex);

    LoggerPtr log = getLogger("CertificateReloader");

    std::list<MultiData> data TSA_GUARDED_BY(data_mutex);
    std::unordered_map<std::string, std::list<MultiData>::iterator> data_index TSA_GUARDED_BY(data_mutex);
    mutable std::mutex data_mutex;
};

/// A callback for OpenSSL
int setCertificateCallback(SSL * ssl, const CertificateReloader::Data * current_data, LoggerPtr log);

}

#endif
