#pragma once

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>

#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <common/logger_useful.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_SSL
#    include <openssl/ssl.h>
#    include <openssl/x509v3.h>
#    include <Poco/Crypto/RSAKey.h>
#    include <Poco/Crypto/X509Certificate.h>
#    include <Poco/Net/Context.h>
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/Utility.h>
#endif

namespace DB
{
#if USE_SSL

/// The CertificateReloader singleton performs 2 functions:
/// 1. Dynamic reloading of TLS keypair when requested by main:
///   Main notifies CertificateReloader when the config changes. On changed config,
///   CertificateReloader reloads certs from disk.
/// 2. Implement `SSL_CTX_set_cert_cb` to set certificate for a new connection:
///   OpenSSL invokes `cert_reloader_dispatch_set_cert` to setup a connection.
class CertificateReloader
{
public:
    using stat_t = struct stat;

    /// Singleton
    CertificateReloader(CertificateReloader const &) = delete;
    void operator=(CertificateReloader const &) = delete;
    static CertificateReloader & instance()
    {
        static CertificateReloader instance;
        return instance;
    }

    /// Initialize the callback and perfom the initial cert loading
    void init(const Poco::Util::AbstractConfiguration & config);

    /// Handle configuration reload
    void reload(const Poco::Util::AbstractConfiguration & config);

    /// Add cert, key to SSL* connection. SetCertificate runs in an IO thread during
    /// connection setup. SetCertificate is
    /// establishing a new TLS connection.
    int setCertificate(SSL * ssl);

private:
    CertificateReloader()
    {
    }

    mutable std::shared_mutex mutex;
    Poco::Logger * log = &Poco::Logger::get("CertificateReloader");

    std::string cert_file;
    stat_t cert_file_st;
    std::unique_ptr<Poco::Crypto::X509Certificate> cert;
    bool setCertificateFile(std::string cert_file);

    std::string key_file;
    stat_t key_file_st;
    std::unique_ptr<Poco::Crypto::RSAKey> key;
    bool setKeyFile(std::string key_file);
};

#else

class CertificateReloader
{
public:
    static CertificateReloader & instance()
    {
        static CertificateReloader instance;
        return instance;
    }

    void init(const Poco::Util::AbstractConfiguration &)
    {
    }

    void reload(const Poco::Util::AbstractConfiguration &)
    {
    }
};

#endif
}
