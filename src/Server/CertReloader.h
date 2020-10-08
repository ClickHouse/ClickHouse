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
#if !USE_SSL
class CertReloader
{
public:
    /// Singleton
    CertReloader(CertReloader const &) = delete;
    void operator=(CertReloader const &) = delete;
    static CertReloader & instance()
    {
        static CertReloader instance;
        return instance;
    }

    /// Initialize the callback and perfom the initial cert loading
    void init([[maybe_unused]] const ::Poco::Util::AbstractConfiguration & config) { LOG_WARNING(log, "Not reloading (SSL is disabled)."); }

    /// Handle configuration reload
    void reload([[maybe_unused]] const Poco::Util::AbstractConfiguration & config){};

private:
    CertReloader() : log(&Poco::Logger::get("CertReloader")){};
    Poco::Logger * log;
};

#else

/// SSL_CTX_set_cert_cb function
extern "C" int cert_reloader_dispatch(SSL * ssl, void * arg);

/// The CertReloader singleton performs 2 functions:
/// 1. Dynamic reloading of TLS keypair when requested by main:
///   Main notifies CertReloader when the config changes. On changed config,
///   CertReloader reloads certs from disk.
/// 2. Implement `SSL_CTX_set_cert_cb` to set certificate for a new connection:
///   OpenSSL invokes `cert_reloader_dispatch_set_cert` to setup a connection.
class CertReloader
{
public:
    using stat_t = struct stat;

    /// Singleton
    CertReloader(CertReloader const &) = delete;
    void operator=(CertReloader const &) = delete;
    static CertReloader & instance()
    {
        static CertReloader instance;
        return instance;
    }

    /// Initialize the callback and perfom the initial cert loading
    void init(const ::Poco::Util::AbstractConfiguration & config);

    /// Handle configuration reload
    void reload(const Poco::Util::AbstractConfiguration & config);

    /// Add cert, key to SSL* connection. SetCert runs in an IO thread during
    /// connection setup. SetCert is
    /// establishing a new TLS connection.
    int SetCert(SSL * ssl);

private:
    CertReloader() : log(&Poco::Logger::get("CertReloader")), cert_file(""), cert(nullptr), key_file(""), key(nullptr) { }

    mutable std::shared_mutex lck;
    Poco::Logger * log;

    std::string cert_file;
    stat_t cert_file_st;
    std::unique_ptr<Poco::Crypto::X509Certificate> cert;
    bool setCertFile(std::string cert_file);

    std::string key_file;
    stat_t key_file_st;
    std::unique_ptr<Poco::Crypto::RSAKey> key;
    bool setKeyFile(std::string key_file);
};

#endif
}
