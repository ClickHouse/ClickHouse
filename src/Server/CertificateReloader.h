#pragma once

#include <Common/config.h>

#if USE_SSL

#include <string>
#include <filesystem>

#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <Poco/Crypto/RSAKey.h>
#include <Poco/Crypto/X509Certificate.h>
#include <Common/MultiVersion.h>


namespace DB
{

/// The CertificateReloader singleton performs 2 functions:
/// 1. Dynamic reloading of TLS key-pair when requested by server:
///   Server config reloader notifies CertificateReloader when the config changes.
///   On changed config, CertificateReloader reloads certs from disk.
/// 2. Implement `SSL_CTX_set_cert_cb` to set certificate for a new connection:
///   OpenSSL invokes a callback to setup a connection.
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

    /// Initialize the callback and perform the initial cert loading
    void init();

    /// Handle configuration reload
    void tryLoad(const Poco::Util::AbstractConfiguration & config);

    /// A callback for OpenSSL
    int setCertificate(SSL * ssl);

private:
    CertificateReloader() = default;

    Poco::Logger * log = &Poco::Logger::get("CertificateReloader");

    struct File
    {
        const char * description;
        explicit File(const char * description_) : description(description_) {}

        std::string path;
        std::filesystem::file_time_type modification_time;

        bool changeIfModified(std::string new_path, Poco::Logger * logger);
    };

    File cert_file{"certificate"};
    File key_file{"key"};

    struct Data
    {
        Poco::Crypto::X509Certificate cert;
        Poco::Crypto::EVPPKey key;

        Data(std::string cert_path, std::string key_path, std::string pass_phrase);
    };

    MultiVersion<Data> data;
    bool init_was_not_made = true;
};

}

#endif
