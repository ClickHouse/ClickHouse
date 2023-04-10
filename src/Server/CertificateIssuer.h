#pragma once

#include "config.h"

#if USE_SSL

#include <atomic>
#include <functional>
#include <string>

#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <acme-lw.h>

namespace DB
{

class CertificateIssuer
{
public:
    /// Singleton class for issuing let's enrypt certificates
    CertificateIssuer(CertificateIssuer const &) = delete;
    void operator=(CertificateIssuer const &) = delete;
    static CertificateIssuer & instance()
    {
        static CertificateIssuer instance;
        return instance;
    }

    struct LetsEncryptConfigurationData
    {
        bool is_issuing_enabled;
        int reissue_hours_before;
        std::string domain_name;
        std::string account_private_key;
        std::string export_directory_path;

        std::string certificate_private_key_path;
        std::string certificate_path;

        LetsEncryptConfigurationData(const Poco::Util::AbstractConfiguration & config);
    };

    void UpdateCertificates(const LetsEncryptConfigurationData & config_data, std::function<void()> callback);

    void UpdateCertificatesIfNeeded(const Poco::Util::AbstractConfiguration & config);

    void PlaceFile(const std::string & domainName, const std::string & url, const std::string & keyAuthorization);

private:
    CertificateIssuer() = default;

    Poco::Logger * log = &Poco::Logger::get("CertificateIssuer");

    std::string export_directory_path;
};

}

#endif
