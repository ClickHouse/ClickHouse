#include "CertificateIssuer.h"

#include <fmt/format.h>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#if USE_SSL

#include <Common/logger_useful.h>
#include <IO/copyData.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/Crypto/X509Certificate.h>
#include <Poco/Net/SSLException.h>

namespace DB
{

namespace
{
    const std::vector<std::string> config_names = {
            "LetsEncrypt.domainName", "file_system.base_directory",
            "openSSL.server.certificateFile", "openSSL.server.privateKeyFile",
            "LetsEncrypt.accountPrivateKeyFile"
    };

    void PlaceFileCall(const std::string & domainName, const std::string & url, const std::string & keyAuthorization)
    {
        CertificateIssuer::instance().PlaceFile(domainName, url, keyAuthorization);
    }

    void CheckConfiguration (const Poco::Util::AbstractConfiguration & config)
    {
        for (const auto & name : config_names)
        {
            if (!config.has(name))
            {
                throw Poco::Net::SSLException(fmt::format("Config must have {} for Let's Encrypt Integration", name));
            }
        }
    }
}

void CertificateIssuer::UpdateCertificates(const LetsEncryptConfigurationData & config_data, std::function<void()> callback)
{
    if (update_started.load())
        return;

    update_started = true;

    acme_lw::AcmeClient::init();
    acme_lw::AcmeClient client(config_data.account_private_key);
    const auto certificate = client.issueCertificate({config_data.domain_name}, PlaceFileCall);

    std::ofstream key_file(config_data.certificate_private_key_path);
    key_file << certificate.privkey;
    key_file.close();

    std::ofstream certificate_file(config_data.certificate_path);
    certificate_file << certificate.fullchain;
    certificate_file.close();

    callback();
    update_started = false;
}

bool CertificateIssuer::ShouldUpdateCertificates(const Poco::Util::AbstractConfiguration & config)
{
    if (!config.getBool("LetsEncrypt.enableAutomaticIssue", false))
        return false;

    const auto certificatePath = config.getString("openSSL.server.certificateFile", "");
    const auto keyPath = config.getString("openSSL.server.privateKeyFile", "");
    const auto reissue_hours_before = config.getInt("LetsEncrypt.reissueHoursBefore", 48);
    Poco::Crypto::X509Certificate certificate{certificatePath};
    if (std::filesystem::exists(certificatePath) && std::filesystem::exists(keyPath) &&
         certificate.expiresOn().timestamp()
            <= Poco::Timestamp() + Poco::Timespan(3600ll * reissue_hours_before, 0))
        return true;
    return false;
}

void CertificateIssuer::UpdateCertificatesIfNeeded(const Poco::Util::AbstractConfiguration & config)
{
    if (ShouldUpdateCertificates(config))
        UpdateCertificates(LetsEncryptConfigurationData(config), []() {});
}

CertificateIssuer::LetsEncryptConfigurationData::LetsEncryptConfigurationData(const Poco::Util::AbstractConfiguration & config)
{
    CheckConfiguration(config);
    reissue_hours_before = config.getInt("LetsEncrypt.reissueHoursBefore", 48);

    domain_name = config.getString("LetsEncrypt.domainName", "");
    export_directory_path = config.getString("file_system.base_directory", "/");

    certificate_private_key_path = config.getString("openSSL.server.certificateFile", "/");
    certificate_path = config.getString("openSSL.server.privateKeyFile", "/");

    DB::WriteBufferFromString out_buffer(account_private_key);
    DB::ReadBufferFromFile in_buffer(config.getString("LetsEncrypt.accountPrivateKeyFile", ""));
    DB::copyData(in_buffer, out_buffer);
}

void CertificateIssuer::PlaceFile(const std::string & domainName, const std::string & url, const std::string & keyAuthorization)
{
    std::ofstream out(export_directory_path + url.substr(7 + domainName.length()));
    out << keyAuthorization;
    out.close();
}

}

#endif
