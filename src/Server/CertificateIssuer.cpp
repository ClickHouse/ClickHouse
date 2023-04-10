#include "CertificateIssuer.h"

#if USE_SSL

#include <Common/logger_useful.h>

namespace DB
{

void CertificateIssuer::UpdateCertificates(const LetsEncryptConfigurationData & config_data, std::function<void()> callback)
{
    LOG_ERROR(log, "AAAAAAAAAAAA\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nAAAAAAAAAAAA {}", config_data.reissue_hours_before);
    callback();

}

void CertificateIssuer::UpdateCertificatesIfNeeded(const Poco::Util::AbstractConfiguration & config){
    if (config.getBool("LetsEncrypt.enableAutomaticIssue", false))
        //TODO: Проверить что если серты есть и не устарели, то не перезапрашивать.
        // Иначе будешь перезапрашивать серты при каждом ребуте
        UpdateCertificates(LetsEncryptConfigurationData(config), [](){});
}

CertificateIssuer::LetsEncryptConfigurationData::LetsEncryptConfigurationData(const Poco::Util::AbstractConfiguration & config)
{
    is_issuing_enabled = config.getBool("LetsEncrypt.enableAutomaticIssue", false);
    reissue_hours_before = config.getInt("LetsEncrypt.reissueHoursBefore", 48);
}

}

#endif
