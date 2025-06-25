#pragma once
#include "config.h"

#if USE_AWS_S3

#include <IO/S3/Client.h>
#include <IO/S3RequestSettings.h>
#include <IO/S3AuthSettings.h>

namespace DB
{

class IS3ClientCreator
{
public:
    virtual std::unique_ptr<S3::Client> getClient(const std::string & endpoint, ContextPtr context, S3::S3RequestSettings request_settings, bool for_disk_s3);
    virtual std::unique_ptr<S3::Client> getClient(const S3::URI & url_, ContextPtr context, S3::S3RequestSettings request_settings, bool for_disk_s3) = 0;

    /// Return true if settings were changed.
    virtual bool applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const S3::URI & uri,
        ContextPtr context) = 0;

    virtual ~IS3ClientCreator() = default;
};

class S3ClientCreatorFromAuthSettings : public IS3ClientCreator
{
public:
    explicit S3ClientCreatorFromAuthSettings(
        const S3::S3AuthSettings & auth_settings_)
        : auth_settings(auth_settings_)
    {}

    std::unique_ptr<S3::Client> getClient(const std::string & endpoint, ContextPtr context, S3::S3RequestSettings request_settings, bool for_disk_s3) override;
    std::unique_ptr<S3::Client> getClient(const S3::URI & url_, ContextPtr context, S3::S3RequestSettings request_settings, bool for_disk_s3) override;

    bool applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const S3::URI & uri,
        ContextPtr context) override;

private:
    S3::S3AuthSettings auth_settings;

};

}

#endif
