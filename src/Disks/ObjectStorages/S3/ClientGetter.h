#pragma once

#include <IO/S3Settings.h>
#include <IO/S3/Client.h>
#include <IO/S3/URI.h>
#include <Common/MultiVersion.h>

namespace DB
{

class IS3ClientGetter
{
public:

    virtual std::unique_ptr<S3::Client> makeClient(
        const std::string & endpoint,
        const S3::S3RequestSettings & request_settings,
        ContextPtr context,
        bool for_disk_s3) const = 0;

    virtual std::unique_ptr<S3::Client> makeClient(
        const S3::URI & url_,
        const S3::S3RequestSettings & request_settings,
        ContextPtr context,
        bool for_disk_s3) const = 0;

    virtual bool applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const S3::URI & uri,
        ContextPtr context) = 0;

    virtual bool isNoSignRequest() const = 0;

    virtual void addHeaders(const HTTPHeaderEntries & headers) = 0;

    virtual ~IS3ClientGetter() = default;
};


class S3ClientGetterFromAuthSettings : public IS3ClientGetter
{
public:
    explicit S3ClientGetterFromAuthSettings(const S3::S3AuthSettings & auth_settings_)
        : auth_settings(std::make_unique<S3::S3AuthSettings>(auth_settings_))
    {}

    std::unique_ptr<S3::Client> makeClient(
        const std::string & endpoint,
        const S3::S3RequestSettings & request_settings,
        ContextPtr context,
        bool for_disk_s3) const override;

    std::unique_ptr<S3::Client> makeClient(
        const S3::URI & url_,
        const S3::S3RequestSettings & request_settings,
        ContextPtr context,
        bool for_disk_s3) const override;

    bool applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const S3::URI & uri,
        ContextPtr context) override;

    bool isNoSignRequest() const override;

    void addHeaders(const HTTPHeaderEntries & headers) override;
private:
    MultiVersion<S3::S3AuthSettings> auth_settings;
};

using ClientGetterPtr = std::shared_ptr<IS3ClientGetter>;

}
