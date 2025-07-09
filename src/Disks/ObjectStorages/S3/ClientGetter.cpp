#include <Disks/ObjectStorages/S3/ClientGetter.h>

#include <Disks/ObjectStorages/S3/diskSettings.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <IO/S3AuthSettings.h>

namespace DB::S3AuthSetting
{
    extern const S3AuthSettingsBool no_sign_request;
}


namespace DB
{

std::unique_ptr<S3::Client> S3ClientGetterFromAuthSettings::makeClient(
    const std::string & endpoint,
    const S3::S3RequestSettings & request_settings,
    ContextPtr context,
    bool for_disk_s3) const
{
    return getClient(endpoint, request_settings, *auth_settings.get(), context, for_disk_s3);
}

std::unique_ptr<S3::Client> S3ClientGetterFromAuthSettings::makeClient(
    const S3::URI & url_,
    const S3::S3RequestSettings & request_settings,
    ContextPtr context,
    bool for_disk_s3) const
{
    return getClient(url_, request_settings, *auth_settings.get(), context, for_disk_s3);
}

bool S3ClientGetterFromAuthSettings::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const S3::URI & uri,
    ContextPtr context)
{
    auto modified_settings = *auth_settings.get();

    auto auth_settings_from_config = S3::S3AuthSettings(config, context->getSettingsRef(), config_prefix);

    modified_settings.updateIfChanged(auth_settings_from_config);

    if (auto endpoint_settings = context->getStorageS3Settings().getSettings(uri.uri.toString(), context->getUserName()))
        modified_settings.updateIfChanged(endpoint_settings->auth_settings);

    if (auth_settings.get()->hasUpdates(modified_settings))
    {
        auth_settings.set(std::make_unique<S3::S3AuthSettings>(std::move(modified_settings)));
        return true;
    }

    return false;
}

bool S3ClientGetterFromAuthSettings::isNoSignRequest() const
{
    return (*auth_settings.get())[S3AuthSetting::no_sign_request];
}

void S3ClientGetterFromAuthSettings::addHeaders(const HTTPHeaderEntries & headers)
{
    if (headers.empty())
        return;

    auto current_headers = auth_settings.get()->headers;
    bool new_header_found = false;
    for (const auto & header : headers)
    {
        auto header_searcher = [&](const HTTPHeaderEntry & entry)
        {
            return entry.name == header.name;
        };

        auto it = std::find_if(current_headers.begin(), current_headers.end(), header_searcher);
        if (it != current_headers.end())
        {
            if (it->value != header.value)
            {
                it->value = header.value;
                new_header_found = true;
            }

        }
        else
        {
            current_headers.emplace_back(header);
            new_header_found = true;
        }
    }

    if (new_header_found)
    {
        auto new_auth_settings = std::make_unique<S3::S3AuthSettings>(*auth_settings.get());
        new_auth_settings->headers = current_headers;
        auth_settings.set(std::move(new_auth_settings));
    }
}

}
