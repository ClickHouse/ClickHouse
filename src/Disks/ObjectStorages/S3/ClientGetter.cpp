#include <Disks/ObjectStorages/S3/ClientGetter.h>

#include <Disks/ObjectStorages/S3/diskSettings.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

namespace DB
{

std::unique_ptr<S3::Client> ClientGetterFromAuthSettings::makeClient(
    const std::string & endpoint,
    const S3::S3RequestSettings & request_settings,
    ContextPtr context,
    bool for_disk_s3) const
{
    return getClient(endpoint, request_settings, *auth_settings.get(), context, for_disk_s3);
}

std::unique_ptr<S3::Client> ClientGetterFromAuthSettings::makeClient(
    const S3::URI & url_,
    const S3::S3RequestSettings & request_settings,
    ContextPtr context,
    bool for_disk_s3) const
{
    return getClient(url_, request_settings, *auth_settings.get(), context, for_disk_s3);
}

bool ClientGetterFromAuthSettings::applyNewSettings(
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


}
