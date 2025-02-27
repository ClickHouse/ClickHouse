#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/S3/Configuration.h>
#include "KernelHelper.h"
#include "KernelUtils.h"

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace DeltaLake
{

/// A helper class to manage S3-compatible storage types.
class S3KernelHelper final : public IKernelHelper
{
public:
    S3KernelHelper(
        const DB::S3::URI & url_,
        const std::string & access_key_id_,
        const std::string & secret_access_key_,
        const std::string & region_)
        : url(url_)
        , access_key_id(access_key_id_)
        , secret_access_key(secret_access_key_)
        , region(region_)
        , table_location(getTableLocation(url_))
    {
    }

    const std::string & getTableLocation() const override { return table_location; }

    const std::string & getDataPath() const override { return url.key; }

    ffi::EngineBuilder * createBuilder() const override
    {
        ffi::EngineBuilder * builder = KernelUtils::unwrapResult(
            ffi::get_engine_builder(
                KernelUtils::toDeltaString(table_location),
                &KernelUtils::allocateError),
            "get_engine_builder");

        auto set_option = [&](const std::string & name, const std::string & value)
        {
            ffi::set_builder_option(builder, KernelUtils::toDeltaString(name), KernelUtils::toDeltaString(value));
        };

        /// The delta-kernel-rs integration is currently under experimental flag,
        /// because we wait for delta-kernel maintainers to provide ffi api
        /// which will allow us to provide our own s3 client to delta-kernel.
        /// For now it uses its own client, which would lake all the auth options
        /// which our own client supports.

        set_option("aws_access_key_id", access_key_id);
        set_option("aws_secret_access_key", secret_access_key);
        set_option("aws_region", region);
        set_option("allow_http", "true");
        set_option("aws_endpoint", url.endpoint);

        return builder;
    }

private:
    const DB::S3::URI url;
    const std::string access_key_id;
    const std::string secret_access_key;
    const std::string region;
    const std::string table_location;

    static std::string getTableLocation(const DB::S3::URI & url)
    {
        return "s3://" + url.bucket + "/" + url.key;
    }
};

}

namespace DB
{

namespace S3AuthSetting
{
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsString secret_access_key;
    extern const S3AuthSettingsString region;
}

DeltaLake::KernelHelperPtr getKernelHelper(const StorageObjectStorage::ConfigurationPtr & configuration)
{
    switch (configuration->getType())
    {
        case DB::ObjectStorageType::S3:
        {
            const auto * s3_conf = dynamic_cast<const DB::StorageS3Configuration *>(configuration.get());
            const auto auth_settings = s3_conf->getAuthSettings();

            return std::make_shared<DeltaLake::S3KernelHelper>(
                s3_conf->getURL(),
                auth_settings[S3AuthSetting::access_key_id],
                auth_settings[S3AuthSetting::secret_access_key],
                auth_settings[S3AuthSetting::region]);
        }
        default:
        {
            throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                                "Unsupported storage type: {}", configuration->getType());
        }
    }
}

}

#endif
