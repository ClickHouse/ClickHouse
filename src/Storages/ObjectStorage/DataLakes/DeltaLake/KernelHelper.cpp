#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include "KernelHelper.h"
#include "KernelUtils.h"
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace DB::S3AuthSetting
{
    extern const S3AuthSettingsBool no_sign_request;
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
        const std::string & region_,
        const std::string & token_,
        bool no_sign_)
        : url(url_)
        , access_key_id(access_key_id_)
        , secret_access_key(secret_access_key_)
        , region(region_)
        , token(token_)
        , no_sign(no_sign_)
        , table_location(getTableLocation(url_))
    {
        /// Check if user didn't mention any region.
        /// Same as in S3/Client.cpp (stripping len("https://s3.")).
        if (url.endpoint.substr(11) == "amazonaws.com")
            url.addRegionToURI(region);
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

        /// Supported options
        /// https://github.com/apache/arrow-rs/blob/main/object_store/src/aws/builder.rs#L191
        if (!access_key_id.empty())
            set_option("aws_access_key_id", access_key_id);
        if (!secret_access_key.empty())
            set_option("aws_secret_access_key", secret_access_key);

        /// Set even if token is empty to prevent delta-kernel
        /// from trying to access token api.
        set_option("aws_token", token);

        if (no_sign || (access_key_id.empty() && secret_access_key.empty()))
            set_option("aws_skip_signature", "true");

        if (!region.empty())
            set_option("aws_region", region);

        set_option("aws_bucket", url.bucket);

        if (url.uri_str.starts_with("http"))
        {
            set_option("allow_http", "true");
            set_option("aws_endpoint", url.endpoint);
        }

        LOG_TRACE(
            getLogger("KernelHelper"),
            "Using endpoint: {}, uri: {}, region: {}, bucket: {}",
            url.endpoint, url.uri_str, region, url.bucket);

        return builder;
    }

private:
    DB::S3::URI url;
    const std::string access_key_id;
    const std::string secret_access_key;
    const std::string region;
    const std::string token;
    const bool no_sign;

    const std::string table_location;

    static std::string getTableLocation(const DB::S3::URI & url)
    {
        return "s3://" + url.bucket + "/" + url.key;
    }
};

/// A helper class to manage local fs storage.
class LocalKernelHelper final : public IKernelHelper
{
public:
    explicit LocalKernelHelper(const std::string & path_) : table_location(getTableLocation(path_)), path(path_) {}

    const std::string & getTableLocation() const override { return table_location; }

    const std::string & getDataPath() const override { return path; }

    ffi::EngineBuilder * createBuilder() const override
    {
        ffi::EngineBuilder * builder = KernelUtils::unwrapResult(
            ffi::get_engine_builder(
                KernelUtils::toDeltaString(table_location),
                &KernelUtils::allocateError),
            "get_engine_builder");

        return builder;
    }

private:
    const std::string table_location;
    const std::string path;

    static std::string getTableLocation(const std::string & path)
    {
        return "file://" + path + "/";
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

DeltaLake::KernelHelperPtr getKernelHelper(
    const StorageObjectStorage::ConfigurationPtr & configuration,
    const ObjectStoragePtr & object_storage)
{
    switch (configuration->getType())
    {
        case DB::ObjectStorageType::S3:
        {
            const auto * s3_conf = dynamic_cast<const DB::StorageS3Configuration *>(configuration.get());
            const auto & auth_settings = s3_conf->getAuthSettings();
            const auto & s3_client = object_storage->getS3StorageClient();
            const auto & s3_credentials = s3_client->getCredentials();
            const auto & url = s3_conf->getURL();

            auto region = s3_client->getRegion();
            if (region.empty() || region == Aws::Region::AWS_GLOBAL)
                region = s3_client->getRegionForBucket(url.bucket, /* force_detect */true);

            return std::make_shared<DeltaLake::S3KernelHelper>(
                url,
                s3_credentials.GetAWSAccessKeyId(),
                s3_credentials.GetAWSSecretKey(),
                std::move(region),
                s3_credentials.GetSessionToken(),
                auth_settings[S3AuthSetting::no_sign_request]);
        }
        case DB::ObjectStorageType::Local:
        {
            const auto * local_conf = dynamic_cast<const DB::StorageLocalConfiguration *>(configuration.get());
            return std::make_shared<DeltaLake::LocalKernelHelper>(local_conf->getPath());
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
