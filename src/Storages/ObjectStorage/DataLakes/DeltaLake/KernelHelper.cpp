#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Common/logger_useful.h>

#if USE_AZURE_BLOB_STORAGE
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <azure/storage/common/storage_credential.hpp>
#include <azure/identity/client_secret_credential.hpp>
#include <azure/identity/workload_identity_credential.hpp>
#include <azure/identity/managed_identity_credential.hpp>
#endif


namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
#if USE_AZURE_BLOB_STORAGE
    extern const int BAD_ARGUMENTS;
#endif
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
        std::shared_ptr<const DB::S3::Client> client_,
        const DB::S3::S3AuthSettings & auth_settings)
        : url(url_)
        , table_location(getTableLocation(url_))
        , client(client_)
    {
        region = client->getRegion();
        if (region.empty() || region == Aws::Region::AWS_GLOBAL)
            region = client->getRegionForBucket(url.bucket, /* force_detect */true);

        /// Check if user didn't mention any region.
        /// Same as in S3/Client.cpp (stripping len("https://s3.")).
        if (url.endpoint.substr(11) == "amazonaws.com")
            url.addRegionToURI(region);

        no_sign = auth_settings[DB::S3AuthSetting::no_sign_request];
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

        const auto & credentials = client->getCredentials();
        auto access_key_id = credentials.GetAWSAccessKeyId();
        auto secret_access_key = credentials.GetAWSSecretKey();
        auto token = credentials.GetSessionToken();

        /// Supported options
        /// https://github.com/apache/arrow-rs-object-store/blob/main/src/aws/builder.rs#L446
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
            log,
            "Using endpoint: {}, uri: {}, region: {}, bucket: {}, no sign: {}, "
            "has access_key_id: {}, has secret_access_key: {}, has token: {}",
            url.endpoint, url.uri_str, region, url.bucket, no_sign,
            !access_key_id.empty(), !secret_access_key.empty(), !token.empty());

        return builder;
    }

private:
    DB::S3::URI url;
    const std::string table_location;
    const std::shared_ptr<const DB::S3::Client> client;
    const LoggerPtr log = getLogger("S3KernelHelper");

    std::string region;
    bool no_sign;

    static std::string getTableLocation(const DB::S3::URI & url)
    {
        return "s3://" + url.bucket + "/" + url.key;
    }
};

#if USE_AZURE_BLOB_STORAGE
/// A helper class to manage Azure Blob Storage.
class AzureKernelHelper final : public IKernelHelper
{
public:
    AzureKernelHelper(
        const DB::AzureBlobStorage::ConnectionParams & connection_params_,
        const std::string & blob_path_)
        : connection_params(connection_params_)
        , table_location(buildTableLocation(connection_params_, blob_path_))
        , data_path(blob_path_)
    {}

    const std::string & getTableLocation() const override { return table_location; }

    const std::string & getDataPath() const override { return data_path; }

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

        const auto & endpoint = connection_params.endpoint;

        /// Supported options
        /// https://github.com/apache/arrow-rs-object-store/blob/main/src/azure/builder.rs#L390
        set_option("azure_container_name", endpoint.container_name);

        /// Extracts the storage account name from the hostname of storage_account_url.
        /// Standard Azure Blob endpoints have the form https://<account>.blob.core.windows.net,
        /// so the subdomain before the first '.' is the account name.
        auto get_account_name = [&]() -> std::string
        {
            const auto & url = endpoint.storage_account_url;
            auto scheme_end = url.find("://");
            if (scheme_end == std::string::npos)
                return {};

            auto host_start = scheme_end + 3;
            auto dot_pos = url.find('.', host_start);
            if (dot_pos == std::string::npos)
                return {};

            return url.substr(host_start, dot_pos - host_start);
        };

        switch (connection_params.auth_method.index())
        {
            case 0: /// ConnectionString
            {
                const auto & auth = std::get<DB::AzureBlobStorage::ConnectionString>(connection_params.auth_method);
                /// delta-kernel-rs does not support azure_storage_connection_string directly.
                /// Parse the connection string into individual components instead.
                /// Translate Azure SDK std::logic_error subtypes (e.g. std::invalid_argument
                /// from std::stoi for malformed ports inside the connection string's
                /// BlobEndpoint URL) to DB::Exception so they don't trigger
                /// abortOnFailedAssertion in debug/sanitizer builds.
                Azure::Storage::_internal::ConnectionStringParts parsed;
                try
                {
                    parsed = Azure::Storage::_internal::ParseConnectionString(auth.toUnderType());
                }
                catch (const std::logic_error & e)
                {
                    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                        "Failed to parse Azure connection string: {}", e.what());
                }

                if (!parsed.AccountName.empty())
                    set_option("azure_storage_account_name", parsed.AccountName);

                if (!parsed.AccountKey.empty())
                {
                    set_option("azure_storage_account_key", parsed.AccountKey);
                }
                else
                {
                    /// SAS-based connection string: extract the SAS token from the
                    /// blob service URL query parameters (appended by ParseConnectionString).
                    auto query_params = parsed.BlobServiceUrl.GetQueryParameters();
                    if (!query_params.empty())
                    {
                        std::string sas;
                        for (const auto & [k, v] : query_params)
                        {
                            if (!sas.empty())
                                sas += '&';
                            sas += k + '=' + v;
                        }
                        set_option("azure_storage_sas_key", sas);
                    }
                }

                /// Set the blob service endpoint URL (without SAS query parameters).
                const auto & blob_url = parsed.BlobServiceUrl;
                const auto & scheme = blob_url.GetScheme();
                set_option("azure_endpoint", connection_params.getConnectionURL());
                if (!scheme.empty() && scheme == "http")
                    set_option("azure_allow_http", "true");
                break;
            }
            case 2: /// StorageSharedKeyCredential
            case 4: /// ManagedIdentityCredential
            {
                const auto & name = endpoint.account_name.empty() ? get_account_name() : endpoint.account_name;
                if (!name.empty())
                    set_option("azure_storage_account_name", name);
                if (!connection_params.endpoint.account_key.empty())
                    set_option("azure_storage_account_key", connection_params.endpoint.account_key);
                break;
            }
            case 1: /// ClientSecretCredential
            case 3: /// WorkloadIdentityCredential
            case 5: /// StaticCredential
            default:
                /// Other variants are not supported yet
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                                "Unsupported authentication type for azure: {}", connection_params.auth_method.index());
        }

        if (!endpoint.sas_auth.empty())
            set_option("azure_storage_sas_key", endpoint.sas_auth);

        /// For non-standard endpoints (e.g., Azurite emulator), set the endpoint explicitly.
        /// Also allow plain HTTP connections when the endpoint uses http://, since the object-store
        /// Azure builder defaults to https_only=true and would reject plain HTTP requests.
        if (!endpoint.storage_account_url.empty() && endpoint.storage_account_url.starts_with("http://"))
        {
            set_option("azure_endpoint", connection_params.getConnectionURL());
            set_option("azure_allow_http", "true");
        }

        LOG_TRACE(
            log,
            "Using azure container: {}, data_path: {}",
            endpoint.container_name, data_path);

        return builder;
    }

private:
    const DB::AzureBlobStorage::ConnectionParams connection_params;
    const std::string table_location;
    const std::string data_path;
    const LoggerPtr log = getLogger("AzureKernelHelper");

    static std::string buildTableLocation(
        const DB::AzureBlobStorage::ConnectionParams & params,
        const std::string & blob_path)
    {
        auto path = blob_path;
        if (!path.empty() && path.front() == '/')
            path = path.substr(1);

        const auto & prefix = params.endpoint.prefix;
        std::string full_path = prefix.empty() ? path : (std::filesystem::path(prefix) / path).string();

        return "az://" + params.endpoint.container_name + "/" + full_path;
    }
};
#endif

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
    const StorageObjectStorageConfigurationPtr & configuration,
    const ObjectStoragePtr & object_storage)
{
    switch (configuration->getType())
    {
        case DB::ObjectStorageType::S3:
        {
            const auto * s3_conf = dynamic_cast<const DB::StorageS3Configuration *>(configuration.get());
            return std::make_shared<DeltaLake::S3KernelHelper>(
                s3_conf->url,
                object_storage->getS3StorageClient(),
                s3_conf->getAuthSettings());
        }
#if USE_AZURE_BLOB_STORAGE
        case DB::ObjectStorageType::Azure:
        {
            return std::make_shared<DeltaLake::AzureKernelHelper>(
                object_storage->getAzureBlobStorageConnectionParams(),
                configuration->getRawPath().path);
        }
#endif
        case DB::ObjectStorageType::Local:
        {
            const auto * local_conf = dynamic_cast<const DB::StorageLocalConfiguration *>(configuration.get());
            return std::make_shared<DeltaLake::LocalKernelHelper>(local_conf->getPathForRead().path);
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
