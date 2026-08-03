#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorageConnectionInfo.h>

#if USE_AZURE_BLOB_STORAGE
#include <chrono>
#include <memory>
#include <variant>

#if ENABLE_DISTRIBUTED_CACHE
#include <Core/DistributedCacheProtocol.h>
#endif

#include <Disks/ObjectStorages/ObjectStorageClientsCache.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Common/SipHash.h>

#include <azure/identity/default_azure_credential.hpp>
#include <azure/storage/common/internal/constants.hpp>
#include <azure/storage/common/internal/storage_bearer_token_auth.hpp>


namespace CurrentMetrics
{
    extern const Metric DistrCacheServerS3CachedClients;
}

namespace ProfileEvents
{
    extern const Event DistrCacheServerNewS3CachedClients;
    extern const Event DistrCacheServerReusedS3CachedClients;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int PROTOCOL_VERSION_MISMATCH;
}

AzureObjectStorageConnectionInfo::AzureObjectStorageConnectionInfo(const AzureBlobStorage::ConnectionParams & connection_params)
{
    auto identity = std::get<std::shared_ptr<Azure::Identity::WorkloadIdentityCredential>>(connection_params.auth_method);

    auto az_context = Azure::Core::Context();
    auto token_request_context = Azure::Core::Credentials::TokenRequestContext{
        .Scopes = { Azure::Storage::_internal::StorageScope },
        .TenantId = "",
    };

    auto access_token = identity->GetToken(token_request_context, az_context);

    client_info = AzureClientInfo{
        .endpoint = connection_params.endpoint.getServiceEndpoint(),
        .container_name = connection_params.getContainer() + "/" + connection_params.endpoint.prefix,
        .session_token = access_token.Token,
        .expires_on = static_cast<std::chrono::system_clock::time_point>(access_token.ExpiresOn),
    };
}

AzureObjectStorageConnectionInfo::AzureObjectStorageConnectionInfo(const std::string & user_info_)
    : user_info(user_info_)
{
}

bool AzureObjectStorageConnectionInfo::equals(const IObjectStorageConnectionInfo & other) const
{
    const auto * azure_info = dynamic_cast<const AzureObjectStorageConnectionInfo *>(&other);
    if (azure_info)
        return client_info == azure_info->client_info;
    return false;
}

void AzureObjectStorageConnectionInfo::readBinaryImpl(ReadBuffer & in)
{
    DB::readBinary(client_info.endpoint, in);
    DB::readBinary(client_info.container_name, in);
    DB::readBinary(client_info.session_token, in);
}

void AzureObjectStorageConnectionInfo::writeBinaryImpl(size_t protocol_version, WriteBuffer & out)
{
    if (protocol_version < DistributedCache::PROTOCOL_VERSION_WITH_AZURE_AUTH)
        throw Exception(ErrorCodes::PROTOCOL_VERSION_MISMATCH, "Protocol version does not support Azure");

    DB::writeBinary(client_info.endpoint, out);
    DB::writeBinary(client_info.container_name, out);
    DB::writeBinary(client_info.session_token, out);
}

std::unique_ptr<ReadBufferFromFileBase> AzureObjectStorageConnectionInfo::createReader(
    const StoredObject & object,
    const ReadSettings & read_settings)
{
    if (!client)
        client = ObjectStorageClientsCache<AzureObjectStorageConnectionInfo>::instance().getClient(user_info, client_info);

    AzureBlobStorage::RequestSettings settings;
    return std::make_unique<ReadBufferFromAzureBlobStorage>(
        client,
        object.remote_path,
        read_settings,
        settings.max_single_read_retries,
        settings.max_single_download_retries,
        /* restricted_seek */true,
        /* use_external_buffer */true,
        /* read_until_position */0
    );
}

std::shared_ptr<AzureBlobStorage::ContainerClient> AzureObjectStorageConnectionInfo::makeClient(const AzureClientInfo & info)
{
    auto params = AzureBlobStorage::ConnectionParams{};
    AzureBlobStorage::processURL(info.endpoint, info.container_name, params.endpoint, params.auth_method);

    auto global_context = Context::getGlobalContextInstance();
    auto settings = AzureBlobStorage::getRequestSettings(global_context->getSettingsRef());
    params.client_options = AzureBlobStorage::getClientOptions(global_context, global_context->getSettingsRef(), *settings, /*for_disk*/true);
    params.auth_method = std::make_shared<AzureBlobStorage::StaticCredential>(info.session_token, info.expires_on);

    return params.createForContainer();
}

ObjectStorageConnectionInfoPtr getAzureObjectStorageConnectionInfo(const AzureBlobStorage::ConnectionParams & connection_params)
{
    try
    {
        std::get<std::shared_ptr<Azure::Identity::WorkloadIdentityCredential>>(connection_params.auth_method);
    }
    catch (const std::bad_variant_access & e)
    {
        LOG_WARNING(getLogger("AzureObjectStorageConnectionInfo"),
                    "AzureObjectStorageConnectionInfo: only Workload Identity is supported; {}", e.what());

        return nullptr;
    }

    return std::make_shared<AzureObjectStorageConnectionInfo>(connection_params);
}

}
#endif
