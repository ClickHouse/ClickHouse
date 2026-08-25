#pragma once
#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Disks/ObjectStorages/IObjectStorageConnectionInfo.h>
#include <Disks/ObjectStorages/ObjectStorageClientsCache.h>


namespace DB
{
class ReadBuffer;


ObjectStorageConnectionInfoPtr getAzureObjectStorageConnectionInfo(const AzureBlobStorage::ConnectionParams & connection_params);

struct AzureClientInfo
{
    std::string endpoint;
    std::string container_name;
    std::string session_token;
    std::chrono::system_clock::time_point expires_on;

    bool operator == (const AzureClientInfo & other) const
    {
        /// We do not include `expires_on`.
        return endpoint == other.endpoint
            && container_name == other.container_name
            && session_token == other.session_token;
    }

    void updateHash(SipHash & hash, bool include_credentials) const
    {
        hash.update(endpoint);
        hash.update(container_name);
        if (include_credentials)
            hash.update(session_token);
    }
};

class AzureObjectStorageConnectionInfo : public IObjectStorageConnectionInfo
{
    friend class IObjectStorageConnectionInfo;
public:
    using Client = AzureBlobStorage::ContainerClient;
    using ClientInfo = AzureClientInfo;

    /// Writer constructor.
    explicit AzureObjectStorageConnectionInfo(const AzureBlobStorage::ConnectionParams & connection_params);
    /// Reader constructor.
    explicit AzureObjectStorageConnectionInfo(const std::string & user_info_);

    bool equals(const IObjectStorageConnectionInfo &) const override;

    ObjectStorageType getType() const override { return ObjectStorageType::Azure; }

    void updateHash(SipHash & hash, bool include_credentials) const override { client_info.updateHash(hash, include_credentials); }

    std::unique_ptr<ReadBufferFromFileBase> createReader(
        const StoredObject & object,
        const ReadSettings & read_settings) override;

    static std::shared_ptr<AzureBlobStorage::ContainerClient> makeClient(const AzureClientInfo & info);

protected:
    void writeBinaryImpl(size_t protocol_version, WriteBuffer & out) override;

    void readBinaryImpl(ReadBuffer & in);

private:
    AzureClientInfo client_info;
    const std::string user_info;
    std::shared_ptr<const AzureBlobStorage::ContainerClient> client;
};
}

#endif
