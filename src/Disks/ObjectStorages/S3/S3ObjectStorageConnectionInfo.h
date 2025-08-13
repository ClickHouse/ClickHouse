#pragma once
#include "config.h"

#if USE_AWS_S3
#include <Disks/ObjectStorages/IObjectStorageConnectionInfo.h>
#include <Disks/ObjectStorages/ObjectStorageClientsCache.h>
#include <IO/S3/ProviderType.h>


namespace DB
{

namespace S3 { class Client; }

class ReadBuffer;

ObjectStorageConnectionInfoPtr getS3ObjectStorageConnectionInfo(const S3::Client & client, const std::string & bucket);

struct S3ObjectStorageClientInfo
{
    S3::ProviderType provider_type = S3::ProviderType::UNKNOWN;

    std::string endpoint;
    std::string region;
    std::string bucket;
    std::string access_key_id;
    std::string secret_key;
    std::string session_token;
    std::string gcs_token;

    bool operator == (const S3ObjectStorageClientInfo & other) const = default;

    void updateHash(SipHash & hash, bool include_credentials) const;
};

class S3ObjectStorageConnectionInfo : public IObjectStorageConnectionInfo
{
    friend class IObjectStorageConnectionInfo;
public:
    using Client = S3::Client;
    using ClientInfo = S3ObjectStorageClientInfo;

    /// Writer constructor.
    S3ObjectStorageConnectionInfo(const S3::Client & client, const std::string & bucket);
    /// Reader constructor.
    explicit S3ObjectStorageConnectionInfo(const std::string & user_info_);

    ObjectStorageType getType() const override { return ObjectStorageType::S3; }

    bool equals(const IObjectStorageConnectionInfo &) const override;

    bool refreshCredentials() override;

    void updateHash(SipHash & hash, bool include_credentials) const override { client_info.updateHash(hash, include_credentials); }

    std::unique_ptr<ReadBufferFromFileBase> createReader(
        const StoredObject & object,
        const ReadSettings & read_settings) override;

    static std::shared_ptr<S3::Client> makeClient(const S3ObjectStorageClientInfo &);

protected:
    void writeBinaryImpl(size_t protocol_version, WriteBuffer & out) override;

    void readBinaryImpl(ReadBuffer & in);

private:
    S3ObjectStorageClientInfo client_info;
    std::shared_ptr<S3::Client> client;
    const LoggerPtr log;
    const std::string user_info;
};
}

#endif
