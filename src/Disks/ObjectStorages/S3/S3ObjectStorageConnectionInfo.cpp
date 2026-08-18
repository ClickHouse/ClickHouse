#include <Disks/ObjectStorages/S3/S3ObjectStorageConnectionInfo.h>

#if USE_AWS_S3
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/S3/Client.h>
#include <IO/S3/ProviderType.h>
#include <Interpreters/Context.h>
#include <Common/SipHash.h>
#include <Core/Settings.h>
#if ENABLE_DISTRIBUTED_CACHE
#include <Core/DistributedCacheProtocol.h>
#endif
#include <Disks/ObjectStorages/ObjectStorageClientsCache.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int PROTOCOL_VERSION_MISMATCH;
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool s3_validate_request_settings;
}
namespace S3AuthSetting
{
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsString secret_access_key;
    extern const S3AuthSettingsString session_token;
    extern const S3AuthSettingsString region;
}

void S3ObjectStorageClientInfo::updateHash(SipHash & hash, bool include_credentials) const
{
    hash.update(provider_type);
    hash.update(endpoint);
    hash.update(region);
    hash.update(bucket);

    if (include_credentials)
    {
        switch (provider_type)
        {
            case S3::ProviderType::AWS:
            {
                hash.update(access_key_id);
                hash.update(secret_key);
                break;
            }
            case S3::ProviderType::GCS:
            {
                hash.update(gcs_token);
                break;
            }
            case S3::ProviderType::UNKNOWN:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown S3 provider type");
        }
    }
}

S3ObjectStorageConnectionInfo::S3ObjectStorageConnectionInfo(const S3::Client & client_, const std::string & bucket_)
    : client(client_.clone())
    , log(getLogger("S3ObjectStorageConnectionInfo"))
{
    const auto credentials = client->getCredentials();
    auto provider_type = client->getProviderType();

    if (provider_type == S3::ProviderType::UNKNOWN)
        provider_type = S3::ProviderType::AWS;

    client_info = ClientInfo{
        .provider_type = provider_type,
        .endpoint = client->getInitialEndpoint(),
        .region = client->getRegion(),
        .bucket = bucket_,
        .access_key_id = credentials.GetAWSAccessKeyId(),
        .secret_key = credentials.GetAWSSecretKey(),
        .session_token = credentials.GetSessionToken(),
        .gcs_token = client->getGCSOAuthToken(),
    };
}

S3ObjectStorageConnectionInfo::S3ObjectStorageConnectionInfo(const std::string & user_info_)
    : log(getLogger("S3ObjectStorageConnectionInfo"))
    , user_info(user_info_)
{
}

bool S3ObjectStorageConnectionInfo::equals(const IObjectStorageConnectionInfo & other) const
{
    const auto * s3_info = dynamic_cast<const S3ObjectStorageConnectionInfo *>(&other);
    if (s3_info)
        return client_info == s3_info->client_info;
    return false;
}

void S3ObjectStorageConnectionInfo::readBinaryImpl(ReadBuffer & in)
{
    UInt8 type;
    readVarUInt(type, in);
    client_info.provider_type = S3::ProviderType{type};
    switch (client_info.provider_type)
    {
        case S3::ProviderType::AWS:
        {
            DB::readBinary(client_info.endpoint, in);
            DB::readBinary(client_info.region, in);
            DB::readBinary(client_info.bucket, in);
            DB::readBinary(client_info.access_key_id, in);
            DB::readBinary(client_info.secret_key, in);
            DB::readBinary(client_info.session_token, in);
            break;
        }
        case S3::ProviderType::GCS:
        {
            DB::readBinary(client_info.endpoint, in);
            DB::readBinary(client_info.region, in);
            DB::readBinary(client_info.bucket, in);
            DB::readBinary(client_info.access_key_id, in);
            DB::readBinary(client_info.secret_key, in);

            DB::readBinary(client_info.gcs_token, in);

            break;
        }
        case S3::ProviderType::UNKNOWN:
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown provider type");
        }
    }
}

bool S3ObjectStorageConnectionInfo::refreshCredentials()
{
    if (!client)
        client = ObjectStorageClientsCache<S3ObjectStorageConnectionInfo>::instance().getClient(user_info, client_info);

    const auto credentials = client->getCredentials();
    const auto & access_key_id = credentials.GetAWSAccessKeyId();
    const auto & secret_key = credentials.GetAWSSecretKey();
    const auto & session_token = credentials.GetSessionToken();
    const auto & gcs_token = client->getGCSOAuthToken();

    if (client_info.access_key_id == access_key_id
        && client_info.secret_key == secret_key
        && client_info.session_token == session_token
        && client_info.gcs_token == gcs_token)
    {
        LOG_TEST(log, "Credentials did not change, will not update");
        return false;
    }

    LOG_TRACE(log, "Credentials changed, will update");

    client_info.access_key_id = access_key_id;
    client_info.secret_key = secret_key;
    client_info.session_token = session_token;
    client_info.gcs_token = gcs_token;

    return true;
}

void S3ObjectStorageConnectionInfo::writeBinaryImpl(size_t mutual_protocol_version, WriteBuffer & out)
{
    if (client_info.provider_type == S3::ProviderType::GCS
        && mutual_protocol_version < DistributedCache::PROTOCOL_VERSION_WITH_GCS_TOKEN)
        throw Exception(ErrorCodes::PROTOCOL_VERSION_MISMATCH, "Protocol version does not support GCS token");

    writeVarUInt(UInt8(client_info.provider_type), out);
    switch (client_info.provider_type)
    {
        case S3::ProviderType::AWS:
        {
            DB::writeBinary(client_info.endpoint, out);
            DB::writeBinary(client_info.region, out);
            DB::writeBinary(client_info.bucket, out);
            DB::writeBinary(client_info.access_key_id, out);
            DB::writeBinary(client_info.secret_key, out);
            DB::writeBinary(client_info.session_token, out);
            break;
        }
        case S3::ProviderType::GCS:
        {
            DB::writeBinary(client_info.endpoint, out);
            DB::writeBinary(client_info.region, out);
            DB::writeBinary(client_info.bucket, out);
            DB::writeBinary(client_info.access_key_id, out);
            DB::writeBinary(client_info.secret_key, out);

            DB::writeBinary(client_info.gcs_token, out);

            break;
        }
        case S3::ProviderType::UNKNOWN:
            UNREACHABLE();
    }
}

std::unique_ptr<ReadBufferFromFileBase> S3ObjectStorageConnectionInfo::createReader(
    const StoredObject & object,
    const ReadSettings & read_settings)
{
    if (!client)
        client = ObjectStorageClientsCache<S3ObjectStorageConnectionInfo>::instance().getClient(user_info, client_info);

    S3::S3RequestSettings request_settings;
    return std::make_unique<ReadBufferFromS3>(
        client,
        client_info.bucket,
        object.remote_path,
        /* version_id */"",
        request_settings,
        read_settings,
        /* use_external_buffer */true,
        /* offset */0,
        /* read_until_position */0,
        /* restricted_seek */true);
}


std::shared_ptr<S3::Client> S3ObjectStorageConnectionInfo::makeClient(const S3ObjectStorageClientInfo & info)
{
    auto global_context = Context::getGlobalContextInstance();
    std::unique_ptr<S3Settings> s3_settings= std::make_unique<S3Settings>();

    S3::URI url(info.endpoint);
    if (url.bucket.empty() || url.bucket == "default")
        url.bucket = info.bucket;
    chassert(url.bucket == info.bucket);

    s3_settings->loadFromConfigForObjectStorage(
        global_context->getConfigRef(), "s3", global_context->getSettingsRef(), url.uri.getScheme(), global_context->getSettingsRef()[Setting::s3_validate_request_settings]);

    s3_settings->auth_settings[S3AuthSetting::access_key_id] = info.access_key_id;
    s3_settings->auth_settings[S3AuthSetting::secret_access_key] = info.secret_key;
    s3_settings->auth_settings[S3AuthSetting::session_token] = info.session_token;
    s3_settings->auth_settings[S3AuthSetting::region] = info.region;

    if (!info.gcs_token.empty())
    {
        s3_settings->auth_settings.headers.push_back(
            {"Authorization", fmt::format("Bearer {}", info.gcs_token)});
    }

    return getClient(url, *s3_settings, global_context, true /* for_disk_s3 */);
}


ObjectStorageConnectionInfoPtr getS3ObjectStorageConnectionInfo(const S3::Client & client, const std::string & bucket)
{
    return std::make_shared<S3ObjectStorageConnectionInfo>(client, bucket);
}

}
#endif
