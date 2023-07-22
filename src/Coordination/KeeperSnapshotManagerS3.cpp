#include <Coordination/KeeperSnapshotManagerS3.h>

#if USE_AWS_S3
#include <Core/UUID.h>

#include <Common/Exception.h>
#include <Common/setThreadName.h>

#include <IO/S3/getObjectInfo.h>
#include <IO/S3/Credentials.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Common/Macros.h>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Errors.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

struct KeeperSnapshotManagerS3::S3Configuration
{
    S3Configuration(S3::URI uri_, S3::AuthSettings auth_settings_, std::shared_ptr<const S3::Client> client_)
        : uri(std::move(uri_))
        , auth_settings(std::move(auth_settings_))
        , client(std::move(client_))
    {}

    S3::URI uri;
    S3::AuthSettings auth_settings;
    std::shared_ptr<const S3::Client> client;
};

KeeperSnapshotManagerS3::KeeperSnapshotManagerS3()
    : snapshots_s3_queue(std::numeric_limits<size_t>::max())
    , log(&Poco::Logger::get("KeeperSnapshotManagerS3"))
    , uuid(UUIDHelpers::generateV4())
{}

void KeeperSnapshotManagerS3::updateS3Configuration(const Poco::Util::AbstractConfiguration & config, const MultiVersion<Macros>::Version & macros)
{
    try
    {
        const std::string config_prefix = "keeper_server.s3_snapshot";

        if (!config.has(config_prefix))
        {
            std::lock_guard client_lock{snapshot_s3_client_mutex};
            if (snapshot_s3_client)
                LOG_INFO(log, "S3 configuration was removed");
            snapshot_s3_client = nullptr;
            return;
        }

        auto auth_settings = S3::AuthSettings::loadFromConfig(config_prefix, config);

        String endpoint = macros->expand(config.getString(config_prefix + ".endpoint"));
        auto new_uri = S3::URI{endpoint};

        {
            std::lock_guard client_lock{snapshot_s3_client_mutex};
            // if client is not changed (same auth settings, same endpoint) we don't need to update
            if (snapshot_s3_client && snapshot_s3_client->client && auth_settings == snapshot_s3_client->auth_settings
                && snapshot_s3_client->uri.uri == new_uri.uri)
                return;
        }

        LOG_INFO(log, "S3 configuration was updated");

        auto credentials = Aws::Auth::AWSCredentials(auth_settings.access_key_id, auth_settings.secret_access_key);
        auto headers = auth_settings.headers;

        static constexpr size_t s3_max_redirects = 10;
        static constexpr bool enable_s3_requests_logging = false;

        if (!new_uri.key.empty())
        {
            LOG_ERROR(log, "Invalid endpoint defined for S3, it shouldn't contain key, endpoint: {}", endpoint);
            return;
        }

        S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
            auth_settings.region,
            RemoteHostFilter(), s3_max_redirects,
            enable_s3_requests_logging,
            /* for_disk_s3 = */ false, /* get_request_throttler = */ {}, /* put_request_throttler = */ {});

        client_configuration.endpointOverride = new_uri.endpoint;

        auto client = S3::ClientFactory::instance().create(
            client_configuration,
            new_uri.is_virtual_hosted_style,
            credentials.GetAWSAccessKeyId(),
            credentials.GetAWSSecretKey(),
            auth_settings.server_side_encryption_customer_key_base64,
            std::move(headers),
            S3::CredentialsConfiguration
            {
                auth_settings.use_environment_credentials.value_or(false),
                auth_settings.use_insecure_imds_request.value_or(false),
                auth_settings.expiration_window_seconds.value_or(S3::DEFAULT_EXPIRATION_WINDOW_SECONDS),
                auth_settings.no_sign_request.value_or(false),
            });

        auto new_client = std::make_shared<KeeperSnapshotManagerS3::S3Configuration>(std::move(new_uri), std::move(auth_settings), std::move(client));

        {
            std::lock_guard client_lock{snapshot_s3_client_mutex};
            snapshot_s3_client = std::move(new_client);
        }
        LOG_INFO(log, "S3 client was updated");
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to create an S3 client for snapshots");
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}
std::shared_ptr<KeeperSnapshotManagerS3::S3Configuration> KeeperSnapshotManagerS3::getSnapshotS3Client() const
{
    std::lock_guard lock{snapshot_s3_client_mutex};
    return snapshot_s3_client;
}

void KeeperSnapshotManagerS3::uploadSnapshotImpl(const std::string & snapshot_path)
{
    try
    {
        auto s3_client = getSnapshotS3Client();
        if (s3_client == nullptr)
            return;

        S3Settings::RequestSettings request_settings_1;

        const auto create_writer = [&](const auto & key)
        {
            return WriteBufferFromS3
            {
                s3_client->client,
                s3_client->uri.bucket,
                key,
                request_settings_1
            };
        };

        LOG_INFO(log, "Will try to upload snapshot on {} to S3", snapshot_path);
        ReadBufferFromFile snapshot_file(snapshot_path);

        auto snapshot_name = fs::path(snapshot_path).filename().string();
        auto lock_file = fmt::format(".{}_LOCK", snapshot_name);

        if (S3::objectExists(*s3_client->client, s3_client->uri.bucket, snapshot_name))
        {
            LOG_ERROR(log, "Snapshot {} already exists", snapshot_name);
            return;
        }

        // First we need to verify that there isn't already a lock file for the snapshot we want to upload
        // Only leader uploads a snapshot, but there can be a rare case where we have 2 leaders in NuRaft
        if (S3::objectExists(*s3_client->client, s3_client->uri.bucket, lock_file))
        {
            LOG_ERROR(log, "Lock file for {} already, exists. Probably a different node is already uploading the snapshot", snapshot_name);
            return;
        }

        // We write our UUID to lock file
        LOG_DEBUG(log, "Trying to create a lock file");
        WriteBufferFromS3 lock_writer = create_writer(lock_file);
        writeUUIDText(uuid, lock_writer);
        lock_writer.finalize();

        // We read back the written UUID, if it's the same we can upload the file
        S3Settings::RequestSettings request_settings_2;
        request_settings_2.max_single_read_retries = 1;
        ReadBufferFromS3 lock_reader
        {
            s3_client->client,
            s3_client->uri.bucket,
            lock_file,
            "",
            request_settings_2,
            {}
        };

        std::string read_uuid;
        readStringUntilEOF(read_uuid, lock_reader);

        if (read_uuid != toString(uuid))
        {
            LOG_ERROR(log, "Failed to create a lock file");
            return;
        }

        SCOPE_EXIT(
        {
            LOG_INFO(log, "Removing lock file");
            try
            {
                S3::DeleteObjectRequest delete_request;
                delete_request.SetBucket(s3_client->uri.bucket);
                delete_request.SetKey(lock_file);
                auto delete_outcome = s3_client->client->DeleteObject(delete_request);
                if (!delete_outcome.IsSuccess())
                    throw S3Exception(delete_outcome.GetError().GetMessage(), delete_outcome.GetError().GetErrorType());
            }
            catch (...)
            {
                LOG_INFO(log, "Failed to delete lock file for {} from S3", snapshot_path);
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        });

        WriteBufferFromS3 snapshot_writer = create_writer(snapshot_name);
        copyData(snapshot_file, snapshot_writer);
        snapshot_writer.finalize();

        LOG_INFO(log, "Successfully uploaded {} to S3", snapshot_path);
    }
    catch (...)
    {
        LOG_INFO(log, "Failure during upload of {} to S3", snapshot_path);
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void KeeperSnapshotManagerS3::snapshotS3Thread()
{
    setThreadName("KeeperS3SnpT");

    while (!shutdown_called)
    {
        std::string snapshot_path;
        if (!snapshots_s3_queue.pop(snapshot_path))
            break;

        if (shutdown_called)
            break;

        uploadSnapshotImpl(snapshot_path);
    }
}

void KeeperSnapshotManagerS3::uploadSnapshot(const std::string & path, bool async_upload)
{
    if (getSnapshotS3Client() == nullptr)
        return;

    if (async_upload)
    {
        if (!snapshots_s3_queue.push(path))
            LOG_WARNING(log, "Failed to add snapshot {} to S3 queue", path);

        return;
    }

    uploadSnapshotImpl(path);
}

void KeeperSnapshotManagerS3::startup(const Poco::Util::AbstractConfiguration & config, const MultiVersion<Macros>::Version & macros)
{
    updateS3Configuration(config, macros);
    snapshot_s3_thread = ThreadFromGlobalPool([this] { snapshotS3Thread(); });
}

void KeeperSnapshotManagerS3::shutdown()
{
    if (shutdown_called)
        return;

    LOG_DEBUG(log, "Shutting down KeeperSnapshotManagerS3");
    shutdown_called = true;

    try
    {
        snapshots_s3_queue.finish();
        if (snapshot_s3_thread.joinable())
            snapshot_s3_thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    LOG_INFO(log, "KeeperSnapshotManagerS3 shut down");
}

}

#endif
