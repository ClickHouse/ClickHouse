#include <Backups/BackupIO_S3.h>

#if USE_AWS_S3
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Common/Throttler.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Interpreters/Context.h>
#include <IO/SharedThreadPools.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/S3/copyS3File.h>
#include <IO/S3/deleteFileFromS3.h>
#include <IO/S3/Client.h>
#include <IO/S3/Credentials.h>
#include <IO/S3/getObjectInfo.h>
#include <Disks/IDisk.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <aws/core/auth/AWSCredentials.h>

#include <boost/algorithm/string/predicate.hpp>

#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 backup_restore_s3_retry_attempts;
    extern const SettingsUInt64 backup_restore_s3_retry_initial_backoff_ms;
    extern const SettingsUInt64 backup_restore_s3_retry_max_backoff_ms;
    extern const SettingsFloat backup_restore_s3_retry_jitter_factor;
    extern const SettingsBool enable_s3_requests_logging;
    extern const SettingsBool s3_disable_checksum;
    extern const SettingsUInt64 s3_max_connections;
    extern const SettingsBool s3_slow_all_threads_after_network_error;
    extern const SettingsBool backup_slow_all_threads_after_retryable_s3_error;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt64 s3_max_redirects;
}

namespace S3AuthSetting
{
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsUInt64 expiration_window_seconds;
    extern const S3AuthSettingsBool no_sign_request;
    extern const S3AuthSettingsString region;
    extern const S3AuthSettingsString secret_access_key;
    extern const S3AuthSettingsString server_side_encryption_customer_key_base64;
    extern const S3AuthSettingsBool use_environment_credentials;
    extern const S3AuthSettingsBool use_insecure_imds_request;

    extern const S3AuthSettingsString role_arn;
    extern const S3AuthSettingsString role_session_name;
    extern const S3AuthSettingsString external_id;
    extern const S3AuthSettingsString session_token;
    extern const S3AuthSettingsString http_client;
    extern const S3AuthSettingsString service_account;
    extern const S3AuthSettingsString metadata_service;
    extern const S3AuthSettingsString request_token_path;
    extern const S3AuthSettingsString google_adc_client_id;
    extern const S3AuthSettingsString google_adc_client_secret;
    extern const S3AuthSettingsString google_adc_refresh_token;
}

namespace S3RequestSetting
{
    extern const S3RequestSettingsBool allow_native_copy;
    extern const S3RequestSettingsString storage_class_name;
    extern const S3RequestSettingsUInt64 http_max_fields;
    extern const S3RequestSettingsUInt64 http_max_field_name_size;
    extern const S3RequestSettingsUInt64 http_max_field_value_size;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
class S3BackupClientCreator
{
public:
    explicit S3BackupClientCreator(const ContextPtr & context)
    {
        const Settings & local_settings = context->getSettingsRef();
        retry_strategy = S3::PocoHTTPClientConfiguration::RetryStrategy{
            .max_retries = static_cast<unsigned>(local_settings[Setting::backup_restore_s3_retry_attempts]),
            .initial_delay_ms = static_cast<unsigned>(local_settings[Setting::backup_restore_s3_retry_initial_backoff_ms]),
            .max_delay_ms = static_cast<unsigned>(local_settings[Setting::backup_restore_s3_retry_max_backoff_ms]),
            .jitter_factor = static_cast<double>(local_settings[Setting::backup_restore_s3_retry_jitter_factor])};
        slow_all_threads_after_retryable_error = local_settings[Setting::backup_slow_all_threads_after_retryable_s3_error];
    }

    S3BackupDiskClientFactory::Entry operator()(DiskPtr disk) const
    {
        auto disk_client = disk->getS3StorageClient();

        auto config = disk_client->getClientConfiguration();
        config.retry_strategy = retry_strategy;
        config.s3_slow_all_threads_after_retryable_error = slow_all_threads_after_retryable_error;

        return {disk_client->cloneWithConfigurationOverride(config), disk_client};
    }

private:
    S3::PocoHTTPClientConfiguration::RetryStrategy retry_strategy;
    bool slow_all_threads_after_retryable_error = false;
};

    std::shared_ptr<S3::Client> makeS3Client(
        const S3::URI & s3_uri,
        const String & access_key_id,
        const String & secret_access_key,
        String role_arn,
        String role_session_name,
        String external_id,
        bool gcp_oauth_supplied_by_query,
        bool from_named_collection,
        const S3Settings & settings,
        const ContextPtr & context)
    {
        Aws::Auth::AWSCredentials credentials(access_key_id, secret_access_key);
        HTTPHeaderEntries headers;
        String session_token = settings.auth_settings[S3AuthSetting::session_token];
        String sse_customer_key = settings.auth_settings[S3AuthSetting::server_side_encryption_customer_key_base64];
        S3::ServerSideEncryptionKMSConfig sse_kms_config = settings.auth_settings.server_side_encryption_kms_config;
        /// Whether the base key pair came from the request rather than the server `<s3>` config fallback below.
        const bool base_keys_supplied_by_query = !access_key_id.empty();
        if (access_key_id.empty())
        {
            credentials = Aws::Auth::AWSCredentials(settings.auth_settings[S3AuthSetting::access_key_id], settings.auth_settings[S3AuthSetting::secret_access_key]);
            headers = settings.auth_settings.headers;
        }

        /// The explicit `BACKUP ... TO S3(url, ak, sk)` form supplies its own keys but has no way to supply a
        /// session_token, so any token here was inherited from the server `<s3>`/endpoint config. Clear it so
        /// the server's temporary token is not sent with the query's keys (a named collection carries its own).
        if (base_keys_supplied_by_query && !from_named_collection)
            session_token.clear();

        /// Neither the explicit `url, ak, sk` form nor the bare `url` form can supply request-auth material, so
        /// under the restriction any headers / SSE-C / SSE-KMS here came from the server `<s3>`/endpoint config.
        /// Drop them: the explicit-key form must not send the server's material alongside the query's keys, and
        /// the bare-URL form that resolves to an anonymous/NOSIGN client must stay genuinely anonymous rather
        /// than still sending operator headers or encryption keys. A named collection is handled by its caller.
        if (!from_named_collection && context->shouldRestrictUserQueryS3Credentials())
        {
            sse_customer_key.clear();
            sse_kms_config = {};
            headers.clear();
        }

        const auto & request_settings = settings.request_settings;
        const auto & server_settings = context->getGlobalContext()->getServerSettings();
        const Settings & global_settings = context->getGlobalContext()->getSettingsRef();
        const Settings & local_settings = context->getSettingsRef();

        /// The passed-in role_arn comes from the query/named collection; if empty, fall back to server config.
        const bool role_arn_supplied_by_query = !role_arn.empty();
        if (role_arn.empty())
        {
            role_arn = settings.auth_settings[S3AuthSetting::role_arn];
            role_session_name = settings.auth_settings[S3AuthSetting::role_session_name];
            external_id = settings.auth_settings[S3AuthSetting::external_id];
        }

        /// Under the restriction a role_arn may be assumed only with the request's own base keys, never the
        /// server `<s3>` keys as the STS base. Drop a server-inherited role used on top of explicit keys, and
        /// (positional/`extra_credentials` form only) drop a query role whose base keys fell back to server
        /// config. A named-collection role keeps its keys from the collection, so a role-only collection is left
        /// to reach the central rejection (a query-overridden role is handled in registerBackupEngineS3).
        const bool drop_inherited_role = !role_arn_supplied_by_query && !credentials.IsEmpty();
        const bool drop_role_on_server_keys
            = !from_named_collection && role_arn_supplied_by_query && !base_keys_supplied_by_query;
        if (!role_arn.empty() && (drop_inherited_role || drop_role_on_server_keys)
            && context->shouldRestrictUserQueryS3Credentials())
        {
            role_arn.clear();
            role_session_name.clear();
            external_id.clear();
        }


        S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
            settings.auth_settings[S3AuthSetting::region],
            context->getRemoteHostFilter(),
            static_cast<unsigned>(server_settings[ServerSetting::s3_max_redirects]),
            S3::PocoHTTPClientConfiguration::RetryStrategy{
                .max_retries = static_cast<unsigned>(local_settings[Setting::backup_restore_s3_retry_attempts]),
                .initial_delay_ms = static_cast<unsigned>(local_settings[Setting::backup_restore_s3_retry_initial_backoff_ms]),
                .max_delay_ms = static_cast<unsigned>(local_settings[Setting::backup_restore_s3_retry_max_backoff_ms]),
                .jitter_factor = static_cast<double>(local_settings[Setting::backup_restore_s3_retry_jitter_factor])},

            local_settings[Setting::s3_slow_all_threads_after_network_error],
            local_settings[Setting::backup_slow_all_threads_after_retryable_s3_error],
            local_settings[Setting::enable_s3_requests_logging],
            /* for_disk_s3 = */ false,
            /* opt_disk_name = */ {},
            request_settings.request_throttler,
            s3_uri.uri.getScheme());

        client_configuration.endpointOverride = s3_uri.endpoint;
        client_configuration.maxConnections = static_cast<unsigned>(global_settings[Setting::s3_max_connections]);
        /// Increase connect timeout
        client_configuration.connectTimeoutMs = 10 * 1000;
        /// Requests in backups can be extremely long, set to one hour
        client_configuration.requestTimeoutMs = 60 * 60 * 1000;
        client_configuration.http_keep_alive_timeout = S3::DEFAULT_KEEP_ALIVE_TIMEOUT;
        client_configuration.http_keep_alive_max_requests = S3::DEFAULT_KEEP_ALIVE_MAX_REQUESTS;
        client_configuration.http_max_fields = request_settings[S3RequestSetting::http_max_fields];
        client_configuration.http_max_field_name_size = request_settings[S3RequestSetting::http_max_field_name_size];
        client_configuration.http_max_field_value_size = request_settings[S3RequestSetting::http_max_field_value_size];

        client_configuration.http_client = settings.auth_settings[S3AuthSetting::http_client];
        client_configuration.service_account = settings.auth_settings[S3AuthSetting::service_account];
        client_configuration.metadata_service = settings.auth_settings[S3AuthSetting::metadata_service];
        client_configuration.request_token_path = settings.auth_settings[S3AuthSetting::request_token_path];
        /// Propagate the explicit Google ADC triple so a user-supplied `gcp_oauth` is accepted by the restriction.
        client_configuration.google_adc_client_id = settings.auth_settings[S3AuthSetting::google_adc_client_id];
        client_configuration.google_adc_client_secret = settings.auth_settings[S3AuthSetting::google_adc_client_secret];
        client_configuration.google_adc_refresh_token = settings.auth_settings[S3AuthSetting::google_adc_refresh_token];

        /// Drop a server-inherited `gcp_oauth` (so the backup uses its explicit keys). The ADC triple is only
        /// ever supplied by a named collection, which also sets `gcp_oauth_supplied_by_query`; so when the
        /// `gcp_oauth` is not query-supplied, the triple here is server config too and must be dropped with it.
        /// Otherwise a server `<s3>` `gcp_oauth` plus a full ADC triple would be treated as explicit and mint a
        /// server bearer token to the user-chosen endpoint. A query-supplied `gcp_oauth` is left in place (and
        /// reaches the central `getCredentialsProvider` rejection when it has no ADC triple).
        if (boost::iequals(client_configuration.http_client, "gcp_oauth")
            && !gcp_oauth_supplied_by_query && context->shouldRestrictUserQueryS3Credentials())
        {
            client_configuration.http_client.clear();
            client_configuration.service_account.clear();
            client_configuration.metadata_service.clear();
            client_configuration.request_token_path.clear();
            client_configuration.google_adc_client_id.clear();
            client_configuration.google_adc_client_secret.clear();
            client_configuration.google_adc_refresh_token.clear();
        }

        S3::ClientSettings client_settings{
            .use_virtual_addressing = s3_uri.is_virtual_hosted_style,
            .disable_checksum = local_settings[Setting::s3_disable_checksum],
            .gcs_issue_compose_request = context->getConfigRef().getBool("s3.gcs_issue_compose_request", false),
            .is_s3express_bucket = S3::isS3ExpressEndpoint(s3_uri.endpoint),
        };

        S3::CredentialsConfiguration credentials_configuration
        {
            settings.auth_settings[S3AuthSetting::use_environment_credentials],
            settings.auth_settings[S3AuthSetting::use_insecure_imds_request],
            settings.auth_settings[S3AuthSetting::expiration_window_seconds],
            settings.auth_settings[S3AuthSetting::no_sign_request],
            std::move(role_arn),
            std::move(role_session_name),
            std::move(external_id),
            /*sts_endpoint_override=*/""
        };

        /// BACKUP/RESTORE TO S3 is driven by user SQL, so it must not be able to reuse the server's own
        /// S3 credentials.
        credentials_configuration.forbid_implicit_credentials = context->shouldRestrictUserQueryS3Credentials();

        auto shared_cache = S3::ClientCacheRegistry::instance().getOrCreateCacheForKey(s3_uri.endpoint, s3_uri.bucket);

        return S3::ClientFactory::instance().create(
            client_configuration,
            client_settings,
            credentials.GetAWSAccessKeyId(),
            credentials.GetAWSSecretKey(),
            sse_customer_key,
            sse_kms_config,
            std::move(headers),
            std::move(credentials_configuration),
            session_token,
            shared_cache);
    }

    String getS3BackupObjectKey(const S3::URI & s3_uri, const String & file_name)
    {
        return fs::path{s3_uri.key} / file_name;
    }

    /// Serializes the S3 request settings effectively used by the backup. The HTTP-client-level values
    /// that makeS3Client overrides (retries, redirects, timeout, slow-thread/logging behavior) are
    /// replaced with the authoritative values from the client configuration; the throttle fields with the
    /// resolved client throttlers (S3RequestSettings::finishInit derives bursts from rps); and the seek
    /// threshold with the read setting (remote_read_min_bytes_for_seek) actually used by ReadBufferFromS3.
    std::map<String, String> serializeBackupS3RequestSettings(
        const S3::S3RequestSettings & request_settings, const S3::PocoHTTPClientConfiguration & client_config,
        const ReadSettings & read_settings)
    {
        auto res = request_settings.getSettingsRepresentation();

        res["retry_attempts"] = std::to_string(client_config.retry_strategy.max_retries);
        res["retry_initial_delay_ms"] = std::to_string(client_config.retry_strategy.initial_delay_ms);
        res["retry_max_delay_ms"] = std::to_string(client_config.retry_strategy.max_delay_ms);
        res["max_redirects"] = std::to_string(client_config.s3_max_redirects);
        res["request_timeout_ms"] = std::to_string(client_config.requestTimeoutMs);
        res["slow_all_threads_after_network_error"] = client_config.s3_slow_all_threads_after_network_error ? "1" : "0";
        res["slow_all_threads_after_retryable_error"] = client_config.s3_slow_all_threads_after_retryable_error ? "1" : "0";
        res["enable_request_logging"] = client_config.enable_s3_requests_logging ? "1" : "0";
        res["min_bytes_for_seek"] = std::to_string(read_settings.remote_fs_settings.min_bytes_for_seek);

        const auto & get_throttler = client_config.request_throttler.get_throttler;
        const auto & put_throttler = client_config.request_throttler.put_throttler;
        res["max_get_rps"] = std::to_string(get_throttler ? get_throttler->getMaxSpeed() : 0UL);
        res["max_get_burst"] = std::to_string(get_throttler ? get_throttler->getMaxBurst() : 0UL);
        res["max_put_rps"] = std::to_string(put_throttler ? put_throttler->getMaxSpeed() : 0UL);
        res["max_put_burst"] = std::to_string(put_throttler ? put_throttler->getMaxBurst() : 0UL);

        /// Drop request settings that backup S3 IO never consumes, so the map reflects only what the
        /// backup engine actually uses:
        ///  - objects_chunk_size_to_delete: `removeFiles` always deletes in chunks of 1000 (the S3
        ///    DeleteObjects API limit), ignoring this setting;
        ///  - list_object_keys_size: `fileExists`/`getFileSize` use HeadObject, backup never lists keys;
        ///  - read_only, throw_on_zero_files_match: disk/storage configuration not used by backup IO.
        for (const auto * key : {"objects_chunk_size_to_delete", "list_object_keys_size", "read_only", "throw_on_zero_files_match"})
            res.erase(key);

        return res;
    }
}


S3BackupDiskClientFactory::S3BackupDiskClientFactory(const S3BackupDiskClientFactory::CreateFn & create_fn_)
    : create_fn(create_fn_)
{
}

std::shared_ptr<S3::Client> S3BackupDiskClientFactory::getOrCreate(DiskPtr disk)
{
    std::lock_guard lock(clients_mutex);

    auto [it, inserted] = clients.try_emplace(disk->getName(), Entry{});
    auto log = getLogger("S3BackupDiskClientFactory");
    auto & entry = it->second;
    if (inserted)
        LOG_TRACE(log, "Creating S3 client for copy from disk '{}' to backup bucket", disk->getName());
    else if (const_pointer_cast<const S3::Client>(entry.disk_reported_client.lock()) != disk->getS3StorageClient())
        LOG_INFO(
            log, "Updating S3 client for copy from disk '{}' to the backup bucket because the disk client was updated", disk->getName());

    while (const_pointer_cast<const S3::Client>(entry.disk_reported_client.lock()) != disk->getS3StorageClient())
        entry = create_fn(disk);

    chassert(entry.backup_client);
    return entry.backup_client;
}

BackupReaderS3::BackupReaderS3(
    const S3::URI & s3_uri_,
    const String & access_key_id_,
    const String & secret_access_key_,
    const String & role_arn,
    const String & role_session_name,
    const String & external_id,
    const std::optional<S3::S3AuthSettings> & named_collection_auth,
    bool allow_s3_native_copy,
    const ReadSettings & read_settings_,
    const WriteSettings & write_settings_,
    const ContextPtr & context_,
    bool is_internal_backup)
    : BackupReaderDefault(read_settings_, write_settings_, getLogger("BackupReaderS3"))
    , s3_uri(s3_uri_)
    , data_source_description{DataSourceType::ObjectStorage, ObjectStorageType::S3, MetadataStorageType::None, s3_uri.endpoint, false, false, ""}
{
    s3_settings.loadFromConfig(context_->getConfigRef(), "s3", context_->getSettingsRef());

    if (auto endpoint_settings = context_->getStorageS3Settings().getSettings(
            s3_uri.uri.toString(), context_->getUserName(), /*ignore_user=*/is_internal_backup))
    {
        s3_settings.updateIfChanged(*endpoint_settings);
    }

    /// A backup named collection fully overrides the credential fields (so a URL-only collection stays
    /// anonymous); the explicit url/key form passes no override and keeps the server `<s3>` config values.
    if (named_collection_auth)
    {
        /// `updateIfChanged` cannot clear non-scalar fields, so under the restriction first drop the server
        /// `<s3>`/endpoint request-auth material (headers/access headers and SSE-C/SSE-KMS keys); a URL-only
        /// collection then stays anonymous and an explicit-key collection does not inherit the server SSE keys.
        if (context_->shouldRestrictUserQueryS3Credentials())
            s3_settings.auth_settings.clearServerManagedRequestAuth();
        s3_settings.auth_settings.updateIfChanged(*named_collection_auth);
    }

    s3_settings.request_settings.updateFromSettings(context_->getSettingsRef(), /* if_changed */true);
    s3_settings.request_settings[S3RequestSetting::allow_native_copy] = allow_s3_native_copy;

    /// A `gcp_oauth` is server-managed unless the named collection supplied it itself (see makeS3Client).
    const bool gcp_oauth_supplied_by_query = named_collection_auth
        && boost::iequals(String((*named_collection_auth)[S3AuthSetting::http_client]), "gcp_oauth");
    client = makeS3Client(s3_uri_, access_key_id_, secret_access_key_, role_arn, role_session_name, external_id, gcp_oauth_supplied_by_query, /* from_named_collection */ named_collection_auth.has_value(), s3_settings, context_);

    if (auto blob_storage_system_log = context_->getBlobStorageLog())
        blob_storage_log = std::make_shared<BlobStorageLogWriter>(blob_storage_system_log);
}

BackupReaderS3::~BackupReaderS3() = default;

std::map<String, String> BackupReaderS3::getSerializedSettings() const
{
    return serializeBackupS3RequestSettings(s3_settings.request_settings, client->getClientConfiguration(), read_settings);
}

bool BackupReaderS3::fileExists(const String & file_name)
{
    return S3::objectExists(*client, s3_uri.bucket, getS3BackupObjectKey(s3_uri, file_name), s3_uri.version_id);
}

UInt64 BackupReaderS3::getFileSize(const String & file_name)
{
    return S3::getObjectSize(*client, s3_uri.bucket, getS3BackupObjectKey(s3_uri, file_name), s3_uri.version_id);
}

std::unique_ptr<ReadBufferFromFileBase> BackupReaderS3::readFile(const String & file_name)
{
    return std::make_unique<ReadBufferFromS3>(
        client, s3_uri.bucket, fs::path(s3_uri.key) / file_name, s3_uri.version_id, s3_settings.request_settings, read_settings);
}

void BackupReaderS3::copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                    DiskPtr destination_disk, const String & destination_path, WriteMode write_mode)
{
    /// Use the native copy as a more optimal way to copy a file from S3 to S3 if it's possible.
    /// We don't check for `has_throttling` here because the native copy almost doesn't use network.
    auto destination_data_source_description = destination_disk->getDataSourceDescription();
    if (destination_data_source_description.sameKind(data_source_description)
        && (destination_data_source_description.is_encrypted == encrypted_in_backup))
    {
        LOG_TRACE(log, "Copying {} from S3 to disk {}", path_in_backup, destination_disk->getName());
        auto write_blob_function = [&](const Strings & blob_path, WriteMode mode, const std::optional<ObjectAttributes> & object_attributes) -> size_t
        {
            /// Object storage always uses mode `Rewrite` because it simulates append using metadata and different files.
            if (blob_path.size() != 2 || mode != WriteMode::Rewrite)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Blob writing function called with unexpected blob_path.size={} or mode={}",
                                blob_path.size(), mode);

            copyS3File(
                client,
                s3_uri.bucket,
                fs::path(s3_uri.key) / path_in_backup,
                0,
                file_size,
                /* dest_s3_client= */ destination_disk->getS3StorageClient(),
                /* dest_bucket= */ blob_path[1],
                /* dest_key= */ blob_path[0],
                s3_settings.request_settings,
                read_settings,
                blob_storage_log,
                threadPoolCallbackRunnerUnsafe<void>(getBackupsIOThreadPool().get(), ThreadName::S3_BACKUP_READER),
                [&, this] { return readFile(path_in_backup); },
                object_attributes);

            return file_size;
        };

        destination_disk->writeFileUsingBlobWritingFunction(destination_path, write_mode, write_blob_function);
        return; /// copied!
    }

    /// Fallback to copy through buffers.
    BackupReaderDefault::copyFileToDisk(path_in_backup, file_size, encrypted_in_backup, destination_disk, destination_path, write_mode);
}

BackupWriterS3::BackupWriterS3(
    const S3::URI & s3_uri_,
    const String & access_key_id_,
    const String & secret_access_key_,
    const String & role_arn,
    const String & role_session_name,
    const String & external_id,
    const std::optional<S3::S3AuthSettings> & named_collection_auth,
    bool allow_s3_native_copy,
    const String & storage_class_name,
    const ReadSettings & read_settings_,
    const WriteSettings & write_settings_,
    const ContextPtr & context_,
    bool is_internal_backup)
    : BackupWriterDefault(read_settings_, write_settings_, getLogger("BackupWriterS3"))
    , s3_uri(s3_uri_)
    , data_source_description{DataSourceType::ObjectStorage, ObjectStorageType::S3, MetadataStorageType::None, s3_uri.endpoint, false, false, ""}
    , s3_capabilities(getCapabilitiesFromConfig(context_->getConfigRef(), "s3"))
    , disk_client_factory(S3BackupClientCreator(context_))
{
    s3_settings.loadFromConfig(context_->getConfigRef(), "s3", context_->getSettingsRef());

    if (auto endpoint_settings = context_->getStorageS3Settings().getSettings(
            s3_uri.uri.toString(), context_->getUserName(), /*ignore_user=*/is_internal_backup))
    {
        s3_settings.updateIfChanged(*endpoint_settings);
    }

    /// A backup named collection fully overrides the credential fields (so a URL-only collection stays
    /// anonymous); the explicit url/key form passes no override and keeps the server `<s3>` config values.
    if (named_collection_auth)
    {
        /// `updateIfChanged` cannot clear non-scalar fields, so under the restriction first drop the server
        /// `<s3>`/endpoint request-auth material (headers/access headers and SSE-C/SSE-KMS keys); a URL-only
        /// collection then stays anonymous and an explicit-key collection does not inherit the server SSE keys.
        if (context_->shouldRestrictUserQueryS3Credentials())
            s3_settings.auth_settings.clearServerManagedRequestAuth();
        s3_settings.auth_settings.updateIfChanged(*named_collection_auth);
    }

    s3_settings.request_settings.updateFromSettings(context_->getSettingsRef(), /* if_changed */true);
    s3_settings.request_settings[S3RequestSetting::allow_native_copy] = allow_s3_native_copy;
    s3_settings.request_settings[S3RequestSetting::storage_class_name] = storage_class_name;

    /// A `gcp_oauth` is server-managed unless the named collection supplied it itself (see makeS3Client).
    const bool gcp_oauth_supplied_by_query = named_collection_auth
        && boost::iequals(String((*named_collection_auth)[S3AuthSetting::http_client]), "gcp_oauth");
    client = makeS3Client(s3_uri_, access_key_id_, secret_access_key_, role_arn, role_session_name, external_id, gcp_oauth_supplied_by_query, /* from_named_collection */ named_collection_auth.has_value(), s3_settings, context_);

    if (auto blob_storage_system_log = context_->getBlobStorageLog())
    {
        blob_storage_log = std::make_shared<BlobStorageLogWriter>(blob_storage_system_log);
        if (context_->hasQueryContext())
            blob_storage_log->query_id = context_->getQueryContext()->getCurrentQueryId();
    }
}

void BackupWriterS3::copyFileFromDisk(
    const String & path_in_backup, DiskPtr src_disk, const String & src_path, bool copy_encrypted, UInt64 start_pos, UInt64 length)
{
    /// Use the native copy as a more optimal way to copy a file from S3 to S3 if it's possible.
    /// We don't check for `has_throttling` here because the native copy almost doesn't use network.
    auto source_data_source_description = src_disk->getDataSourceDescription();
    if (source_data_source_description.sameKind(data_source_description) && (source_data_source_description.is_encrypted == copy_encrypted))
    {
        /// getBlobPath() can return more than 2 elements if the file is stored as multiple objects in S3 bucket.
        /// In this case we can't use the native copy.
        if (auto blob_path = src_disk->getBlobPath(src_path); blob_path.size() == 2)
        {
            LOG_TRACE(log, "Copying file {} from disk {} to S3", src_path, src_disk->getName());
            copyS3File(
                /* src_s3_client */ disk_client_factory.getOrCreate(src_disk),
                /* src_bucket */ blob_path[1],
                /* src_key */ blob_path[0],
                start_pos,
                length,
                /* dest_s3_client */ client,
                /* dest_bucket */ s3_uri.bucket,
                /* dest_key */ fs::path(s3_uri.key) / path_in_backup,
                s3_settings.request_settings,
                read_settings,
                blob_storage_log,
                threadPoolCallbackRunnerUnsafe<void>(getBackupsIOThreadPool().get(), ThreadName::S3_BACKUP_WRITER),
                [&, this]
                {
                    LOG_TRACE(log, "Falling back to copy file {} from disk {} to S3 through buffers", src_path, src_disk->getName());

                    if (copy_encrypted)
                        return src_disk->readEncryptedFile(src_path, read_settings);

                    return src_disk->readFile(src_path, read_settings);
                });
            return; /// copied!
        }
    }

    /// Fallback to copy through buffers.
    BackupWriterDefault::copyFileFromDisk(path_in_backup, src_disk, src_path, copy_encrypted, start_pos, length);
}

void BackupWriterS3::copyFile(const String & destination, const String & source, size_t size)
{
    LOG_TRACE(log, "Copying file inside backup from {} to {}", source, destination);

    const auto source_key = fs::path(s3_uri.key) / source;
    copyS3File(
        client,
        /* src_bucket */ s3_uri.bucket,
        /* src_key= */ source_key,
        0,
        size,
        /* dest_s3_client= */ client,
        /* dest_bucket= */ s3_uri.bucket,
        /* dest_key= */ fs::path(s3_uri.key) / destination,
        s3_settings.request_settings,
        read_settings,
        blob_storage_log,
        threadPoolCallbackRunnerUnsafe<void>(getBackupsIOThreadPool().get(), ThreadName::S3_BACKUP_WRITER),
        [&, this]
        {
            LOG_TRACE(log, "Falling back to copy file inside backup from {} to {} through direct buffers", source, destination);
            return std::make_unique<ReadBufferFromS3>(
                client, s3_uri.bucket, source_key, s3_uri.version_id, s3_settings.request_settings, read_settings);
        });
}

void BackupWriterS3::copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length)
{
    copyDataToS3File(create_read_buffer, start_pos, length, client, s3_uri.bucket, fs::path(s3_uri.key) / path_in_backup,
                     s3_settings.request_settings, blob_storage_log,
                     threadPoolCallbackRunnerUnsafe<void>(getBackupsIOThreadPool().get(), ThreadName::S3_BACKUP_WRITER));
}

BackupWriterS3::~BackupWriterS3() = default;

std::map<String, String> BackupWriterS3::getSerializedSettings() const
{
    return serializeBackupS3RequestSettings(s3_settings.request_settings, client->getClientConfiguration(), read_settings);
}

bool BackupWriterS3::fileExists(const String & file_name)
{
    return S3::objectExists(*client, s3_uri.bucket, getS3BackupObjectKey(s3_uri, file_name), s3_uri.version_id);
}

UInt64 BackupWriterS3::getFileSize(const String & file_name)
{
    return S3::getObjectSize(*client, s3_uri.bucket, getS3BackupObjectKey(s3_uri, file_name), s3_uri.version_id);
}

std::unique_ptr<ReadBuffer> BackupWriterS3::readFile(const String & file_name, size_t expected_file_size)
{
    return std::make_unique<ReadBufferFromS3>(
            client, s3_uri.bucket, fs::path(s3_uri.key) / file_name, s3_uri.version_id, s3_settings.request_settings, read_settings,
            false, 0, 0, false, expected_file_size);
}

std::unique_ptr<WriteBuffer> BackupWriterS3::writeFile(const String & file_name)
{
    return std::make_unique<WriteBufferFromS3>(
        client,
        s3_uri.bucket,
        fs::path(s3_uri.key) / file_name,
        DBMS_DEFAULT_BUFFER_SIZE,
        s3_settings.request_settings,
        blob_storage_log,
        std::nullopt,
        threadPoolCallbackRunnerUnsafe<void>(getBackupsIOThreadPool().get(), ThreadName::S3_BACKUP_WRITER),
        write_settings);
}

void BackupWriterS3::removeFile(const String & file_name)
{
    deleteFileFromS3(client, s3_uri.bucket, fs::path(s3_uri.key) / file_name, /* if_exists = */ true,
                     blob_storage_log);
}

void BackupWriterS3::removeFiles(const Strings & file_names)
{
    Strings keys;
    keys.reserve(file_names.size());
    for (const String & file_name : file_names)
        keys.push_back(fs::path(s3_uri.key) / file_name);

    /// One call of DeleteObjects() cannot remove more than 1000 keys.
    size_t batch_size = 1000;

    deleteFilesFromS3(client, s3_uri.bucket, keys, /* if_exists = */ true,
                      s3_capabilities, batch_size, blob_storage_log);
}

}

#endif
