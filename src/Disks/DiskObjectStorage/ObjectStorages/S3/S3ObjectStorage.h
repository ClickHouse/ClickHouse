#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <IO/S3/S3Capabilities.h>
#include <IO/S3Settings.h>
#include <Common/MultiVersion.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <IO/ReadBufferFromS3.h>
#include <Parsers/IParser.h>
#include <IO/S3/Client.h>

namespace DB
{

namespace S3RequestSetting
{
    extern const S3RequestSettingsBool read_only;
}


class S3ObjectStorage : public IObjectStorage
{
public:
    using S3CredentialsRefreshCallback = ReadBufferFromS3::S3CredentialsRefreshCallback;

private:
    friend class S3PlainObjectStorage;

    S3ObjectStorage(
        const char * logger_name,
        std::unique_ptr<S3::Client> && client_,
        std::unique_ptr<S3Settings> && s3_settings_,
        S3::URI uri_,
        const S3Capabilities & s3_capabilities_,
        ObjectStorageKeyGeneratorPtr key_generator_,
        const String & disk_name_,
        bool for_disk_s3_ = true,
        const S3CredentialsRefreshCallback & credentials_refresh_callback_ = [] -> std::unique_ptr<const S3::Client>{ return nullptr; })
        : uri(uri_)
        , disk_name(disk_name_)
        , client_slot(std::make_shared<MultiVersion<S3::Client>>(std::move(client_)))
        , client(*client_slot)
        , s3_settings(std::move(s3_settings_))
        , s3_capabilities(s3_capabilities_)
        , key_generator(std::move(key_generator_))
        , log(getLogger(logger_name))
        , for_disk_s3(for_disk_s3_)
        , credentials_refresh_callback(credentials_refresh_callback_)
    {
    }

public:
    template <typename... Args>
    explicit S3ObjectStorage(std::unique_ptr<S3::Client> && client_, Args && ...args)
        : S3ObjectStorage("S3ObjectStorage", std::move(client_), std::forward<Args>(args)...)
    {
    }

    std::string getName() const override { return "S3"; }

    std::string getDiskName() const override { return disk_name; }

    std::string getCommonKeyPrefix() const override { return uri.key; }

    std::string getDescription() const override { return uri.endpoint; }

    ObjectStorageType getType() const override { return ObjectStorageType::S3; }

    bool supportsListObjectsCache() override { return true; }

    bool exists(const StoredObject & object) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
        std::optional<size_t> read_hint = {},
        bool use_external_buffer = false,
        bool restrict_seek = false) const override;

    SmallObjectDataWithMetadata readSmallObjectAndGetObjectMetadata( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings,
        size_t max_size_bytes,
        std::optional<size_t> read_hint = {}) const override;

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override;

    ObjectStorageIteratorPtr iterate(
        const std::string & path_prefix,
        size_t max_keys,
        bool with_tags,
        const std::optional<std::string> & start_after) const override;

    ObjectStorageIteratorPtr iterate(
        const std::string & path_prefix,
        size_t max_keys,
        bool with_tags,
        const std::optional<std::string> & start_after,
        const ObjectStorageControlRequest & request) const override;

    /// Uses `DeleteObjectRequest`.
    void removeObjectIfExists(const StoredObject & object) override;

    /// Uses `DeleteObjectsRequest` if it is allowed by `s3_capabilities`, otherwise `DeleteObjectRequest`.
    /// `DeleteObjectsRequest` does not exist on GCS, see https://issuetracker.google.com/issues/162653700 .
    void removeObjectsIfExist(const StoredObjects & objects) override;

    /// Uses `DeleteObjectRequest` with `If-Match` (token-exact removal for content-addressed disks).
    ConditionalRemoveResult removeObjectIfTokenMatches(const StoredObject & object, const std::string & etag) override;

    ConditionalRemoveResult removeObjectIfTokenMatches(
        const StoredObject & object, const std::string & etag, const ObjectStorageControlRequest & request) override;

    /// One `DeleteObjects` for the given objects (the caller chunks to at most 1000); absence is success.
    /// Exactly one object is always a plain `DeleteObject` instead (never gated on `s3_capabilities`: a
    /// single physical request per call, so there is nothing here for that capability to say no to).
    /// For more than one object, throws `NOT_IMPLEMENTED` without sending anything once `DeleteObjects`
    /// is known unsupported (a configured or a just-learned `S3Capabilities::isBatchDeleteSupported() ==
    /// false`) -- this storage never substitutes a per-key loop of its own, since the caller is the one
    /// that can admit each physical delete as its own request.
    void removeObjectsIfExistUnderProfile(
        const StoredObjects & objects, const ObjectStorageControlRequest & request) override;

    void tagObjects(const StoredObjects & objects, const std::string & tag_key, const std::string & tag_value) override;

    ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override;

    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override;

    /// Marks the HEAD request eligible for the typed NativeConditional mode, so the CAS backend's
    /// `nativeHead` can read a GCS generation token where the client's HTTP layer supports one.
    std::optional<ObjectMetadata> tryGetObjectMetadataWithNativeToken(const std::string & path, bool with_tags) const override;

    std::optional<ObjectMetadata> tryGetObjectMetadataWithNativeToken(
        const std::string & path, bool with_tags, const ObjectStorageControlRequest & request) const override;

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void copyObjectToAnotherObjectStorage( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings,
        IObjectStorage & object_storage_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override;

    void startup() override;

    void applyNewSettings(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context,
        const ApplyNewSettingsOptions & options) override;

    std::string getObjectsNamespace() const override { return uri.bucket; }

    bool isRemote() const override { return true; }

    bool supportParallelWrite() const override { return true; }

    ObjectStorageKeyGeneratorPtr createKeyGenerator() const override;

    bool isReadOnly() const override { return s3_settings.get()->request_settings[S3RequestSetting::read_only]; }

    bool conditionalOpsUseGenerationTokens() const override;

    void pinConditionalOpsGenerationDialect(bool expect_generation_tokens) override;

    std::optional<bool> isBucketVersioningEnabled() const override;

    bool supportsRetryProfile(ObjectStorageRetryProfile) const override { return true; }

    bool supportsCopyMode(ObjectStorageCopyMode mode) const override;

    std::shared_ptr<const S3::Client> getS3StorageClient() override;
    std::shared_ptr<const S3::Client> tryGetS3StorageClient() override;

    bool tryRefreshCredentialsViaCallback() override;

    S3::URI getURI() const { return uri; }
    S3Settings getS3Settings() const { return *s3_settings.get(); }

    /// Lazily-built clone of the current disk client with the single-attempt retry profile
    /// (SingleAttemptRetryStrategy, max_retries=0, Expect:100-continue floor). Rebuilt whenever the
    /// disk client rotates (applyNewSettings/credentials refresh) — the cached clone is keyed by the
    /// base client's identity, so a stale clone can never outlive a rotation.
    /// `request_timeout_ms` overrides the clone's send/receive inactivity bound; 0 keeps the disk's.
    /// `connect_timeout_cap_ms` caps the clone's connect timeout (0 = no cap); the cache key is the
    /// pair, so two callers asking for the same request timeout but different caps get distinct clones.
    std::shared_ptr<const S3::Client> getSingleAttemptClient(uint64_t request_timeout_ms, uint64_t connect_timeout_cap_ms = 0) const;

    /// True iff a clone for exactly this (request timeout, connect cap) pair is already cached --
    /// never builds one. Lets a test prove dispatch used a SPECIFIC key (and no other) without ever
    /// creating a clone itself and without measuring anything.
    bool hasSingleAttemptClientForTest(uint64_t request_timeout_ms, uint64_t connect_timeout_cap_ms) const;

private:
    void removeObjectImpl(const StoredObject & object, bool if_exists);
    void removeObjectsImpl(const StoredObjects & objects, bool if_exists);

    /// Shared by tryGetObjectMetadata/tryGetObjectMetadataWithNativeToken: the only difference between
    /// the two public overrides is which ObjectStorageRequestMode the HEAD wrapper carries.
    std::optional<ObjectMetadata> tryGetObjectMetadataImpl(
        const std::string & path,
        bool with_tags,
        ObjectStorageRequestMode request_mode,
        const std::shared_ptr<const S3::Client> & used_client,
        size_t attempt_seed = 0) const;

    ConditionalRemoveResult removeObjectIfTokenMatchesImpl(
        const StoredObject & object, const std::string & etag, const std::shared_ptr<const S3::Client> & used_client,
        size_t attempt_seed);

    void removeObjectsIfExistImpl(
        const StoredObjects & objects, const std::shared_ptr<const S3::Client> & used_client, size_t attempt_seed);

    std::shared_ptr<const S3::Client> clientForRetryProfile(const ObjectStorageControlRequest & request) const;

    /// Runs `fn` and, if it failed because the vended credentials expired, refreshes this disk's
    /// client and runs it once more. `fn` must re-read the client itself, so the second run signs
    /// with the refreshed one.
    template <typename Fn>
    auto refreshAndRetryOnExpiredCredentials(Fn && fn) const;

    const S3::URI uri;

    std::string disk_name;

    /// The slot this disk's client lives in, and the only thing a credential refresh installs into.
    /// Held by `shared_ptr` so a read buffer -- which can outlive this storage -- carries the SLOT
    /// rather than a pointer to the storage: a refresh that arrives late then replaces a client
    /// nobody will read again, instead of writing into a destroyed object.
    const std::shared_ptr<MultiVersion<S3::Client>> client_slot;
    /// Reference into the slot above. Every ordinary call site keeps using `client.get()`/`client.set()`
    /// unchanged; only code that must capture the client independently of this storage's own lifetime
    /// (the credential-refresh lambda handed to a read buffer that can outlive this object) captures
    /// `client_slot` directly instead.
    MultiVersion<S3::Client> & client;
    MultiVersion<S3Settings> s3_settings;
    S3Capabilities s3_capabilities;

    const ObjectStorageKeyGeneratorPtr key_generator;

    LoggerPtr log;

    const bool for_disk_s3;
    S3CredentialsRefreshCallback credentials_refresh_callback;

    /// Set once by a caller that has derived persistent state from the conditional-ops dialect (see
    /// `pinConditionalOpsGenerationDialect`). Once set, `applyNewSettings` refuses a reload whose
    /// effective `http_client` would flip the dialect, and keeps the working client.
    std::atomic<int8_t> pinned_generation_dialect{-1};   /// -1 unpinned, 0 pinned ETag, 1 pinned generation

    mutable std::mutex single_attempt_client_mutex;
    /// One clone per (requested timeout, connect cap) pair: the verbs of one operation ask for
    /// different bounds, and a single slot would rebuild a whole S3 client (and lose its connection
    /// pool) on every alternation between them.
    mutable std::map<std::pair<uint64_t, uint64_t>, std::shared_ptr<const S3::Client>> single_attempt_clients;
    /// The base client the cached clones above were built from. Deliberately held as a shared_ptr (not
    /// a raw pointer): a raw pointer would be compared for identity AFTER the object it once pointed
    /// to could have been freed and a new client reallocated at the same address by an unrelated
    /// rotation (ABA), which would false-match and serve a stale clone (e.g. built from retired
    /// credentials) indefinitely. Holding the shared_ptr pins at most one retired client version —
    /// released as soon as the next rotation is observed and the clones are dropped — which is what
    /// makes the identity comparison in getSingleAttemptClient sound.
    mutable std::shared_ptr<const S3::Client> single_attempt_client_base;
    /// Set for the duration of a `shutdown()` (cleared by the matching `startup()`); every clone already
    /// cached when `shutdown()` runs is disabled there and then, and this flag disables any built
    /// afterwards to match. See the comment on `shutdown()` for what that disabling does and does not do.
    mutable bool single_attempt_clients_disabled = false;
};

}

#endif
