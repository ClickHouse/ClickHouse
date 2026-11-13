#pragma once

#include "config.h"

#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Interpreters/Context_fwd.h>
#if CLICKHOUSE_CLOUD && USE_SSL
#include <Core/KMS.h>
#endif

#include <map>
#include <optional>

namespace DB
{

#if CLICKHOUSE_CLOUD && USE_SSL
/// The key of an encrypted storage, see NamedCollectionsMetadataStorage::getEncryptionKeyInfo.
struct NamedCollectionsEncryptionKeyInfo
{
    String algorithm;
    UInt128 key_fingerprint{};
    /// The wrapped key (`key_aws`, `key_gcp`, `key_azure`) as the configuration describes it, see
    /// `parseCloudKMSKeyInfo`; nullopt for a locally configured key (`key_hex`).
    std::optional<CloudKMSKeyInfo> kms_key_info;
};
#endif

class NamedCollectionsMetadataStorage : private WithContext
{
public:
    static std::unique_ptr<NamedCollectionsMetadataStorage> create(const ContextPtr & context);

    NamedCollectionsMap getAll() const;

    MutableNamedCollectionPtr get(const std::string & collection_name) const;

    MutableNamedCollectionPtr create(const ASTCreateNamedCollectionQuery & query);

    MutableNamedCollectionPtr createOrReplace(const ASTCreateNamedCollectionQuery & query);

    void remove(const std::string & collection_name);

    bool removeIfExists(const std::string & collection_name);

    MutableNamedCollectionPtr update(const ASTAlterNamedCollectionQuery & query);

    void shutdown();

    /// Return true if update was made
    bool waitUpdate();

    bool isReplicated() const;

    /// Names of the SQL-created collections present in the storage.
    std::vector<std::string> listCollections() const;

    /// Stored bytes of one collection, exactly as the storage holds them.
    std::string readRaw(const std::string & collection_name) const;

    /// Stored bytes of every collection, exactly as the storage holds them, keyed by the
    /// collection name. Never decrypts anything. A collection dropped concurrently (by another
    /// replica of a replicated storage) between listing and reading is skipped.
    std::map<std::string, std::string> getAllRaw() const;

    /// Identifies the shared storage for backup coordination: the Keeper path holding the
    /// collections. Only meaningful when isReplicated() is true.
    std::string getReplicationId() const;

    /// Whether the entries are encrypted at rest.
    bool isEncrypted() const;

#if CLICKHOUSE_CLOUD && USE_SSL
    /// Describes the key an encrypted storage encrypts with, as captured when the key was loaded:
    /// the storage keeps that key, so a later configuration reload does not change what this
    /// returns. Loads the key if it is not loaded yet, failing if it cannot be loaded (e.g. the
    /// KMS wrapping it is unavailable), so ask only when the key is needed anyway. Nullopt for an
    /// unencrypted storage.
    std::optional<NamedCollectionsEncryptionKeyInfo> getEncryptionKeyInfo() const;
#endif

    /// Appends a message authentication code to stored bytes exported into a backup, because the
    /// stored ciphertext alone is malleable (see `decryptStoredFromBackup`). Only supported with
    /// an encrypted storage.
    std::string authenticateStoredForBackup(const std::string & stored) const;

    /// Turns the bytes of a backup entry written by `authenticateStoredForBackup` back into the
    /// `CREATE NAMED COLLECTION` statement: verifies the key fingerprint of the entry's header
    /// against this storage's key, verifies the authentication code, and decrypts. Only supported
    /// with an encrypted storage.
    std::string decryptStoredFromBackup(const std::string & data) const;

private:
    class INamedCollectionsStorage;
    class LocalStorage;
    class LocalStorageEncrypted;
    class ZooKeeperStorage;
    class ZooKeeperStorageEncrypted;

    std::shared_ptr<INamedCollectionsStorage> storage;

    NamedCollectionsMetadataStorage(std::shared_ptr<INamedCollectionsStorage> storage_, ContextPtr context_);

    ASTCreateNamedCollectionQuery readCreateQuery(const std::string & collection_name) const;

    void writeCreateQuery(const String & collection_name, const String & create_statement, bool replace = false);
};


}
