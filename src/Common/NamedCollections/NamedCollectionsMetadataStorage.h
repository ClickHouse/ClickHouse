#pragma once
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Interpreters/Context_fwd.h>

#include <map>

namespace DB
{

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

    /// Turns stored bytes back into the `CREATE NAMED COLLECTION` statement, verifying the key
    /// fingerprint of the entry's header against this storage's key for the `*_encrypted` storage
    /// types.
    std::string decryptStored(const std::string & stored) const;

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
