#pragma once

#include <Parsers/ASTSQLClusterQuery.h>
#include <Interpreters/Context_fwd.h>

#include <memory>
#include <unordered_map>
#include <vector>


namespace DB
{

using SQLClusterCreateQueries = std::unordered_map<String, ASTCreateSQLClusterQuery>;

class SQLClusterMetadataStorage : private WithContext
{
public:
    static std::unique_ptr<SQLClusterMetadataStorage> create(const ContextPtr & context);

    std::vector<String> listClusterNames() const;
    /// Read all stored CREATE CLUSTER statements as a consistent snapshot.
    /// Concurrently dropped children (Keeper `ZNONODE` between list and read) are skipped; for
    /// replicated storage the read is retried until the root version is unchanged across the child reads.
    SQLClusterCreateQueries getAll() const;
    bool exists(const String & cluster_name) const;
    ASTCreateSQLClusterQuery readCreateQuery(const String & cluster_name) const;
    void writeCreateQuery(const String & cluster_name, const String & create_statement, bool replace);
    void remove(const String & cluster_name);
    bool removeIfExists(const String & cluster_name);

    /// Return true if storage contents changed.
    bool waitUpdate();

    /// Mark the last listed snapshot as successfully loaded (Keeper root-version watch).
    void commitReload() const;

    bool isReplicated() const;

private:
    class ISQLClusterStorage;
    class LocalStorage;
    class LocalStorageEncrypted;
    class ZooKeeperStorage;
    class ZooKeeperStorageEncrypted;

    std::shared_ptr<ISQLClusterStorage> storage;

    SQLClusterMetadataStorage(std::shared_ptr<ISQLClusterStorage> storage_, ContextPtr context_);

    SQLClusterCreateQueries readClusters(const std::vector<String> & cluster_names) const;
};

}
