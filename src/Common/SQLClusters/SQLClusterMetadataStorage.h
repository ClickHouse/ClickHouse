#pragma once

#include <Parsers/ASTSQLClusterQuery.h>
#include <Interpreters/Context_fwd.h>

#include <memory>
#include <vector>


namespace DB
{

class SQLClusterMetadataStorage : private WithContext
{
public:
    static std::unique_ptr<SQLClusterMetadataStorage> create(const ContextPtr & context);

    std::vector<String> listClusterNames() const;
    bool exists(const String & cluster_name) const;
    ASTCreateSQLClusterQuery readCreateQuery(const String & cluster_name) const;
    void writeCreateQuery(const String & cluster_name, const String & create_statement, bool replace);
    void remove(const String & cluster_name);
    bool removeIfExists(const String & cluster_name);

    /// Return true if storage contents changed.
    bool waitUpdate();

    bool isReplicated() const;

private:
    class ISQLClusterStorage;
    class LocalStorage;
    class LocalStorageEncrypted;
    class ZooKeeperStorage;
    class ZooKeeperStorageEncrypted;

    std::shared_ptr<ISQLClusterStorage> storage;

    SQLClusterMetadataStorage(std::shared_ptr<ISQLClusterStorage> storage_, ContextPtr context_);
};

}
