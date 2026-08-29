#pragma once

#include <Parsers/ASTSQLClusterQuery.h>
#include <Interpreters/Context_fwd.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

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

    /// Return true if Keeper children changed.
    bool waitUpdate();

private:
    String root_path;
    mutable zkutil::ZooKeeperPtr zookeeper_client;
    mutable Coordination::EventPtr wait_event;
    mutable Int32 node_cversion = 0;

    SQLClusterMetadataStorage(ContextPtr context_, String root_path_);

    zkutil::ZooKeeperPtr getClient() const;
    String getPath(const String & file_name) const;
    std::vector<String> list() const;
    String read(const String & file_name) const;
    void write(const String & file_name, const String & data, bool replace);
    bool removeNodeIfExists(const String & file_name);
    bool waitUpdateImpl(size_t timeout);
};

}
