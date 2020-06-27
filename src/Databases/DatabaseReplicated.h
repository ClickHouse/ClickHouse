#pragma once

#include <Databases/DatabaseOrdinary.h>
#include <Common/randomSeed.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{
/** Replicated database engine.
  * It stores tables list using list of .sql files,
  *  that contain declaration of table represented by SQL ATTACH TABLE query
  *  and operation log in zookeeper
  */
class DatabaseReplicated : public DatabaseOrdinary
{
public:
    DatabaseReplicated(const String & name_, const String & metadata_path_, const String & zookeeper_path_, const String & replica_name_, const Context & context);

    String getEngineName() const override { return "Replicated"; }

    void createTable(
        const Context & context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void dropTable(
        const Context & context,
        const String & table_name,
        bool no_delay) override;

    void renameTable(
        const Context & context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        bool exchange) override;

    void drop(const Context & context) override;

    void loadStoredObjects(
        Context & context,
        bool has_force_restore_data_flag) override;

private:
    String zookeeper_path;
    String replica_name;
    String replica_path;

    zkutil::ZooKeeperPtr current_zookeeper;        /// Use only the methods below.
    mutable std::mutex current_zookeeper_mutex;    /// To recreate the session in the background thread.

    zkutil::ZooKeeperPtr tryGetZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;
    void setZooKeeper(zkutil::ZooKeeperPtr zookeeper);

    void syncReplicaState(Context & context);

    void updateMetadata(Context & context);
};

}
