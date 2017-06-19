#pragma once

#include <Databases/IDatabase.h>
#include <Common/UInt128.h>
#include <Storages/IStorage.h>


namespace Poco { class Logger; }

namespace DB
{

/** Allows you to create "cloud tables".
  * A list of such tables is stored in ZooKeeper.
  * All servers that refer to the same path in ZooKeeper see the same list of tables.
  * CREATE, DROP, RENAME are atomic.
  *
  * The replication level N is set for the database.
  * When writing to a cloud table, N live servers are selected in some way in different datacenters,
  *  on each of them a local table is created, and data is written to them.
  *
  * The engine has the parameters: Cloud(zookeeper_path, replication_factor, datacenter_name)
  * Example: Cloud('/clickhouse/clouds/production/', 3, 'FIN')
  *
  * Structure in ZooKeeper:
  *
  * cloud_path                   - the path to the "cloud"; There may be several different independent clouds
        /table_definitions       - set of unique table definitions so you do not write them many times for a large number of tables
            /hash128 -> sql      - mapping: hash from table definition (identifier) -> table definition itself as CREATE query
        /tables                  - list of tables
            /database_name       - name of the database
                /name_hash_mod -> compressed_table_list
                                 - the list of tables is made two-level to reduce the number of nodes in ZooKeeper if there are a large number of tables
                                 - nodes are created for each remainder of the hash partition from the table name, for example, to 4096
                                 - and each node stores a list of tables (table name, local table name, hash from structure, hosts list) in a compressed form
        /local_tables            - a list of local tables so that by the name of the local table you can determine its structure
            /database_name
                /name_hash_mod -> compressed_table_list
                                 - list of pairs (hash from the table name, hash from the structure) in a compressed form
        /locality_keys           - a serialized list of locality keys in the order in which they appear
                                 - locality key - an arbitrary string
                                 - the database engine defines the servers for the data location in such a way,
                                   that, with the same set of live servers,
                                   one locality key corresponds to one group of N servers for the data location.
        /nodes                   - the list of servers on which cloud databases are registered with the same path in ZK
            /hostname            - hostname
        TODO    /alive           - an ephemeral node for pre-testing liveliness
                /datacenter      - the name of the data center
        TODO    /disk_space

  * For one cloud there may be more than one database named differently. For example, DB `hits` and `visits` can belong to the same cloud.
  */
class DatabaseCloud : public IDatabase
{
private:
    const String name;
    const String data_path;
    String zookeeper_path;
    const size_t replication_factor;
    const String hostname;
    const String datacenter_name;

    using Logger = Poco::Logger;
    Logger * log;

    Context & context;

    /** Local tables are tables that are located directly on the local server.
      * They store the data of the cloud tables: the cloud table is represented by several local tables on different servers.
      * These tables are not visible to the user when listing tables, although they are available when referring by name.
      * The name of the local tables has a special form, for example, start with _local so that they do not get confused with the cloud tables.
      * Local tables are loaded lazily, on first access.
      */
    Tables local_tables_cache;
    mutable std::mutex local_tables_mutex;

    friend class DatabaseCloudIterator;

public:
    DatabaseCloud(
        bool attach,
        const String & name_,
        const String & zookeeper_path_,
        size_t replication_factor_,
        const String & datacenter_name_,
        Context & context_);

    String getEngineName() const override { return "Cloud"; }

    void loadTables(Context & context, ThreadPool * thread_pool, bool has_force_restore_data_flag) override;

    bool isTableExist(const String & table_name) const override;
    StoragePtr tryGetTable(const String & table_name) override;

    DatabaseIteratorPtr getIterator() override;

    bool empty() const override;

    void createTable(
        const String & table_name, const StoragePtr & table, const ASTPtr & query, const String & engine, const Settings & settings) override;

    void removeTable(const String & table_name) override;

    void attachTable(const String & table_name, const StoragePtr & table) override;
    StoragePtr detachTable(const String & table_name) override;

    void renameTable(
        const Context & context, const String & table_name, IDatabase & to_database, const String & to_table_name, const Settings & settings) override;

    time_t getTableMetadataModificationTime(const String & name) override;

    ASTPtr getCreateQuery(const String & table_name) const override;

    void shutdown() override;
    void drop() override;

    void alterTable(
        const Context & context,
        const String & name,
        const NamesAndTypesList & columns,
        const NamesAndTypesList & materialized_columns,
        const NamesAndTypesList & alias_columns,
        const ColumnDefaults & column_defaults,
        const ASTModifier & engine_modifier) override
    {
        throw Exception("ALTER TABLE is not supported by database engine " + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    using Hash = UInt128;

private:
    void createZookeeperNodes();

    /// Get the name of the node in which the part of the list of tables will be stored. (The list of tables is two-level.)
    String getNameOfNodeWithTables(const String & table_name) const;

    /// Hash the table name along with the database name.
    Hash getTableHash(const String & table_name) const;

    /// Table definitions are stored indirectly and addressed by its hash. Calculate the hash.
    Hash getHashForTableDefinition(const String & definition) const;

    /// Go to ZooKeeper and get a table definition by hash.
    String getTableDefinitionFromHash(Hash hash) const;

    /// Define the servers on which the table data will be stored.
    std::vector<String> selectHostsForTable(const String & locality_key) const;
};

}
