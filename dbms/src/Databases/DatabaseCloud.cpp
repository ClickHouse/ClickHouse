#include <Common/ZooKeeper/ZooKeeper.h>
#include <Interpreters/Context.h>
#include <Common/escapeForFileName.h>
#include <Common/SipHash.h>
#include <Common/UInt128.h>
#include <Databases/DatabaseCloud.h>
#include <Databases/DatabasesCommon.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/HexWriteBuffer.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
}

namespace
{
    constexpr size_t TABLE_TO_NODE_DIVISOR = 4096;

    using Hash = DatabaseCloud::Hash;
}


void DatabaseCloud::createZookeeperNodes()
{
    zkutil::ZooKeeperPtr zookeeper = context->getZookeeper();

    zookeeper->createAncestors(zookeeper_path);

    auto acl = zookeeper->getDefaultACL();

    zkutil::Ops ops;
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path, "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path + "/table_definitions", "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path + "/tables", "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path + "/local_tables", "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path + "/locality_keys", "", acl, zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(zookeeper_path + "/nodes", "", acl, zkutil::CreateMode::Persistent));

    auto code = zookeeper->tryMulti(ops);
    if (code == ZOK)
        LOG_INFO(log, "Created new cloud.");
    else if (code == ZNODEEXISTS)
        LOG_INFO(log, "Adding server to existing cloud.");
    else
        throw zkutil::KeeperException(code);

    zookeeper->createIfNotExists(zookeeper_path + "/tables/" + name, "");
    zookeeper->createIfNotExists(zookeeper_path + "/local_tables/" + name, "");
    zookeeper->createIfNotExists(zookeeper_path + "/nodes/" + hostname, "");
    zookeeper->createIfNotExists(zookeeper_path + "/nodes/" + hostname + "/datacenter", datacenter_name);
}


DatabaseCloud::DatabaseCloud(
    bool attach,
    const String & name_,
    const String & zookeeper_path_,
    size_t replication_factor_,
    const String & datacenter_name_,
    Context & context_)
    :
    name(name_),
    zookeeper_path(context_.getMacros().expand(zookeeper_path_)),
    replication_factor(replication_factor_),
    datacenter_name(context_.getMacros().expand(datacenter_name_)),
    log(&Logger::get("DatabaseCloud (" + name + ")")),
    context(context_)
{
    if (zookeeper_path.empty())
        throw Exception("Logical error: empty zookeeper_path passed", ErrorCodes::LOGICAL_ERROR);

    if (zookeeper_path.back() == '/')
        zookeeper_path.pop_back();

    hostname = context.getInterserverIOAddress().first;

    data_path = context.getPath() + "/data/" + escapeForFileName(name) + "/";

    if (!attach)
        createZookeeperNodes();
}


void loadTables(Context & context, ThreadPool * thread_pool, bool has_force_restore_data_flag)
{
    /// Do nothing - all tables are loaded lazily.
}


Hash DatabaseCloud::getTableHash(const String & table_name) const
{
    SipHash hash;
    hash.update(name.data(), name.size() + 1);    /// Hashing also a zero byte as a separator.
    hash.update(table_name.data(), table_name.size());

    Hash res;
    hash.get128(reinterpret_cast<char *>(&res));
    return res;
}


String DatabaseCloud::getNameOfNodeWithTables(const String & table_name) const
{
    Hash hash = getTableHash(table_name);
    WriteBufferFromOwnString out;
    writeText(hash.first % TABLE_TO_NODE_DIVISOR, out);
    return out.str();
}


static String hashToHex(Hash hash)
{
    String res;
    {
        WriteBufferFromString str_out(res);
        HexWriteBuffer hex_out(str_out);
        writePODBinary(hash, hex_out);
    }
    return res;
}


String DatabaseCloud::getTableDefinitionFromHash(Hash hash) const
{
    zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();
    return zookeeper->get(zookeeper_path + "/table_definitions/" + hashToHex(hash));
}


/** Description of one table in the list of tables in ZooKeeper.
  * No table name (right side of map).
  */
struct TableDescription
{
    /// Hash of the table structure. The structure itself is stored separately.
    Hash definition_hash;
    /// The name of the local table to store data. It can be empty if nothing else has been written to the table.
    String local_table_name;
    /// The list of hosts on which the table data is located. It can be empty if nothing else has been written to the table.
    std::vector<String> hosts;

    void write(WriteBuffer & buf) const
    {
        writePODBinary(definition_hash, buf);
        writeVectorBinary(hosts, buf);
    }

    void read(ReadBuffer & buf)
    {
        readPODBinary(definition_hash, buf);
        readVectorBinary(hosts, buf);
    }
};


/** Set of tables in ZooKeeper.
  * More precisely, its part, referring to one node.
  * (The whole set is broken into `TABLE_TO_NODE_DIVISOR` nodes.)
  */
struct TableSet
{
    /// Name -> description. In an ordered form, the data will be better compressed.
    using Container = std::map<String, TableDescription>;
    Container map;

    TableSet(const String & data)
    {
        ReadBufferFromString in(data);
        read(in);
    }

    String toString() const
    {
        WriteBufferFromOwnString out;
        write(out);
        return out.str();
    }

    void write(WriteBuffer & buf) const
    {
        writeCString("Version 1\n", buf);

        CompressedWriteBuffer out(buf);     /// NOTE You can reduce size of allocated buffer.
        for (const auto & kv : map)
        {
            writeBinary(kv.first, out);
            kv.second.write(out);
        }
    }

    void read(ReadBuffer & buf)
    {
        assertString("Version 1\n", buf);

        CompressedReadBuffer in(buf);
        while (!in.eof())
        {
            Container::value_type kv;
            readBinary(kv.first, in);
            kv.second.read(in);
            map.emplace(std::move(kv));
        }
    }
};


/** Set of local tables in ZooKeeper.
  * More precisely, its part, referring to one node.
  * (The whole set is broken into `TABLE_TO_NODE_DIVISOR` nodes.)
  */
struct LocalTableSet
{
    /// Hash of name -> hash of structure.
    using Container = std::map<Hash, Hash>;
    Container map;

    TableSet(const String & data)
    {
        ReadBufferFromString in(data);
        read(in);
    }

    String toString() const
    {
        WriteBufferFromOwnString out;
        write(out);
        return out.str();
    }

    void write(WriteBuffer & buf) const
    {
        writeCString("Version 1\n", buf);

        CompressedWriteBuffer out(buf);     /// NOTE You can reduce size of allocated buffer.
        for (const auto & kv : map)
        {
            writePODBinary(kv.first, out);
            writePODBinary(kv.second, out);
        }
    }

    void read(ReadBuffer & buf)
    {
        assertString("Version 1\n", buf);

        CompressedReadBuffer in(buf);
        while (!in.eof())
        {
            Container::value_type kv;
            readPODBinary(kv.first, in);
            readPODBinary(kv.second, in);
            map.emplace(std::move(kv));
        }
    }
};


/** Modify TableSet or LocalTableSet, serialized in ZooKeeper, as a single unit.
  * The compare-and-swap is done. `transform` function can be called many times.
  * If `transform` returns false, it is considered that there is nothing to modify and the result is not saved in ZK.
  */
template <typename TableSet, typename F>
static void modifyTableSet(zkutil::ZooKeeperPtr & zookeeper, const String & path, F && transform)
{
    while (true)
    {
        zkutil::Stat stat;
        String old_value = zookeeper->get(path, &stat);

        TableSet tables_info(old_value);
        if (!transform(tables_info))
            break;

        String new_value = tables_info.toString();

        auto code = zookeeper->trySet(path, new_value, stat.version);

        if (code == ZOK)
            break;
        else if (code == ZBADVERSION)
            continue;   /// Node was changed meanwhile - we'll try again.
            else
                throw zkutil::KeeperException(code, path);
    }
}


template <typename TableSet, typename F>
static void modifyTwoTableSets(zkutil::ZooKeeperPtr & zookeeper, const String & path1, const String & path2, F && transform)
{
    if (path1 == path2)
    {
        modifyTableSet<TableSet>(zookeeper, path1, [&] (TableSet & set) { return transform(set, set); });
        return;
    }

    while (true)
    {
        zkutil::Stat stat1;
        zkutil::Stat stat2;

        String old_value1 = zookeeper->get(path1, &stat1);
        String old_value2 = zookeeper->get(path2, &stat2);

        TableSet tables_info1(old_value1);
        TableSet tables_info2(old_value2);

        if (!transform(tables_info1, tables_info2))
            break;

        String new_value1 = tables_info1.toString();
        String new_value2 = tables_info2.toString();

        zkutil::Ops ops;

        ops.emplace_back(std::make_unique<zkutil::Op::SetData>(path1, new_value1, stat1.version));
        ops.emplace_back(std::make_unique<zkutil::Op::SetData>(path2, new_value2, stat2.version));

        auto code = zookeeper->tryMulti(ops);

        if (code == ZOK)
            break;
        else if (code == ZBADVERSION)
            continue;   /// Node was changed meanwhile - we'll try again.
        else
            throw zkutil::KeeperException(code, path1 + ", " + path2);
    }
}


bool DatabaseCloud::isTableExist(const String & table_name) const
{
    /// We are looking for a local table in the local table cache or in the filesystem in `path`.
    /// If you do not find it, look for the cloud table in ZooKeeper.

    {
        std::lock_guard<std::mutex> lock(local_tables_mutex);
        if (local_tables_cache.count(table_name))
            return true;
    }

    String table_name_escaped = escapeForFileName(table_name);
    if (Poco::File(data_path + table_name_escaped).exists())
        return true;

    zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();

    String table_set_data;
    if (!zookeeper->tryGet(zookeeper_path + "/tables/" + name + "/" + getNameOfNodeWithTables(table_name), table_set_data))
        return false;

    TableSet table_set(table_set_data);
    return table_set.map.count(table_name);
}


StoragePtr DatabaseCloud::tryGetTable(const String & table_name)
{
    /// We are looking for a local table.
    /// If you do not find it, look for the cloud table in ZooKeeper.

    {
        std::lock_guard<std::mutex> lock(local_tables_mutex);
        auto it = local_tables_cache.find(table_name);
        if (it != local_tables_cache.end())
            return it->second;
    }

    zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();

    String table_name_escaped = escapeForFileName(table_name);
    if (Poco::File(data_path + table_name_escaped).exists())
    {
        LocalTableSet local_tables_info(zookeeper->get(
            zookeeper_path + "/local_tables/" + name + "/" + getNameOfNodeWithTables(table_name)));

        Hash table_hash = getTableHash(table_name);
        String definition = getTableDefinitionFromHash(local_tables_info.map.at(table_hash));

        /// Initialize local table.
        {
            std::lock_guard<std::mutex> lock(local_tables_mutex);

            /// And if the table has just been created?
            auto it = local_tables_cache.find(table_name);
            if (it != local_tables_cache.end())
                return it->second;

            String table_name;
            StoragePtr table;
            std::tie(table_name, table) = createTableFromDefinition(
                definition, name, data_path, context, false,
                "in zookeeper node " + zookeeper_path + "/table_definitions/" + hashToHex(table_hash));

            table->startup();

            local_tables_cache.emplace(table_name, table);
            return table;
        }
    }
    else
    {
        const TableSet tables_info(zookeeper->get(
            zookeeper_path + "/tables/" + name + "/" + getNameOfNodeWithTables(table_name)));

        const TableDescription & description = tables_info.at(table_name);
        String definition = getTableDefinitionFromHash(description.definition_hash);

        /// TODO Initialization of `StorageCloud` object
        return {};
    }
}


/// The list of tables can be inconsistent, as it is obtained not atomically, but in pieces, while iterating.
class DatabaseCloudIterator : public IDatabaseIterator
{
private:
    DatabasePtr owned_database;
    zkutil::ZooKeeperPtr zookeeper;
    const String & zookeeper_path;

    bool first = true;
    const Strings nodes;
    Strings::const_iterator nodes_iterator;

    TableSet table_set;
    TableSet::Container::iterator table_set_iterator;

    DatabaseCloud & parent()
    {
        return static_cast<DatabaseCloud &>(*owned_database);
    }

    bool fetchNextTableSet()
    {
        do
        {
            if (first)
                first = false;
            else
                ++nodes_iterator;

            if (nodes_iterator == nodes.end())
                return false;

            table_set = TableSet(zookeeper->get(zookeeper_path + "/" + *nodes_iterator));
            table_set_iterator = table_set.map.begin();
        }
        while (!table_set.map.empty());        /// Skip empty table sets.

        return true;
    }

public:
    DatabaseCloudIterator(DatabasePtr database)
        : owned_database(database),
        zookeeper(parent().context.getZooKeeper()),
        zookeeper_path(parent().zookeeper_path + "/tables/" + parent().name),
        nodes(zookeeper->getChildren(zookeeper_path)),
        nodes_iterator(nodes.begin())
    {
        fetchNextTableSet();
    }

    void next() override
    {
        ++table_set_iterator;
        if (table_set_iterator == table_set.end())
            fetchNextTableSet();
    }

    bool isValid() const override
    {
        return nodes_iterator != nodes.end();
    }

    const String & name() const override
    {
        return table_set_iterator->first;
    }

    StoragePtr & table() const
    {
        String definition = parent().getTableDefinitionFromHash(table_set_iterator->second.definition_hash);

        /// TODO Initialization of `StorageCloud` object
        return {};
    }
};


DatabaseIteratorPtr DatabaseCloud::getIterator()
{
    return std::make_unique<DatabaseCloudIterator>(shared_from_this());
}


bool DatabaseCloud::empty() const
{
    /// There is at least one non-empty node among the list of tables.

    zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();
    Strings nodes = zookeeper->getChildren(zookeeper_path + "/tables/" + name);
    if (nodes.empty())
        return true;

    for (const auto & node : nodes)
        if (!zookeeper->get(zookeeper_path + "/tables/" + name + "/" + node).empty())
            return false;

    return true;
}


ASTPtr DatabaseCloud::getCreateQuery(const String & table_name) const
{
    zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();
    Hash definition_hash;

    String table_name_escaped = escapeForFileName(table_name);
    if (Poco::File(data_path + table_name_escaped).exists())
    {
        LocalTableSet local_tables_info(zookeeper->get(
            zookeeper_path + "/local_tables/" + name + "/" + getNameOfNodeWithTables(table_name)));

        Hash table_hash = getTableHash(table_name);
        definition_hash = local_tables_info.map.at(table_hash);
    }
    else
    {
        const TableSet tables_info(zookeeper->get(
            zookeeper_path + "/tables/" + name + "/" + getNameOfNodeWithTables(table_name)));

        const TableDescription & description = tables_info.at(table_name);
        definition_hash = description.definition_hash;
    }

    String definition = getTableDefinitionFromHash(definition_hash);

    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(parser, definition.data(), definition.data() + definition.size(),
        "in zookeeper node " + zookeeper_path + "/table_definitions/" + hashToHex(definition_hash));

    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    ast_create_query.attach = false;
    ast_create_query.database = name;

    return ast;
}


void DatabaseCloud::attachTable(const String & table_name, const StoragePtr & table)
{
    throw Exception("Attaching tables to cloud database is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

StoragePtr DatabaseCloud::detachTable(const String & table_name)
{
    throw Exception("Detaching tables from cloud database is not supported", ErrorCodes::NOT_IMPLEMENTED);
}


Hash DatabaseCloud::getHashForTableDefinition(const String & definition) const
{
    Hash res;
    SipHash hash;
    hash.update(definition);
    hash.get128(reinterpret_cast<char *>(&res));

    return res;
}


void DatabaseCloud::createTable(
    const String & table_name, const StoragePtr & table, const ASTPtr & query, const String & engine, const Settings & settings)
{
    zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();

    /// Add information about the table structure to ZK.
    String definition = getTableDefinitionFromCreateQuery(query);
    Hash definition_hash = getHashForTableDefinition(definition);
    String zookeeper_definition_path = zookeeper_path + "/table_definitions/" + hashToHex(definition_hash);

    String value;
    if (zookeeper->tryGet(zookeeper_definition_path, &value))
    {
        if (value != definition)
            throw Exception("Logical error: different table definition with same hash", ErrorCodes::LOGICAL_ERROR);
    }
    else
    {
        /// A rarer branch, since there are few unique table definitions.
        /// There is a race condition in which the node already exists, but a check for a logical error (see above) will not be performed.
        /// It does not matter.
        /// By the way, nodes in `table_definitions` are never deleted.
        zookeeper->tryCreate(zookeeper_definition_path, definition, zkutil::CreateMode::Persistent);
    }

    if (engine != "Cloud")
    {
        /// If the local table.
        String table_name_escaped = escapeForFileName(table_name);
        Poco::File(data_path + table_name_escaped).createDirectory();
        Hash table_hash = getTableHash(table_name);

        /// Add information about the local table to ZK.
        modifyTableSet<LocalTableSet>(
            zookeeper,
            zookeeper_path + "/local_tables/" + name + "/" + getNameOfNodeWithTables(table_name),
            [&] (LocalTableSet & set)
            {
                if (!set.map.emplace(table_hash, definition_hash).second)
                    throw Exception("Table " + table_name + " already exists", ErrorCodes::TABLE_ALREADY_EXISTS);
                return true;
            });

        /// Add the local table to the cache.
        {
            std::lock_guard<std::mutex> lock(local_tables_mutex);
            if (!local_tables_cache.emplace(table_name, table).second)
                throw Exception("Table " + table_name + " already exists", ErrorCodes::TABLE_ALREADY_EXISTS);
        }
    }
    else
    {
        /// When creating an empty cloud table, no local tables are created and no servers are defined for them.
        /// Everything is done when you first write to the table.

        TableDescription description;
        description.definition_hash = definition_hash;

        modifyTableSet<TableSet>(
            zookeeper,
            zookeeper_path + "/tables/" + name + "/" + getNameOfNodeWithTables(table_name),
            [&] (TableSet & set)
            {
                if (!set.map.emplace(table_name, description).second)
                    throw Exception("Table " + table_name + " already exists", ErrorCodes::TABLE_ALREADY_EXISTS);
                return true;
            });
    }
}


void DatabaseCloud::removeTable(const String & table_name)
{
    zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();

    /// Looking for a local table.
    /// If you do not find it, look for the cloud table in ZooKeeper.

    String table_name_escaped = escapeForFileName(table_name);
    if (Poco::File(data_path + table_name_escaped).exists())
    {
        Hash table_hash = getTableHash(table_name);

        /// Delete information about the local table from ZK.
        modifyTableSet<LocalTableSet>(
            zookeeper,
            zookeeper_path + "/local_tables/" + name + "/" + getNameOfNodeWithTables(table_name),
            [&] (LocalTableSet & set)
            {
                auto it = set.map.find(table_hash);
                if (it == set.map.end())
                    return false;       /// The table has already been deleted.

                set.map.erase(it);
                return true;
            });

        /// Delete the local table from the cache.
        {
            std::lock_guard<std::mutex> lock(local_tables_mutex);
            local_tables_cache.erase(table_name);
        }
    }
    else
    {
        /// Delete the table from ZK, and also remember the list of servers on which local tables are located.
        TableDescription description;

        modifyTableSet<TableSet>(
            zookeeper,
            zookeeper_path + "/tables/" + name + "/" + getNameOfNodeWithTables(table_name),
            [&] (TableSet & set) mutable
            {
                auto it = set.map.find(table_name);
                if (it == set.map.end())
                    return false;       /// The table has already been deleted.

                description = it->second;
                set.map.erase(it);
                return true;
            });

        if (!description.local_table_name.empty() && !description.hosts.empty())
        {
            /// Deleting local tables. TODO Whether at once here, or in a separate background thread.
        }
    }
}


void DatabaseCloud::renameTable(
    const Context & context, const String & table_name, IDatabase & to_database, const String & to_table_name, const Settings & settings)
{
    /// Only cloud tables can be renamed.
    /// The transfer between databases is not supported.

    if (&to_database != this)
        throw Exception("Moving of tables in Cloud database between databases is not supported", ErrorCodes::NOT_IMPLEMENTED);

    const String node_from = getNameOfNodeWithTables(table_name);
    const String node_to = getNameOfNodeWithTables(to_table_name);

    const String table_set_path_from = zookeeper_path + "/tables/" + name + "/" + node_from;
    const String table_set_path_to = zookeeper_path + "/tables/" + name + "/" + node_to;

    zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();

    modifyTwoTableSets<TableSet>(
        zookeeper, table_set_path_from, table_set_path_to, [&] (TableSet & set_from, TableSet & set_to)
        {
            auto it = set_from.map.find(table_name);
            if (it == set_from.map.end())
                throw Exception("Table " + table_name + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);

            TableDescription description = it->second;
            set_from.map.erase(it);

            if (!set_to.map.emplace(to_table_name, description).second)
                throw Exception("Table " + to_table_name + " already exists", ErrorCodes::TABLE_ALREADY_EXISTS);

            return true;
        });
}


time_t DatabaseCloud::getTableMetaModTime(const String & table_name)
{
    return static_cast<time_t>(0);
}


void DatabaseCloud::shutdown()
{
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function the tables can work with database, and mutex is not recursive.
    Tables local_tables_snapshot;
    {
        std::lock_guard<std::mutex> lock(local_tables_mutex);
        local_tables_snapshot = local_tables_cache;
    }

    for (auto & name_table : local_tables_snapshot)
        name_table.second->shutdown();

    {
        std::lock_guard<std::mutex> lock(local_tables_mutex);
        local_tables_cache.clear();
    }
}


std::vector<String> DatabaseCloud::selectHostsForTable(const String & locality_key) const
{
}


void DatabaseCloud::drop()
{

}

}
