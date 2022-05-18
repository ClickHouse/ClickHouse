#pragma once
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <Access/Common/AccessEntityType.h>
#include <base/UUID.h>
#include <base/types.h>
#include <Poco/Event.h>
#include <Poco/Logger.h>
#include <Poco/Util/LayeredConfiguration.h>

#include "fdb_c_fwd.h"

struct FDB_database;

namespace DB
{
namespace FoundationDB
{
    class MetadataStoreKeyMapper;
    class MetadataConfigParam;

    namespace Proto
    {
        class AccessEntity;
        class AccessEntityIDList;
        class MergeTreePartMeta;
        class MergeTreePartDiskMeta;
        class MergeTreeDetachedPartMeta;
        class MetadataDatabase;
        class MetadataTable;
        class DictInfo;
        class MetadataConfigParam;
        class XmlFuncInfo;
        class SqlFuncInfo;
    }

    using DatabaseKey = std::string;
    using ConfigKey = std::string;
    struct TableKey
    {
        UUID db_uuid;
        std::string table_name;
    };
    struct PartKey
    {
        UUID table_uuid;
        std::string part_name;
    };
}

class MetadataStoreFoundationDB
{
public:
    /// Config example
    /// <foundationdb>
    ///   <!--
    ///     Optional.
    ///     Prefix of all key.
    ///     Default: clickhouse/
    ///   -->
    ///   <key_prefix>clickhouse/<key_prefix>
    ///   <!--
    ///     Optional.
    ///     Specifies the repository where the node uses metadata. The meta_node_id should be different for each instance.
    ///     Default: Server uuid (<path>/uuid).
    ///   -->
    ///   <meta_node_id>instance-0</meta_node_id>
    ///   <!--
    ///     Optional.
    ///     FoundationDB cluster file. See https://apple.github.io/foundationdb/administration.html#cluster-files.
    ///     Default: /etc/foundationdb/fdb.cluster
    ///   -->
    ///   <cluster_file>/etc/foundationdb/fdb.cluster</cluster_file>
    /// </foundationdb>
    MetadataStoreFoundationDB(const Poco::Util::AbstractConfiguration & config, const String & config_name, const UUID & server_uuid);
    ~MetadataStoreFoundationDB();

    /// First boot flag

    /// Mark the first boot with fdb has been completed
    void markFirstBootCompleted();
    /// Check if metadata on fdb is loaded
    bool isFirstBoot();

    /// Database
    using MetadataDatabase = FoundationDB::Proto::MetadataDatabase;
    using DatabaseKey = FoundationDB::DatabaseKey;

    // `uuid` and `loaded` will be removed after refactor of table meta is complete.
    void addDatabaseMeta(const MetadataDatabase & db, const DatabaseKey & db_key, const UUID & uuid);
    std::unique_ptr<MetadataDatabase> getDatabaseMeta(const DatabaseKey & db_key, bool allow_null = false);
    std::vector<std::unique_ptr<MetadataDatabase>> listDatabases();
    bool isExistsDatabase(const DatabaseKey & name);
    void updateDatabaseMeta(const DatabaseKey & name, MetadataDatabase & db);
    void renameDatabase(const DatabaseKey & old_name, const DatabaseKey & new_name);
    void removeDatabaseMeta(const DatabaseKey & name);
    void clearDatabase();
    /// Get a readable key to represent the location of the database metadata in fdb
    std::string getReadableDatabaseKey(const DatabaseKey & db_name);

    /// Table
    using MetadataTable = FoundationDB::Proto::MetadataTable;
    using TableKey = FoundationDB::TableKey;

    void addTableMeta(const MetadataTable & tb_meta, const TableKey & table_key);
    std::unique_ptr<MetadataTable> getTableMeta(const TableKey & table_key);
    std::unique_ptr<MetadataTable> getDroppedTableMeta(const UUID & table_uuid);
    void updateTableMeta(const MetadataTable & tb_meta, const TableKey & table_key);
    void removeTableMeta(const TableKey & table_key);
    void removeDroppedTableMeta(const UUID & table_uuid);
    void renameTable(const TableKey & old_table_key, const TableKey & new_table_key, const MetadataTable & tb_meta);
    std::string renameTableToDropped(const TableKey & table_key, const UUID & table_uuid);
    void exchangeTableMeta(
        const TableKey & old_table_key,
        const TableKey & new_table_key,
        const MetadataTable & table_meta,
        const MetadataTable & other_table_meta);
    bool isExistsTable(const TableKey & table_key);
    void updateDetachTableStatus(const TableKey & table_key, const bool & detached);
    time_t getModificationTime(const TableKey & table_key);
    time_t getDropTime(const UUID & table_uuid);
    std::vector<std::unique_ptr<MetadataTable>> listAllTableMeta(const UUID & db_uuid, bool include_detached = false);
    std::vector<std::unique_ptr<MetadataTable>> listAllDroppedTableMeta();
    bool isDetached(const TableKey & table_key);
    bool isDropped(const UUID & table_uuid);

    /// Config
    using ConfigKey = FoundationDB::ConfigKey;
    using MetadataConfigParam = FoundationDB::Proto::MetadataConfigParam;
    using MetadataConfigParamPtr = std::shared_ptr<MetadataConfigParam>;

    void addConfigParamMeta(const MetadataConfigParam & config, const ConfigKey & config_key);
    void addBunchConfigParamMeta(const std::vector<std::unique_ptr<MetadataConfigParam>> & configs);
    MetadataConfigParamPtr getConfigParamMeta(const ConfigKey & name);
    bool isExistsConfigParamMeta(const ConfigKey & name);
    std::vector<std::unique_ptr<MetadataConfigParam>> listAllConfigParamMeta();
    void updateConfigParamMeta(MetadataConfigParam & config,const ConfigKey & name);
    void removeConfigParamMeta(const ConfigKey & name);
    MetadataConfigParamPtr getAndWatchConfigParamMeta(const ConfigKey & name, std::shared_ptr<Poco::Event> event);
    std::vector<std::unique_ptr<MetadataConfigParam>> watchAllConfigParamMeta(std::shared_ptr<Poco::Event> event);

    /// Access
    using AccessEntity = FoundationDB::Proto::AccessEntity;
    enum AccessEntityScope : char
    {
        CONFIG = 1,
        SQL_DRIVEN = 2
    };

    bool addAccessEntity(
        const AccessEntityScope & scope, const UUID & uuid, const AccessEntity & entity, bool replace_if_exists, bool throw_if_exists);
    bool removeAccessEntity(const AccessEntityScope & scope, const UUID & uuid, AccessEntityType type, bool throw_if_not_exists);
    bool updateAccessEntity(const AccessEntityScope & scope, const UUID & uuid, const AccessEntity & entity, bool throw_if_not_exists);
    std::unique_ptr<AccessEntity> getAccessEntity(const AccessEntityScope & scope, const UUID & uuid, bool throw_if_not_exists);
    std::vector<UUID> listAccessEntity(const AccessEntityScope & scope);
    bool existsAccessEntity(const AccessEntityScope & scope, const UUID & uuid);
    std::vector<std::pair<UUID, std::string>> getAccessEntityListsByType(const AccessEntityScope & scope, AccessEntityType type);
    std::vector<std::pair<UUID, std::unique_ptr<AccessEntity>>> getAllAccessEntities(const AccessEntityScope & scope);
    void clearAccessEntities(const AccessEntityScope & scope);

    /// Part
    using MergeTreePartMeta = FoundationDB::Proto::MergeTreePartMeta;
    using PartKey = FoundationDB::PartKey;
    using MergeTreePartDiskMeta = FoundationDB::Proto::MergeTreePartDiskMeta;

    void addPartMeta(const MergeTreePartDiskMeta & disk, const MergeTreePartMeta & part, const PartKey & part_key);
    std::vector<std::unique_ptr<MergeTreePartDiskMeta>> listParts(const UUID & table_uuid);
    std::unique_ptr<MergeTreePartMeta> getPartMeta(const PartKey & part_key);
    void removePartMeta(const PartKey & part_key);
    bool isExistsPart(const PartKey & part_key);

    using MergeTreeDetachedPartMeta = FoundationDB::Proto::MergeTreeDetachedPartMeta;
    std::vector<std::unique_ptr<MergeTreeDetachedPartMeta>> listDetachedParts(const UUID & table_uuid);
    void addPartDetachedMeta(const MergeTreeDetachedPartMeta & part, const PartKey & part_key);
    std::unique_ptr<MergeTreeDetachedPartMeta> getDetachedPartMeta(const PartKey & part_key);
    void removeDetachedPartMeta(const PartKey & part_key);
    bool isExistsDetachedPart(const PartKey & part_key);

    /// Dictionary
    using DictInfo = FoundationDB::Proto::DictInfo;
    bool addOneDictionary(const DictInfo & dict, const std::string & dict_name);
    bool existOneDictionary(const std::string & dict_name);
    bool deleteOneDictionary(const std::string & dict_name);
    std::unique_ptr<DictInfo> getOneDictionary(const std::string & dict_name);
    bool updateOneDictionary(const std::string & dict_name, const DictInfo & new_dict);
    std::vector<std::unique_ptr<DictInfo>> getAllDictionaries();
    void clearAllDictionaries();

    /// XmlFuctions：
    using XmlFuncInfo = FoundationDB::Proto::XmlFuncInfo;
    bool addOneFunction(const XmlFuncInfo & func, const std::string & func_name);
    bool existOneFunction(const std::string & func_name);
    bool deleteOneFunction(const std::string & func_name);
    std::unique_ptr<XmlFuncInfo> getOneFunction(const std::string & func_name);
    bool updateOneFunction(const std::string & func_name, const XmlFuncInfo & new_func);
    std::vector<std::unique_ptr<XmlFuncInfo>> getAllFunctions();
    void clearAllFunctions();

    /// SqlFunctions
    using SqlFuncInfo = FoundationDB::Proto::SqlFuncInfo;
    bool addSQLFunction(const SqlFuncInfo & func, const std::string & func_name);
    bool existSQLFunction(const std::string & func_name);
    bool deleteSQLFunction(const std::string & func_name);
    std::unique_ptr<SqlFuncInfo> getSQLFunction(const std::string & func_name);
    bool updateSQLFunction(const std::string & func_name, const SqlFuncInfo & new_func);
    std::vector<std::unique_ptr<SqlFuncInfo>> getAllSqlFunctions();
    void clearAllSqlFunctions();

private:
    const Poco::Logger * log;
    String cluster_file_path;

    FDBDatabase * db;
    std::shared_ptr<FDBTransaction> createTransaction();
    /// execTransaction create a transaction and run tr_func.
    /// It implements the recommended retry and backoff behavior for a transaction.
    /// It will automatically retry tr_func if possible.
    template <class TrxFunc>
    auto execTransaction(TrxFunc tr_func, int64_t timeout_ms = 5000) -> decltype(tr_func(nullptr));
    static void commitTransaction(FDBTransaction * tr);

    /// Common operations

    /// List keys prefix with `prefix`. The result include the prefix.
    /// NOTE: Because of the limitation of fdb, listKey() will fetch
    ///       the whole key value pair. If the value is very large,
    ///       you should not using listKey().
    std::vector<String> listKeys(FDBTransaction * tr, const String & prefix);
    inline std::vector<String> listKeys(const String & prefix)
    {
        return execTransaction([&](FDBTransaction * tr) { return listKeys(tr, prefix); });
    }

    /// Get all objects by key prefix.
    template <class Model>
    std::vector<std::unique_ptr<Model>> listValues(FDBTransaction * tr, const String & prefix);
    template <class Model>
    inline std::vector<std::unique_ptr<Model>> listValues(const std::string & prefix)
    {
        return execTransaction([&](auto tr) { return listValues<Model>(tr, prefix); });
    }

    /// Get all KVs startwiths prefix
    template <class Model>
    std::vector<std::pair<std::string, std::unique_ptr<Model>>> listKeyValues(FDBTransaction * tr, const String & prefix);
    template <class Model>
    inline std::vector<std::pair<String, std::unique_ptr<Model>>> listKeyValues(const String & prefix)
    {
        return execTransaction([&](FDBTransaction * tr) { return listKeyValues<Model>(tr, prefix); });
    }

    /// Check exists and set key to value.
    void set(FDBTransaction * tr, const std::string & key, const std::string & data, bool should_exists);
    inline void set(const std::string & key, const std::string & data, bool should_exists)
    {
        execTransaction([&](auto tr) {
            set(tr, key, data, should_exists);
            commitTransaction(tr);
        });
    }

    /// Read and deserialize value by key.
    /// If allow_null is false, get() will throw exception if key is not exists.
    /// Otherwise, get() will return nullptr if key is not exists.
    template <class Model>
    std::unique_ptr<Model> get(FDBTransaction * tr, const std::string & key, bool allow_null = false);
    template <class Model>
    inline std::unique_ptr<Model> get(const std::string & key, bool allow_null = false)
    {
        return execTransaction([&](auto tr) { return get<Model>(tr, key, allow_null); });
    }
    /// Remove key. If the key is not present in fdb, there is no effect.
    void remove(FDBTransaction * tr, const std::string & key);
    inline void remove(const std::string & key)
    {
        execTransaction([&](auto tr) {
            remove(tr, key);
            commitTransaction(tr);
        });
    }

    /// Return true if the key is present in fdb.
    bool isExists(FDBTransaction * tr, const std::string & key);
    inline bool isExists(const std::string & key)
    {
        return execTransaction([&](auto tr) { return isExists(tr, key); });
    }

    /// Report a change in relation to the key’s value as readable by that transaction.
    /// If the value changes and then changes back to its initial value, the watch might not report the change.
    /// Until the transaction that created it has been committed, a watch will not report changes made by other transactions.
    /// See more: https://apple.github.io/foundationdb/api-c.html#c.fdb_transaction_watch
    ///
    /// If the callback throws an exception, the exception message will be recored in the log.
    /// After the method is executed successfully, you must wait for callbacks
    ///  (don't destroy callback data before it will be called).
    void watch(FDBTransaction * tr, const std::string & key, std::function<void(fdb_error_t)> callback);

    /// Clear all key prefix with key_prefix.
    /// DO NOT invoke it in a transaction.
    void clearPrefix(const std::string & key_prefix);

    time_t getTimeByKey(const std::string & time_key);

    std::unique_ptr<FoundationDB::MetadataStoreKeyMapper> keys;
};

}
