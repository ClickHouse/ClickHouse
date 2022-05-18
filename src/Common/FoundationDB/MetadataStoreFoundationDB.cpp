#include <algorithm>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <IO/WriteHelpers.h>
#include <base/UUID.h>
#include <bits/types/time_t.h>
#include <fmt/core.h>
#include <foundationdb/fdb_c.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Access/Common/AccessEntityType.h>
#include <google/protobuf/message.h>
#include <MergeTreePartInfo.pb.h>
#include <MetadataACLEntry.pb.h>
#include <MetadataDatabase.pb.h>
#include <MetadataDictionaries.pb.h>
#include <MetadataConfigParam.pb.h>
#include <MetadataFuctions.pb.h>
#include <MetadataTable.pb.h>
#include <MetadataSqlFunctions.pb.h>

#include "Core/Field.h"
#include "Core/UUID.h"
#include "FoundationDBCommon.h"
#include "FoundationDBHelpers.h"
#include "MetadataStoreFoundationDB.h"
#include "Parsers/IAST.h"
#include "ProtobufTypeHelpers.h"

namespace fs = std::filesystem;
namespace Proto = DB::FoundationDB::Proto;
using namespace std::literals;

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FDB_META_EXCEPTION;
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
    extern const int ACCESS_ENTITY_NOT_FOUND;
    extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
}

/// Helper functions that only used in this file.
/// Some useful utility functions can be moved into FoundationDBCommon.h in the future, if they need to be exported.
namespace
{
    /// Block until the given Future is ready.
    /// It will throw FDBException if the Future is set to an error.
    void wait(FDBFuture * f)
    {
        throwIfFDBError(fdb_future_block_until_ready(f));
        throwIfFDBError(fdb_future_get_error(f));
    }

    /// Block until the given Future is ready.
    /// It will throw FDBException if the Future is set to an error.
    void wait(std::shared_ptr<FDBFuture> f) { wait(f.get()); }

    /// Convert POD to char *. Assuming little endian architecture.
    template <typename T>
    inline std::string_view toBinary(T & value)
    {
        return std::string_view(reinterpret_cast<const char *>(&value), sizeof(value));
    }

    template <typename T>
    inline std::string toBinary(T && value)
    {
        return std::string(reinterpret_cast<const char *>(&value), sizeof(value));
    }

    /// Convert char * to POD. Assuming little endian architecture.
    template <typename T>
    inline const T & fromBinary(const void * value)
    {
        return *reinterpret_cast<const T *>(value);
    }

    template <typename T>
    inline std::unique_ptr<T> deserialize(const void * data, size_t size);

    template <typename T>
    requires std::is_base_of_v<google::protobuf::Message, T>
    inline std::unique_ptr<T> deserialize(const void * data, size_t size)
    {
        auto obj = std::make_unique<T>();
        if (!obj->ParseFromArray(data, static_cast<int>(size)))
        {
            /// TODO: Retrieve more useful error message from protobuf
            throw Exception(ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA, "Failed to parse proto message");
        }
        return obj;
    }

    template <>
    inline std::unique_ptr<String> deserialize<String>(const void * data, size_t size)
    {
        return std::make_unique<String>(static_cast<const char *>(data), size);
    }

    using FDBFutureCallback = std::function<void(FDBFuture *)>;
    void fdb_future_callback(FDBFuture * future, void * payload)
    {
        auto * cb = reinterpret_cast<FDBFutureCallback *>(payload);
        (*cb)(future);
        delete cb;
    }

    static const uint8_t fdb_atomic_plus_one_64[8] = {1, 0, 0, 0, 0, 0, 0, 0};
#define FDB_ATOMIC_PLUS_ONE fdb_atomic_plus_one_64, sizeof(fdb_atomic_plus_one_64), FDB_MUTATION_TYPE_ADD
}

namespace FoundationDB
{
    /// TODO: Rewrite key concatenation for better performance
    class MetadataStoreKeyMapper
    {
    public:
        explicit MetadataStoreKeyMapper(const std::string & common_prefix, const std::string & meta_node_id)
        {
            const auto node_namespace = common_prefix + meta_node_id + '\0';

            loaded_flag = node_namespace + "loaded";
            db_prefix = node_namespace + "db" + '\0';
            tb_prefix = node_namespace + "tb" + '\0';
            acl_prefix = node_namespace + "acl" + '\0';
            part_prefix = node_namespace + "part" + '\0';
            dict_prefix = node_namespace + "dict" + '\0';
            config_prefix = node_namespace + "conf" + '\0';
            func_prefix = node_namespace + "func" + '\0';
            sqlfunc_prefix = node_namespace + "sqlfunc" + '\0';
        }

        /// Loaded Key

        std::string loaded_flag;
        std::string loadedFlagKey() const { return loaded_flag; }

        /// Database Keys
        /// {db_prefix}{db_name} = Database Meta

        std::string db_prefix;
        std::string getDBKeyFromName(const std::string & db_name) const { return db_prefix + db_name; }
        std::string getDBNameFromKey(const std::string & key) const { return key.substr(db_prefix.size()); }

        /// Table Keys
        /// {tb_prefix}using\x00{db_id}meta\x00{table_name}     = Table Meta
        /// {tb_prefix}using\x00{db_id}time\x00{table_name}     = Table Update Time
        /// {tb_prefix}using\x00{db_id}detached\x00{table_name} = Detach flag
        ///
        /// Dropped table keys
        /// {tb_prefix}dropped\x00meta\x00{table_uuid}          = Dropped table meta
        /// {tb_prefix}dropped\x00time\x00{table_uuid}          = Dropped time

        std::string tb_prefix;
        static constexpr auto tb_using_prefix = "using\0"sv;
        static constexpr auto tb_dropped_prefix = "dropped\0"sv;
        static constexpr auto tb_meta_prefix = "meta\0"sv;
        static constexpr auto tb_detached_prefix = "detached\0"sv;
        static constexpr auto tb_time_prefix = "time\0"sv;

        std::string getTableNameFromKey(const std::string & key) const
        {
            return key.substr(tb_prefix.size() + tb_using_prefix.size() + sizeof(UUID) + tb_meta_prefix.size());
        }

        std::string getTableNameFromDetachedKey(const std::string & key) const
        {
            return key.substr(tb_prefix.size() + tb_using_prefix.size() + sizeof(UUID) + tb_detached_prefix.size());
        }

        std::string getTableKeyPrefix(const UUID & db_id) const
        {
            auto key = tb_prefix;
            key.append(tb_using_prefix);
            key.append(toBinary(db_id));

            return key;
        }
        std::string getTableMetaKeyPrefix(const UUID & db_id) const
        {
            auto key = getTableKeyPrefix(db_id);
            key.append(tb_meta_prefix);
            return key;
        }

        std::string getTableMetaKey(const TableKey & table_key) const
        {
            return getTableMetaKeyPrefix(table_key.db_uuid) + table_key.table_name;
        }

        std::string getTableTimeKey(const TableKey & table_key) const
        {
            auto key = getTableKeyPrefix(table_key.db_uuid);
            key.append(tb_time_prefix);
            key.append(table_key.table_name);
            return key;
        }

        std::string getTableDetachFlagKeyPrefix(const UUID & db_id) const
        {
            auto key = getTableKeyPrefix(db_id);
            key.append(tb_detached_prefix);
            return key;
        }

        std::string getTableDetachFlagKey(const TableKey & table_key) const
        {
            return getTableDetachFlagKeyPrefix(table_key.db_uuid) + table_key.table_name;
        }

        std::string getDroppedTableMetaKeyPrefix() const
        {
            auto key = tb_prefix;
            key.append(tb_dropped_prefix);
            key.append(tb_meta_prefix);
            return key;
        }

        std::string getDroppedTableKey(const UUID & table_uuid) const
        {
            auto key = getDroppedTableMetaKeyPrefix();
            key.append(toBinary(table_uuid));
            return key;
        }

        std::string getTableDropTimeKey(const UUID & table_uuid) const
        {
            auto key = tb_prefix;
            key.append(tb_dropped_prefix);
            key.append(tb_time_prefix);
            key.append(toBinary(table_uuid));
            return key;
        }

        /// Access
        /// {acl_prefix}{(char) scope}meta\x00{(UUID) uuid}  = Access Entity
        /// {acl_prefix}{(char) scope}index\x00{(UUID) uuid} = Access Entity Index

        using AccessEntityScope = MetadataStoreFoundationDB::AccessEntityScope;
        std::string acl_prefix;
        static constexpr auto acl_meta_prefix = "meta\0"sv;
        static constexpr auto acl_index_prefix = "index\0"sv;

        std::string protoACLTypeToString(const MetadataStoreFoundationDB::AccessEntity::EntityTypeCase protoType) const
        {
            switch (protoType)
            {
                case MetadataStoreFoundationDB::AccessEntity::EntityTypeCase::kUser:
                    return "USER\0"s;
                case MetadataStoreFoundationDB::AccessEntity::EntityTypeCase::kRole:
                    return "ROLE\0"s;
                case MetadataStoreFoundationDB::AccessEntity::EntityTypeCase::kQuota:
                    return "QUOTA\0"s;
                case MetadataStoreFoundationDB::AccessEntity::EntityTypeCase::kRowPolicy:
                    return "ROW_POLICY\0"s;
                case MetadataStoreFoundationDB::AccessEntity::EntityTypeCase::kSettingsProfile:
                    return "SETTINGS_PROFILE\0"s;
                default:
                    return "MAX\0"s;
            }
        }
        std::string ckACLTypeToString(const AccessEntityType type) const
        {
            switch (type)
            {
                case AccessEntityType::USER:
                    return "USER\0"s;
                case AccessEntityType::ROLE:
                    return "ROLE\0"s;
                case AccessEntityType::QUOTA:
                    return "QUOTA\0"s;
                case AccessEntityType::ROW_POLICY:
                    return "ROW_POLICY\0"s;
                case AccessEntityType::SETTINGS_PROFILE:
                    return "SETTINGS_PROFILE\0"s;
                default:
                    return "MAX\0"s;
            }
        }
        std::string getScopedACLKeyPrefix(const MetadataStoreFoundationDB::AccessEntityScope & scope) const
        {
            std::string prefix;
            prefix.reserve(acl_prefix.size() + sizeof(scope) + acl_meta_prefix.size());

            prefix.append(acl_prefix);
            prefix.append(toBinary(scope));
            prefix.append(acl_meta_prefix);
            return prefix;
        }

        std::string getScopedACLKeyFromUUID(const MetadataStoreFoundationDB::AccessEntityScope & scope, const UUID & uuid) const
        {
            std::string key;
            key.reserve(acl_prefix.size() + sizeof(scope) + acl_meta_prefix.size() + sizeof(UUID));

            key.append(acl_prefix);
            key.append(toBinary(scope));
            key.append(acl_meta_prefix);
            key.append(toBinary(uuid));
            return key;
        }
        std::string getScopedACLIndexKeyPrefix(const MetadataStoreFoundationDB::AccessEntityScope & scope, AccessEntityType type) const
        {
            std::string prefix;
            std::string type_str = ckACLTypeToString(type);
            prefix.reserve(acl_prefix.size() + sizeof(scope) + acl_index_prefix.size() + type_str.size());

            prefix.append(acl_prefix);
            prefix.append(toBinary(scope));
            prefix.append(acl_index_prefix);
            prefix.append(type_str);
            return prefix;
        }
        std::string getScopedACLIndexKeyPrefixWithoutType(const MetadataStoreFoundationDB::AccessEntityScope & scope) const
        {
            std::string prefix;
            prefix.reserve(acl_prefix.size() + sizeof(scope) + acl_index_prefix.size());

            prefix.append(acl_prefix);
            prefix.append(toBinary(scope));
            prefix.append(acl_index_prefix);
            return prefix;
        }

        std::string getScopedACLIndexKeyFromUUID(
            const MetadataStoreFoundationDB::AccessEntityScope & scope,
            const MetadataStoreFoundationDB::AccessEntity & entity,
            const UUID & uuid) const
        {
            std::string key;
            std::string type_str = protoACLTypeToString(entity.entity_type_case());
            key.reserve(acl_prefix.size() + sizeof(scope) + acl_index_prefix.size() + sizeof(UUID) + type_str.size());

            key.append(acl_prefix);
            key.append(toBinary(scope));
            key.append(acl_index_prefix);
            key.append(type_str);
            key.append(toBinary(uuid));
            return key;
        }
        std::string getScopedACLIndexKeyFromUUID(
            const MetadataStoreFoundationDB::AccessEntityScope & scope, const AccessEntityType type, const UUID & uuid) const
        {
            std::string key;
            std::string type_str = ckACLTypeToString(type);
            key.reserve(acl_prefix.size() + sizeof(scope) + acl_index_prefix.size() + type_str.size() + sizeof(UUID));

            key.append(acl_prefix);
            key.append(toBinary(scope));
            key.append(acl_index_prefix);
            key.append(type_str);
            key.append(toBinary(uuid));
            return key;
        }

        UUID getScopedACLUUIDFromKey(const MetadataStoreFoundationDB::AccessEntityScope &, const std::string & key) const
        {
            const auto prefix_size = acl_prefix.size() + sizeof(AccessEntityScope) + acl_meta_prefix.size();
            const auto expect_size = prefix_size + sizeof(UUID);
            if (key.size() != expect_size)
            {
                throw Exception(
                    ErrorCodes::FDB_META_EXCEPTION,
                    "Invalid key of typed acl entry: `{}`. Expect {}, actual {}.",
                    fdb_print_key(key),
                    expect_size,
                    key.size());
            }

            return fromBinary<UUID>(key.data() + prefix_size);
        }
        UUID getScopedACLUUIDFromIndexKey(
            const MetadataStoreFoundationDB::AccessEntityScope &, const std::string & key, AccessEntityType type) const
        {
            const auto prefix_size
                = acl_prefix.size() + sizeof(AccessEntityScope) + acl_index_prefix.size() + ckACLTypeToString(type).size();
            const auto expect_size = prefix_size + sizeof(UUID);
            if (key.size() != expect_size)
            {
                throw Exception(
                    ErrorCodes::FDB_META_EXCEPTION,
                    "Invalid key of typed acl entry: `{}`. Expect {}, actual {}.",
                    fdb_print_key(key),
                    expect_size,
                    key.size());
            }

            return fromBinary<UUID>(key.data() + prefix_size);
        }

        /// Config
        /// {config_prefix}meta\x00{config_name} = Config Value
        /// {config_prefix}version           = Children Version

        std::string config_prefix;
        static constexpr std::string_view config_meta_prefix = "meta\0"sv;
        static constexpr std::string_view config_version = "version";
        std::string getConfigParamKeyPrefix() const
        {
            auto prefix = config_prefix;
            prefix.append(config_meta_prefix);
            return prefix;
        }
        std::string getConfigParamKeyFromName(const std::string & config_name) const { return getConfigParamKeyPrefix() + config_name; }
        std::string getConfigParamNameFromKey(const std::string & key) const
        {
            return key.substr(config_prefix.size() + config_meta_prefix.size());
        }
        std::string getConfigParamChildrenVersionKey() const
        {
            auto key = config_prefix;
            key.append(config_version);
            return key;
        }

        /// Part
        /// {part_prefix}meta\x00{table_uuid}{part_name}     = Part
        /// {part_prefix}disk\x00{table_uuid}{part_name}     = PartDisk
        /// {part_prefix}detached\x00{table_uuid}{part_name} = DetachedPart

        using PartKey = MetadataStoreFoundationDB::PartKey;
        std::string part_prefix;
        static constexpr auto part_meta_prefix = "meta\0"sv;
        static constexpr auto part_disk_prefix = "disk\0"sv;
        static constexpr auto part_detached_prefix = "detached\0"sv;

        std::string getPartPrefix(UUID table_uuid) const
        {
            std::string key;
            key.reserve(part_prefix.size() + part_meta_prefix.size() + sizeof(UUID));

            key.append(part_prefix);
            key.append(part_meta_prefix);
            key.append(toBinary(table_uuid));

            return key;
        }


        std::string getPartKey(const PartKey & part_id) const
        {
            std::string key;
            key.reserve(part_prefix.size() + part_meta_prefix.size() + sizeof(UUID) + part_id.part_name.size());

            key.append(part_prefix);
            key.append(part_meta_prefix);
            key.append(toBinary(part_id.table_uuid));
            key.append(part_id.part_name);
            return key;
        }
        std::string getPartDiskPrefix(const UUID & uuid) const
        {
            std::string key;
            key.reserve(part_prefix.size() + part_disk_prefix.size() + sizeof(UUID));

            key.append(part_prefix);
            key.append(part_disk_prefix);
            key.append(toBinary(uuid));
            return key;
        }

        std::string getPartDiskKey(const PartKey & key) const { return getPartDiskKey(key.table_uuid, key.part_name); }

        /// TODO: Deprecated
        std::string getPartDiskKey(const UUID & uuid, const std::string & part_name) const
        {
            std::string key;
            key.reserve(part_prefix.size() + part_disk_prefix.size() + sizeof(UUID) + part_name.size());

            key.append(part_prefix);
            key.append(part_disk_prefix);
            key.append(toBinary(uuid));
            key.append(part_name);
            return key;
        }
        std::string getDetachedPartPrefix(const UUID & uuid) const
        {
            std::string key;
            key.reserve(part_prefix.size() + part_detached_prefix.size() + sizeof(UUID));

            key.append(part_prefix);
            key.append(part_detached_prefix);
            key.append(toBinary(uuid));
            return key;
        }
        std::string getDetachedPartKey(const UUID & uuid, const std::string & part_name) const
        {
            std::string key;
            key.reserve(part_prefix.size() + part_detached_prefix.size() + sizeof(UUID) + part_name.size());

            key.append(part_prefix);
            key.append(part_detached_prefix);
            key.append(toBinary(uuid));
            key.append(part_name);
            return key;
        }

        /// Dictionary
        /// {dict_prefix}{dict_name} = Dict Value

        std::string dict_prefix;
        std::string getDictInfoKeyFromName(const std::string & dict_name) const { return dict_prefix + dict_name; }
        std::string getDictInfoNameFromKey(const std::string & key) const { return key.substr(dict_prefix.size()); }

        /// XmlFunInfo
        /// {func_prefix}{func_name} = func value

        std::string func_prefix;
        std::string getXmlFuncInfoKeyFromName(const std::string & func_name) const { return func_prefix + func_name; }
        std::string getXmlFuncInfoNameFromKey(const std::string & key) const { return key.substr(func_prefix.size()); }

        /// SqlFuncInfo
        /// {sqlfunc_prefix}{func_name} = func value

        std::string sqlfunc_prefix;
        std::string getSqlFuncInfoKeyFromName(const std::string & func_name) const { return sqlfunc_prefix + func_name; }
        std::string getSqlFuncInfoNameFromKey(const std::string & key) const { return key.substr(sqlfunc_prefix.size()); }
    };
}

MetadataStoreFoundationDB::MetadataStoreFoundationDB(
    const Poco::Util::AbstractConfiguration & config, const String & config_name, const UUID & server_uuid)
    : log(&Poco::Logger::get("MetadataStoreFoundationDB"))
{
    FoundationDBNetwork::ensureStarted();

    /// Parse config
    /// NOTE: Please always update the config example in MetadataStoreFoundationDB.h if needed
    auto const cluster_file_key = config_name + ".cluster_file";
    cluster_file_path = config.getString(cluster_file_key, "/etc/foundationdb/fdb.cluster");

    /// TODO: Support for embedding cluster file in configuration file, for example:
    /// <foundationdb>
    ///   <cluster>
    ///     description:ID@IP:PORT,IP:PORT,...
    ///     description:ID@IP:PORT,IP:PORT,...
    ///   </cluster>
    /// </foundationdb>

    LOG_DEBUG(log, "Using cluster file {}", cluster_file_path);
    if (!fs::exists(cluster_file_path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, fmt::format("Cluster file `{}` not found", cluster_file_path));
    }

    /// The prefix of all key.
    auto key_prefix = config.getString(config_name + ".key_prefix", "clickhouse/");
    LOG_DEBUG(log, "Using key prefix: {}", fdb_print_key(key_prefix));

    /// The key of node scope metadata requires a unique prefix in cluster.
    /// For example, database metadata may be stored in the `{node_meta_key_prefix}{db_prefix}{db_name}`.
    if (server_uuid == UUIDHelpers::Nil)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Server uuid is nil");
    auto const meta_node_id_key = config_name + ".meta_node_id";
    auto meta_node_id = config.getString(meta_node_id_key, std::string(toBinary(server_uuid)));
    LOG_DEBUG(log, "Using meta node id: {}", fdb_print_key(meta_node_id));

    keys = std::make_unique<FoundationDB::MetadataStoreKeyMapper>(key_prefix, meta_node_id);

    /// Init fdb database api
    throwIfFDBError(fdb_create_database(cluster_file_path.c_str(), &db));
}

MetadataStoreFoundationDB::~MetadataStoreFoundationDB()
{
    if (db)
        fdb_database_destroy(db);
}

/// Common

std::shared_ptr<FDBTransaction> MetadataStoreFoundationDB::createTransaction()
{
    FDBTransaction * tr;
    throwIfFDBError(fdb_database_create_transaction(db, &tr));
    return std::shared_ptr<FDBTransaction>(tr, fdb_transaction_destroy);
}

template <class TrxFunc>
auto MetadataStoreFoundationDB::execTransaction(TrxFunc tr_func, int64_t timeout_ms) -> decltype(tr_func(nullptr))
{
    auto tr = createTransaction();
    throwIfFDBError(fdb_transaction_set_option(tr.get(), FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t *>(&timeout_ms), 8));
    while (true)
    {
        try
        {
            return tr_func(tr.get());
        }
        catch (FoundationDBException & e)
        {
            /// Handling transaction errors.
            /// If the transaction can be retried, it will wait for a while to
            /// continue the next loop. Otherwise, it will throw an exception.
            wait(std::shared_ptr<FDBFuture>(fdb_transaction_on_error(tr.get(), e.code), fdb_future_destroy));
            /// TODO: log retry
        }
    }
}

void MetadataStoreFoundationDB::commitTransaction(FDBTransaction * tr)
{
    auto commit_future = fdb_manage_object(fdb_transaction_commit(tr));
    wait(commit_future);
}

template <class Model>
std::vector<std::unique_ptr<Model>> MetadataStoreFoundationDB::listValues(FDBTransaction * tr, const String & prefix)
{
    String range_start = prefix;
    const String range_end = prefix + '\xff';

    std::vector<std::unique_ptr<Model>> res;

    fdb_bool_t more = true;
    for (int iter = 0; more; iter++)
    {
        auto future = fdb_manage_object(fdb_transaction_get_range(
            tr,
            FDB_KEYSEL_FIRST_GREATER_THAN_STRING(range_start),
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(range_end),
            0,
            0,
            FDB_STREAMING_MODE_WANT_ALL,
            iter,
            0,
            0));
        wait(future);

        const FDBKeyValue * kvs = nullptr;
        int kvs_size = 0;
        throwIfFDBError(fdb_future_get_keyvalue_array(future.get(), &kvs, &kvs_size, &more));

        for (int i = 0; i < kvs_size; i++)
        {
            try
            {
                auto obj = deserialize<Model>(kvs[i].value, kvs[i].value_length);
                res.emplace_back(std::move(obj));
            }
            catch (Exception & e)
            {
                e.addMessage(
                    "Fail to deserialize value on fdb, key = {}",
                    fdb_print_key(std::string_view(reinterpret_cast<const char *>(kvs[i].key), kvs[i].key_length)));
            }
        }

        if (!res.empty())
            range_start = std::string(reinterpret_cast<const char *>(kvs[kvs_size - 1].key), kvs[kvs_size - 1].key_length);
    }

    return res;
}


template <class Model>
std::vector<std::pair<std::string, std::unique_ptr<Model>>>
MetadataStoreFoundationDB::listKeyValues(FDBTransaction * tr, const String & prefix)
{
    String range_start = prefix;
    const String range_end = prefix + '\xff';

    std::vector<std::pair<std::string, std::unique_ptr<Model>>> res;

    fdb_bool_t more = true;
    for (int iter = 0; more; iter++)
    {
        auto future = fdb_manage_object(fdb_transaction_get_range(
            tr,
            FDB_KEYSEL_FIRST_GREATER_THAN_STRING(range_start),
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(range_end),
            0,
            0,
            FDB_STREAMING_MODE_WANT_ALL,
            iter,
            0,
            0));
        wait(future);

        const FDBKeyValue * kvs = nullptr;
        int kvs_size = 0;
        throwIfFDBError(fdb_future_get_keyvalue_array(future.get(), &kvs, &kvs_size, &more));

        for (int i = 0; i < kvs_size; i++)
        {
            std::string_view key(reinterpret_cast<const char *>(kvs[i].key), kvs[i].key_length);

            try
            {
                auto obj = deserialize<Model>(kvs[i].value, kvs[i].value_length);
                res.emplace_back(key, std::move(obj));
            }
            catch (Exception & e)
            {
                e.addMessage("Fail to deserialize value on fdb, key = {}", fdb_print_key(key));
            }
        }
        if (!res.empty())
            range_start = res.back().first;
    }
    return res;
}

std::vector<String> MetadataStoreFoundationDB::listKeys(FDBTransaction * tr, const String & prefix)
{
    String range_start = prefix;
    const String range_end = prefix + '\xff';

    std::vector<std::string> res;

    fdb_bool_t more = true;
    for (int iter = 0; more; iter++)
    {
        auto future = fdb_manage_object(fdb_transaction_get_range(
            tr,
            FDB_KEYSEL_FIRST_GREATER_THAN_STRING(range_start),
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(range_end),
            0,
            0,
            FDB_STREAMING_MODE_WANT_ALL,
            iter,
            0,
            0));
        wait(future);

        const FDBKeyValue * kvs = nullptr;
        int kvs_size = 0;
        throwIfFDBError(fdb_future_get_keyvalue_array(future.get(), &kvs, &kvs_size, &more));

        for (int i = 0; i < kvs_size; i++)
            res.push_back(std::string(reinterpret_cast<const char *>(kvs[i].key), kvs[i].key_length));

        if (!res.empty())
            range_start = res.back();
    }

    return res;
}

void MetadataStoreFoundationDB::set(FDBTransaction * tr, const std::string & key, const std::string & data, bool should_exists)
{
    auto future = fdb_manage_object(fdb_transaction_get(tr, FDB_KEY_FROM_STRING(key), false));
    wait(future);

    fdb_bool_t value_present;
    const uint8_t * value;
    int value_length;
    throwIfFDBError(fdb_future_get_value(future.get(), &value_present, &value, &value_length));

    if (value_present && !should_exists)
        throw Exception(ErrorCodes::FDB_META_EXCEPTION, fmt::format("Key `{}` is exists", fdb_print_key(key)));
    else if (!value_present && should_exists)
        throw Exception(ErrorCodes::FDB_META_EXCEPTION, fmt::format("Key `{}` is not exists", fdb_print_key(key)));

    fdb_transaction_set(
        tr,
        reinterpret_cast<const uint8_t *>(key.c_str()),
        static_cast<int>(key.size()),
        reinterpret_cast<const uint8_t *>(data.c_str()),
        static_cast<int>(data.size()));
}

template <class Model>
std::unique_ptr<Model> MetadataStoreFoundationDB::get(FDBTransaction * tr, const std::string & key, bool allow_null)
{
    auto future = fdb_manage_object(fdb_transaction_get(tr, FDB_KEY_FROM_STRING(key), false));
    wait(future);

    fdb_bool_t value_present;
    const uint8_t * value;
    int value_length;
    throwIfFDBError(fdb_future_get_value(future.get(), &value_present, &value, &value_length));

    if (!value_present)
    {
        if (allow_null)
            return nullptr;

        throw Exception(ErrorCodes::FDB_META_EXCEPTION, fmt::format("Key `{}` is not exists", fdb_print_key(key)));
    }

    try
    {
        return deserialize<Model>(value, value_length);
    }
    catch (Exception & e)
    {
        e.addMessage("while deserializing key `{}` in fdb", fdb_print_key(key));
        throw;
    }
}

void MetadataStoreFoundationDB::remove(FDBTransaction * tr, const std::string & key)
{
    fdb_transaction_clear(tr, FDB_KEY_FROM_STRING(key));
}

bool MetadataStoreFoundationDB::isExists(FDBTransaction * tr, const std::string & key)
{
    auto future = fdb_manage_object(fdb_transaction_get_key(tr, FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(key), false));
    wait(future);

    const uint8_t * found_key;
    int found_key_len;

    throwIfFDBError(fdb_future_get_key(future.get(), &found_key, &found_key_len));

    if (key.size() != static_cast<size_t>(found_key_len))
        return false;

    return memcmp(key.data(), reinterpret_cast<const char *>(found_key), found_key_len) == 0;
}

void MetadataStoreFoundationDB::watch(FDBTransaction * tr, const std::string & key, std::function<void(fdb_error_t)> callback)
{
    auto * watch = fdb_transaction_watch(tr, FDB_KEY_FROM_STRING(key));
    FDBFutureCallback * watch_callback = new std::function([callback](FDBFuture * watch_future_ptr) noexcept {
        /// The ownership of watch_future is moved here and must be destroyed.
        auto watch_future = fdb_manage_object(watch_future_ptr);

        try
        {
            callback(fdb_future_get_error(watch_future.get()));
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to invoke the callback of the watcher");
        }
    });

    try
    {
        /// watch_callback will be delete in fdb_future_callback
        throwIfFDBError(fdb_future_set_callback(watch, fdb_future_callback, watch_callback));
    }
    catch (FoundationDBException &)
    {
        /// The watch_callback will not be called.
        /// Destory watch_future and watch_callback.
        fdb_future_destroy(watch);
        delete watch_callback;

        throw;
    }
}

/// Operators on large kv pair. The value of large kv pair means can be larger
/// than 100,000 bytes. But they cannot be list by prefix.
/// A large value is splited into multiple slice:
///   - key \0 0 -> NULL
///   - key \0 1 -> value[0:100,000]
///   - key \0 2 -> value[100,000:200,000]
///   - key \0 3 -> value[200,000:201,000]

static std::string getSliceKey(const std::string & key)
{
    auto slice_key = key;
    slice_key.push_back('\0');
    slice_key.append(toBinary<size_t>(0));
    return slice_key;
}

static std::string getSliceEndKey(const std::string & key)
{
    auto slice_key = key;
    slice_key.push_back('\0');
    slice_key.append(toBinary<size_t>(std::numeric_limits<size_t>::max()));
    return slice_key;
}

static size_t & getSliceIndexFromKey(std::string & slice_key)
{
    return *reinterpret_cast<size_t *>(slice_key.data() + slice_key.size() - sizeof(size_t));
}

static void setLarge(FDBTransaction * tr, const std::string & key, const std::string & data)
{
    auto slice_key = getSliceKey(key);
    auto & slice_idx = getSliceIndexFromKey(slice_key);

    fdb_transaction_set(tr, FDB_KEY_FROM_STRING(slice_key), nullptr, 0);
    slice_idx++;

    const auto * slice_data = reinterpret_cast<const uint8_t *>(data.data());
    size_t remain_size = data.size();
    constexpr size_t max_slice_size = 100'000;

    while (remain_size > 0)
    {
        size_t slice_size = std::min(remain_size, max_slice_size);

        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(slice_key), slice_data, static_cast<int>(slice_size));

        slice_data += slice_size;
        remain_size -= slice_size;
        slice_idx++;
    }

    auto slice_end_key = getSliceEndKey(key);
    fdb_transaction_clear_range(tr, FDB_KEY_FROM_STRING(slice_key), FDB_KEY_FROM_STRING(slice_end_key));
}

template <class Model>
static std::unique_ptr<Model> getLarge(FDBTransaction * tr, const std::string & key, bool allow_null)
{
    auto slice_key = getSliceKey(key);
    auto slice_end_key = getSliceEndKey(key);

    std::string combined_data;
    bool first_iter = true;
    for (fdb_bool_t more = 1; more;)
    {
        auto future = fdb_manage_object(fdb_transaction_get_range(
            tr,
            FDB_KEYSEL_FIRST_GREATER_THAN_STRING(slice_key),
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(slice_end_key),
            0,
            0,
            FDB_STREAMING_MODE_WANT_ALL,
            0,
            0,
            0));
        wait(future);

        const FDBKeyValue * kvs;
        int kvs_len;
        throwIfFDBError(fdb_future_get_keyvalue_array(future.get(), &kvs, &kvs_len, &more));

        if (first_iter && kvs_len == 0)
        {
            if (allow_null)
                return nullptr;

            throw Exception(ErrorCodes::FDB_META_EXCEPTION, fmt::format("Key `{}` is not exists", fdb_print_key(key)));
        }

        for (int i = 0; i < kvs_len; i++)
            combined_data.append(reinterpret_cast<const char *>(kvs[i].value), kvs[i].value_length);

        slice_key.assign(reinterpret_cast<const char *>(kvs[kvs_len - 1].key), kvs[kvs_len - 1].key_length);
    }

    try
    {
        return deserialize<Model>(combined_data.data(), combined_data.size());
    }
    catch (Exception & e)
    {
        e.addMessage("while deserializing key `{}` in fdb", fdb_print_key(key));
        throw;
    }
}

static void removeLarge(FDBTransaction * tr, const std::string & key)
{
    auto slice_key = getSliceKey(key);
    auto slice_end_key = getSliceEndKey(key);
    fdb_transaction_clear_range(tr, FDB_KEY_FROM_STRING(slice_key), FDB_KEY_FROM_STRING(slice_end_key));
}

static bool isExistsLarge(FDBTransaction * tr, const std::string & key)
{
    auto slice_key = getSliceKey(key);
    auto future = fdb_manage_object(fdb_transaction_get(tr, FDB_KEY_FROM_STRING(slice_key), false));
    wait(future);

    fdb_bool_t exists;
    const uint8_t * data;
    int len;

    throwIfFDBError(fdb_future_get_value(future.get(), &exists, &data, &len));

    return exists;
}

void MetadataStoreFoundationDB::clearPrefix(const std::string & key_prefix)
{
    execTransaction([&](FDBTransaction * tr) {
        const auto & key_start = key_prefix;
        const auto key_end = key_start + '\xff';
        fdb_transaction_clear_range(tr, FDB_KEY_FROM_STRING(key_start), FDB_KEY_FROM_STRING(key_end));
        commitTransaction(tr);
    });
}


/// Module

void MetadataStoreFoundationDB::markFirstBootCompleted()
{
    auto key = keys->loadedFlagKey();
    execTransaction([&](FDBTransaction * tr) {
        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(key), nullptr, 0);
        commitTransaction(tr);
    });
}

bool MetadataStoreFoundationDB::isFirstBoot()
{
    return !isExists(keys->loadedFlagKey());
}

/// Database

void MetadataStoreFoundationDB::addDatabaseMeta(const MetadataDatabase & db_meta, const DatabaseKey & db_key, const UUID & uuid)
{
    std::string key = keys->getDBKeyFromName(db_key);
    const auto value = db_meta.SerializeAsString();

    if (uuid == UUID{})
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The database on fdb must have uuid");
    set(key, db_meta.SerializeAsString(), false);
}

std::unique_ptr<Proto::MetadataDatabase> MetadataStoreFoundationDB::getDatabaseMeta(const DatabaseKey & key, bool allow_null)
{
    return get<MetadataDatabase>(keys->getDBKeyFromName(key), allow_null);
}

std::vector<std::unique_ptr<Proto::MetadataDatabase>> MetadataStoreFoundationDB::listDatabases()
{
    return listValues<Proto::MetadataDatabase>(keys->db_prefix);
}

bool MetadataStoreFoundationDB::isExistsDatabase(const DatabaseKey & name)
{
    return isExists(keys->getDBKeyFromName(name));
}

void MetadataStoreFoundationDB::updateDatabaseMeta(const DatabaseKey & name, MetadataDatabase & db_meta)
{
    std::string key = keys->getDBKeyFromName(name);
    set(key, db_meta.SerializeAsString(), true);
}

void MetadataStoreFoundationDB::removeDatabaseMeta(const DatabaseKey & name)
{
    const auto key = keys->getDBKeyFromName(name);

    execTransaction([&](FDBTransaction * tr) {
        fdb_transaction_clear(tr, FDB_KEY_FROM_STRING(key));

        commitTransaction(tr);
    });
}

void MetadataStoreFoundationDB::clearDatabase()
{
    clearPrefix(keys->db_prefix);
}

void MetadataStoreFoundationDB::renameDatabase(const DatabaseKey & name, const DatabaseKey & new_name)
{
    std::string key = keys->getDBKeyFromName(name);
    std::string new_key = keys->getDBKeyFromName(new_name);

    execTransaction([&](FDBTransaction * tr) {
        auto db_meta = get<MetadataDatabase>(tr, key);
        if (!db_meta)
            throw Exception(
                ErrorCodes::FDB_META_EXCEPTION,
                fmt::format("Cannot rename database `{}` to `{}`: `{}` is not exists in fdb", name, new_name, name));

        if (isExists(tr, new_key))
            throw Exception(
                ErrorCodes::FDB_META_EXCEPTION,
                fmt::format("Cannot rename database `{}` to `{}`: `{}` is exists in fdb", name, new_name, new_name));


        // Remove old key
        remove(tr, key);

        // Do rename
        db_meta->set_name(new_name);
        auto new_value = db_meta->SerializeAsString();
        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(new_key), FDB_VALUE_FROM_STRING(new_value));

        commitTransaction(tr);
    });
}

/// Table

void MetadataStoreFoundationDB::addTableMeta(const MetadataTable & tb_meta, const TableKey & table_key)
{
    std::string value = tb_meta.SerializeAsString();
    auto time = toBinary(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
    execTransaction([&](FDBTransaction * tr) {
        const auto key = keys->getTableMetaKey(table_key);
        const auto time_key = keys->getTableTimeKey(table_key);
        if (isExists(tr, key))
            throw Exception(ErrorCodes::FDB_META_EXCEPTION, fmt::format("Key `{}` is exists", fdb_print_key(key)));
        if (isExists(tr, time_key))
            throw Exception(ErrorCodes::FDB_META_EXCEPTION, fmt::format("Key `{}` is exists", fdb_print_key(key)));
        fdb_transaction_set(
            tr,
            reinterpret_cast<const uint8_t *>(key.c_str()),
            static_cast<int>(key.size()),
            reinterpret_cast<const uint8_t *>(value.c_str()),
            static_cast<int>(value.size()));
        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(time_key), FDB_VALUE_FROM_STRING(time));
        commitTransaction(tr);
    });
}

std::unique_ptr<Proto::MetadataTable> MetadataStoreFoundationDB::getTableMeta(const TableKey & table_key)
{
    return get<Proto::MetadataTable>(keys->getTableMetaKey(table_key));
}

std::unique_ptr<Proto::MetadataTable> MetadataStoreFoundationDB::getDroppedTableMeta(const UUID & table_uuid)
{
    return get<Proto::MetadataTable>(keys->getDroppedTableKey(table_uuid));
}

void MetadataStoreFoundationDB::updateTableMeta(const MetadataTable & tb_meta, const TableKey & table_key)
{
    std::string value = tb_meta.SerializeAsString();
    auto time = toBinary(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
    execTransaction([&](FDBTransaction * tr) {
        const auto key = keys->getTableMetaKey(table_key);
        const auto time_key = keys->getTableTimeKey(table_key);
        if (!isExists(tr, key))
            throw Exception(ErrorCodes::FDB_META_EXCEPTION, fmt::format("Key `{}` is not exists", fdb_print_key(key)));
        if (!isExists(tr, time_key))
            throw Exception(ErrorCodes::FDB_META_EXCEPTION, fmt::format("Key `{}` is not exists", fdb_print_key(key)));
        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));
        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(time_key), FDB_VALUE_FROM_STRING(time));
        commitTransaction(tr);
    });
}

void MetadataStoreFoundationDB::removeTableMeta(const TableKey & table_key)
{
    execTransaction([&](FDBTransaction * tr) {
        const auto key = keys->getTableMetaKey(table_key);
        const auto time_key = keys->getTableTimeKey(table_key);
        const auto detach_key = keys->getTableDetachFlagKey(table_key);
        remove(tr, key);
        remove(tr, time_key);
        remove(tr, detach_key);
        commitTransaction(tr);
    });
}

void MetadataStoreFoundationDB::removeDroppedTableMeta(const UUID & table_uuid)
{
    auto key = keys->getDroppedTableKey(table_uuid);
    auto time_key = keys->getTableDropTimeKey(table_uuid);
    execTransaction([&](auto tr) {
        if (!isExists(tr, key))
            throw Exception(
                ErrorCodes::FDB_META_EXCEPTION,
                fmt::format("Cannot remove dropped Table meta `{}` . Dropped table meta is not exists in fdb", toString(table_uuid)));
        if (!isExists(tr, time_key))
            throw Exception(
                ErrorCodes::FDB_META_EXCEPTION,
                fmt::format("Cannot remove dropped Table time `{}`. Table time {} is not exists in fdb. ", time_key, time_key));
        remove(tr, key);
        remove(tr, time_key);
        commitTransaction(tr);
    });
}

void MetadataStoreFoundationDB::renameTable(
    const TableKey & old_table_key, const TableKey & new_table_key, const MetadataTable & new_tb_meta)
{
    auto time = toBinary(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

    execTransaction([&](FDBTransaction * tr) {
        const auto key = keys->getTableMetaKey(old_table_key);
        const auto old_time_key = keys->getTableTimeKey(old_table_key);
        const auto new_key = keys->getTableMetaKey(new_table_key);
        const auto new_time_key = keys->getTableTimeKey(new_table_key);

        if (!isExists(tr, key))
            throw Exception(
                ErrorCodes::FDB_META_EXCEPTION,
                fmt::format("Cannot rename Table `{}`. This table is not exists in fdb", old_table_key.table_name));

        if (isExists(tr, new_key))
            throw Exception(
                ErrorCodes::FDB_META_EXCEPTION,
                fmt::format(
                    "Cannot rename Table `{}` to '{}'. {} is exists in fdb",
                    old_table_key.table_name,
                    new_table_key.table_name,
                    new_table_key.table_name));

        // Remove old key
        remove(tr, key);
        remove(tr, old_time_key);

        // Do rename
        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(new_key), FDB_VALUE_FROM_STRING(new_tb_meta.SerializeAsString()));
        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(new_time_key), FDB_VALUE_FROM_STRING(time));
        commitTransaction(tr);
    });
}

std::string MetadataStoreFoundationDB::renameTableToDropped(const TableKey & table_key, const UUID & table_uuid)
{
    auto time = toBinary(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
    auto drop_table_key = keys->getDroppedTableKey(table_uuid);
    auto drop_time_key = keys->getTableDropTimeKey(table_uuid);

    execTransaction([&](FDBTransaction * tr) {
        const auto key = keys->getTableMetaKey(table_key);
        const auto time_key = keys->getTableTimeKey(table_key);
        const auto detach_key = keys->getTableDetachFlagKey(table_key);

        auto tb_meta = get<MetadataTable>(tr, key);
        if (!tb_meta)
            throw Exception(
                ErrorCodes::FDB_META_EXCEPTION,
                fmt::format("Cannot drop Table `{}`. This table is not exists in fdb", table_key.table_name));

        if (isExists(tr, drop_table_key))
            throw Exception(
                ErrorCodes::FDB_META_EXCEPTION,
                fmt::format("Cannot drop Table `{}`. Table {} is already dropped. ", table_key.table_name, table_key.table_name));

        // Remove old key
        remove(tr, key);
        remove(tr, time_key);
        remove(tr, detach_key);

        // Do rename
        auto tb_meta_value = tb_meta->SerializeAsString();
        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(drop_table_key), FDB_VALUE_FROM_STRING(tb_meta_value));
        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(drop_time_key), FDB_VALUE_FROM_STRING(time));

        commitTransaction(tr);
    });
    return drop_table_key;
}


void MetadataStoreFoundationDB::exchangeTableMeta(
    const TableKey & old_table_key,
    const TableKey & new_table_key,
    const MetadataTable & table_meta,
    const MetadataTable & other_table_meta)
{
    auto time = toBinary(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

    execTransaction([&](FDBTransaction * tr) {
        const auto key = keys->getTableMetaKey(old_table_key);
        const auto time_key = keys->getTableTimeKey(old_table_key);
        const auto other_key = keys->getTableMetaKey(new_table_key);
        const auto time_other_key = keys->getTableTimeKey(new_table_key);

        auto tb_meta = get<MetadataTable>(tr, key);
        auto other_tb_meta = get<MetadataTable>(tr, other_key);
        if (!tb_meta)
            throw Exception(
                ErrorCodes::FDB_META_EXCEPTION,
                fmt::format("Cannot rename Table `{}`. This table is not exists in fdb", old_table_key.table_name));
        if (!other_tb_meta)
            throw Exception(
                ErrorCodes::FDB_META_EXCEPTION,
                fmt::format("Cannot rename Table `{}`. This table is not exists in fdb", new_table_key.table_name));

        // Do exchange
        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(other_table_meta.SerializeAsString()));
        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(time_key), FDB_VALUE_FROM_STRING(time));
        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(other_key), FDB_VALUE_FROM_STRING(table_meta.SerializeAsString()));
        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(time_other_key), FDB_VALUE_FROM_STRING(time));
        commitTransaction(tr);
    });
}

bool MetadataStoreFoundationDB::isExistsTable(const TableKey & table_key)
{
    return isExists(keys->getTableMetaKey(table_key));
}

void MetadataStoreFoundationDB::updateDetachTableStatus(const TableKey & table_key, const bool & detached)
{
    execTransaction([&](FDBTransaction * tr) {
        std::string key = keys->getTableMetaKey(table_key);
        std::string detached_key = keys->getTableDetachFlagKey(table_key);

        if (detached)
        {
            if (isExists(tr, detached_key))
                throw Exception(ErrorCodes::FDB_META_EXCEPTION, "Detach flag key {} already exists", fdb_print_key(detached_key));

            fdb_transaction_set(tr, FDB_KEY_FROM_STRING(detached_key), nullptr, 0);
        }
        else
        {
            if (!isExists(tr, detached_key))
                throw Exception(ErrorCodes::FDB_META_EXCEPTION, "Detach flag key {} not found", fdb_print_key(detached_key));

            remove(detached_key);
        }
        commitTransaction(tr);
    });
}

time_t MetadataStoreFoundationDB::getTimeByKey(const std::string & time_key)
{
    return execTransaction([&](FDBTransaction * tr) {
        auto future = fdb_manage_object(fdb_transaction_get(tr, FDB_KEY_FROM_STRING(time_key), false));
        wait(future);

        fdb_bool_t value_present;
        const uint8_t * value;
        int value_length;
        throwIfFDBError(fdb_future_get_value(future.get(), &value_present, &value, &value_length));

        if (!value_present)
        {
            throw Exception(ErrorCodes::FDB_META_EXCEPTION, fmt::format("Key `{}` is not exists", fdb_print_key(time_key)));
        }

        return fromBinary<time_t>(reinterpret_cast<const char *>(value));
    });
}

time_t MetadataStoreFoundationDB::getModificationTime(const TableKey & table_key)
{
    return getTimeByKey(keys->getTableTimeKey(table_key));
}

time_t MetadataStoreFoundationDB::getDropTime(const UUID & table_uuid)
{
    return getTimeByKey(keys->getTableDropTimeKey(table_uuid));
}

std::vector<std::unique_ptr<Proto::MetadataTable>> MetadataStoreFoundationDB::listAllTableMeta(const UUID & db_uuid, bool include_detached)
{
    return execTransaction([&](FDBTransaction * tr) {
        auto pairs = listKeyValues<MetadataTable>(tr, keys->getTableMetaKeyPrefix(db_uuid));
        const auto detached_key_prefix = keys->getTableDetachFlagKeyPrefix(db_uuid);

        std::set<std::string> detached_tables;

        if (!include_detached)
        {
            for (const auto & key : listKeys(tr, detached_key_prefix))
                detached_tables.emplace(keys->getTableNameFromDetachedKey(key));
        }

        std::vector<std::unique_ptr<MetadataTable>> filted_tables;

        for (auto & pair : pairs)
        {
            if (!detached_tables.contains(keys->getTableNameFromKey(pair.first)))
                filted_tables.emplace_back(std::move(pair.second));
        }

        return filted_tables;
    });
}

std::vector<std::unique_ptr<Proto::MetadataTable>> MetadataStoreFoundationDB::listAllDroppedTableMeta()
{
    return listValues<MetadataTable>(keys->getDroppedTableMetaKeyPrefix());
}


bool MetadataStoreFoundationDB::isDetached(const TableKey & table_key)
{
    return isExists(keys->getTableDetachFlagKey(table_key));
}
bool MetadataStoreFoundationDB::isDropped(const UUID & table_uuid)
{
    return isExists(keys->getDroppedTableKey(table_uuid));
}

/// Config

void MetadataStoreFoundationDB::addConfigParamMeta(const Proto::MetadataConfigParam & config,const ConfigKey & config_key)
{
    std::string key = keys->getConfigParamKeyFromName(config_key);
    std::string value = config.SerializeAsString();
    const auto version_key = keys->getConfigParamChildrenVersionKey();

    execTransaction([&](FDBTransaction * tr) {
        set(tr, key, value, false);
        fdb_transaction_atomic_op(tr, FDB_KEY_FROM_STRING(version_key), FDB_ATOMIC_PLUS_ONE);
        commitTransaction(tr);
    });
}

MetadataStoreFoundationDB::MetadataConfigParamPtr MetadataStoreFoundationDB::getConfigParamMeta(const ConfigKey & name)
{
    return get<MetadataConfigParam>(keys->getConfigParamKeyFromName(name));
}

void MetadataStoreFoundationDB::addBunchConfigParamMeta(const std::vector<std::unique_ptr<MetadataConfigParam>> & configs)
{
    const auto version_key = keys->getConfigParamChildrenVersionKey();
    execTransaction([&](auto tr) {
        for (auto & gc : configs)
        {
            std::string name = keys->getConfigParamKeyFromName(gc->name());
            std::string data = gc->SerializeAsString();
            set(tr, name, data, false); //FIXME:Not sure what to do with records that already exist
        }
        fdb_transaction_atomic_op(tr, FDB_KEY_FROM_STRING(version_key), FDB_ATOMIC_PLUS_ONE);
        commitTransaction(tr);
    });
}

bool MetadataStoreFoundationDB::isExistsConfigParamMeta(const ConfigKey & name)
{
    return isExists(keys->getConfigParamKeyFromName(name));
}

std::vector<std::unique_ptr<MetadataStoreFoundationDB::MetadataConfigParam>> MetadataStoreFoundationDB::listAllConfigParamMeta()
{
    const auto key_prefix = keys->getConfigParamKeyPrefix();
    return listValues<MetadataConfigParam>(key_prefix);
}

void MetadataStoreFoundationDB::updateConfigParamMeta(Proto::MetadataConfigParam & config, const ConfigKey & name)
{
    std::string key = keys->getConfigParamKeyFromName(name);
    std::string value = config.SerializeAsString();
    set(key, value, true);
}

void MetadataStoreFoundationDB::removeConfigParamMeta(const ConfigKey & name)
{
    const auto version_key = keys->getConfigParamChildrenVersionKey();
    execTransaction([&](FDBTransaction * tr) {
        remove(tr, keys->getConfigParamKeyFromName(name));
        fdb_transaction_atomic_op(tr, FDB_KEY_FROM_STRING(version_key), FDB_ATOMIC_PLUS_ONE);
        commitTransaction(tr);
    });
}

std::string MetadataStoreFoundationDB::getReadableDatabaseKey(const std::string & db_name)
{
    return fdb_print_key(keys->getDBKeyFromName(db_name));
}

std::shared_ptr<FoundationDB::Proto::MetadataConfigParam>
MetadataStoreFoundationDB::getAndWatchConfigParamMeta(const ConfigKey & name, std::shared_ptr<Poco::Event> event)
{
    const auto key = keys->getConfigParamKeyFromName(name);
    return execTransaction([&](FDBTransaction * tr) {
        auto config = get<MetadataConfigParam>(key, false);
        watch(tr, key, [event](fdb_error_t) { event->set(); });
        commitTransaction(tr);
        return config;
    });
}

std::vector<std::unique_ptr<FoundationDB::Proto::MetadataConfigParam>>
MetadataStoreFoundationDB::watchAllConfigParamMeta(std::shared_ptr<Poco::Event> event)
{
    const auto key_prefix = keys->getConfigParamKeyPrefix();
    const auto version_key = keys->getConfigParamChildrenVersionKey();

    return execTransaction([&](FDBTransaction * tr) {
        auto list = listValues<MetadataConfigParam>(key_prefix);
        watch(tr, version_key, [event](fdb_error_t) { event->set(); });
        commitTransaction(tr);
        return list;
    });
}

/// Access

bool MetadataStoreFoundationDB::addAccessEntity(
    const AccessEntityScope & scope, const UUID & uuid, const AccessEntity & entity, bool replace_if_exists, bool throw_if_exists)
{
    const auto key = keys->getScopedACLKeyFromUUID(scope, uuid);
    const auto value = entity.SerializeAsString();
    const auto index_key = keys->getScopedACLIndexKeyFromUUID(scope, entity, uuid);
    const auto index_value = entity.name();
    return execTransaction([&](FDBTransaction * tr) {
        bool exists = isExists(tr, key);

        if (exists && throw_if_exists)
            throw Exception(ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS, "Access entity {} already exists in fdb", toString(uuid));

        bool should_add = !exists || replace_if_exists;
        if (should_add)
        {
            fdb_transaction_set(tr, FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));
            fdb_transaction_set(tr, FDB_KEY_FROM_STRING(index_key), FDB_VALUE_FROM_STRING(index_value));
        }

        commitTransaction(tr);
        return should_add;
    });
}

bool MetadataStoreFoundationDB::removeAccessEntity(
    const AccessEntityScope & scope, const UUID & uuid, AccessEntityType type, bool throw_if_not_exists)
{
    const auto key = keys->getScopedACLKeyFromUUID(scope, uuid);
    const auto index_key = keys->getScopedACLIndexKeyFromUUID(scope, type, uuid);
    return execTransaction([&](FDBTransaction * tr) {
        bool exists = isExists(tr, key);

        if (!exists && throw_if_not_exists)
            throw Exception(ErrorCodes::ACCESS_ENTITY_NOT_FOUND, "Access entity {} not found in fdb", toString(uuid));

        if (exists)
        {
            remove(tr, key);
            remove(tr, index_key);
        }
        commitTransaction(tr);
        return exists;
    });
}

bool MetadataStoreFoundationDB::updateAccessEntity(
    const AccessEntityScope & scope, const UUID & uuid, const AccessEntity & entity, bool throw_if_not_exists)
{
    const auto key = keys->getScopedACLKeyFromUUID(scope, uuid);
    const auto idx_key = keys->getScopedACLIndexKeyFromUUID(scope, entity, uuid);
    const auto value = entity.SerializeAsString();

    return execTransaction([&](FDBTransaction * tr) {
        auto name = get<std::string>(tr, idx_key, true);

        if (name == nullptr && throw_if_not_exists)
            throw Exception(ErrorCodes::ACCESS_ENTITY_NOT_FOUND, "Access entity {} not found in fdb", toString(uuid));

        if (name != nullptr)
        {
            fdb_transaction_set(tr, FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));
            if (*name != entity.name())
            {
                fdb_transaction_set(tr, FDB_KEY_FROM_STRING(idx_key), FDB_VALUE_FROM_STRING(entity.name()));
            }
        }

        commitTransaction(tr);
        return name != nullptr;
    });
}

std::unique_ptr<Proto::AccessEntity>
MetadataStoreFoundationDB::getAccessEntity(const AccessEntityScope & scope, const UUID & uuid, bool throw_if_not_exists)
{
    const auto key = keys->getScopedACLKeyFromUUID(scope, uuid);

    return get<Proto::AccessEntity>(key, !throw_if_not_exists);
}

std::vector<UUID> MetadataStoreFoundationDB::listAccessEntity(const AccessEntityScope & scope)
{
    std::string key_prefix = keys->getScopedACLKeyPrefix(scope);
    std::vector<UUID> uuids;
    for (const auto & key : listKeys(key_prefix))
        uuids.emplace_back(keys->getScopedACLUUIDFromKey(scope, key));
    return uuids;
}

bool MetadataStoreFoundationDB::existsAccessEntity(const AccessEntityScope & scope, const UUID & uuid)
{
    const auto key = keys->getScopedACLKeyFromUUID(scope, uuid);
    return isExists(key);
}

std::vector<std::pair<UUID, std::string>>
MetadataStoreFoundationDB::getAccessEntityListsByType(const AccessEntityScope & scope, AccessEntityType type)
{
    std::string key_prefix = keys->getScopedACLIndexKeyPrefix(scope, type);
    std::vector<std::pair<UUID, std::string>> res;
    for (const auto & [key, value] : listKeyValues<String>(key_prefix))
    {
        res.emplace_back(keys->getScopedACLUUIDFromIndexKey(scope, key, type), *value);
    }
    return res;
}
std::vector<std::pair<UUID, std::unique_ptr<Proto::AccessEntity>>>
MetadataStoreFoundationDB::getAllAccessEntities(const AccessEntityScope & scope)
{
    std::string key_prefix = keys->getScopedACLKeyPrefix(scope);
    std::vector<std::pair<UUID, std::unique_ptr<Proto::AccessEntity>>> res;
    for (auto & kv : listKeyValues<Proto::AccessEntity>(key_prefix))
    {
        UUID uuid = keys->getScopedACLUUIDFromKey(scope, kv.first);
        res.push_back({uuid, std::move(kv.second)});
    }
    return res;
}
void MetadataStoreFoundationDB::clearAccessEntities(const AccessEntityScope & scope)
{
    std::string key_prefix = keys->getScopedACLKeyPrefix(scope);
    std::string idx_prefix = keys->getScopedACLIndexKeyPrefixWithoutType(scope);
    execTransaction([&](FDBTransaction * tr) {
        const auto & key_start = key_prefix;
        const auto key_end = key_start + '\xff';
        fdb_transaction_clear_range(tr, FDB_KEY_FROM_STRING(key_start), FDB_KEY_FROM_STRING(key_end));

        const auto & idx_start = idx_prefix;
        const auto idx_end = idx_start + '\xff';
        fdb_transaction_clear_range(tr, FDB_KEY_FROM_STRING(idx_start), FDB_KEY_FROM_STRING(idx_end));

        commitTransaction(tr);
    });
}

/// Part

void MetadataStoreFoundationDB::addPartMeta(const MergeTreePartDiskMeta & disk, const MergeTreePartMeta & part, const PartKey & part_key)
{
    const auto part_meta_key = keys->getPartKey(part_key);
    const auto disk_meta_key = keys->getPartDiskKey(part_key);
    execTransaction([&](FDBTransaction * tr) {
        set(tr, disk_meta_key, disk.SerializeAsString(), false);
        if (isExistsLarge(tr, part_meta_key))
            throw Exception(ErrorCodes::FDB_META_EXCEPTION, "Part meta key is exists: {}", fdb_print_key(part_meta_key));
        setLarge(tr, part_meta_key, part.SerializeAsString());
        commitTransaction(tr);
    });
}

std::unique_ptr<Proto::MergeTreePartMeta> MetadataStoreFoundationDB::getPartMeta(const PartKey & part_key)
{
    const auto key = keys->getPartKey(part_key);
    return execTransaction([&](FDBTransaction * tr) { return getLarge<Proto::MergeTreePartMeta>(tr, key, false); });
}

void MetadataStoreFoundationDB::removePartMeta(const PartKey & part_key)
{
    const auto part_meta_key = keys->getPartKey(part_key);
    const auto disk_meta_key = keys->getPartDiskKey(part_key);
    execTransaction([&](FDBTransaction * tr) {
        removeLarge(tr, part_meta_key);
        remove(tr, disk_meta_key);
        commitTransaction(tr);
    });
}

bool MetadataStoreFoundationDB::isExistsPart(const PartKey & part_key)
{
    const auto key = keys->getPartKey(part_key);
    return execTransaction([&](FDBTransaction * tr) { return isExistsLarge(tr, key); });
}

std::vector<std::unique_ptr<Proto::MergeTreePartDiskMeta>> MetadataStoreFoundationDB::listParts(const UUID & table_uuid)
{
    return listValues<Proto::MergeTreePartDiskMeta>(keys->getPartDiskPrefix(table_uuid));
}

std::unique_ptr<Proto::MergeTreeDetachedPartMeta> MetadataStoreFoundationDB::getDetachedPartMeta(const PartKey & part_key)
{
    const auto key = keys->getDetachedPartKey(part_key.table_uuid, part_key.part_name);
    return get<Proto::MergeTreeDetachedPartMeta>(key);
}
std::vector<std::unique_ptr<Proto::MergeTreeDetachedPartMeta>> MetadataStoreFoundationDB::listDetachedParts(const UUID & table_uuid)
{
    return listValues<Proto::MergeTreeDetachedPartMeta>(keys->getDetachedPartPrefix(table_uuid));
}
void MetadataStoreFoundationDB::addPartDetachedMeta(const MergeTreeDetachedPartMeta & part, const PartKey & part_key)
{
    const auto key = keys->getDetachedPartKey(part_key.table_uuid, part_key.part_name);
    set(key, part.SerializeAsString(), false);
}
void MetadataStoreFoundationDB::removeDetachedPartMeta(const PartKey & part_key)
{
    const auto key = keys->getDetachedPartKey(part_key.table_uuid, part_key.part_name);
    remove(key);
}
bool MetadataStoreFoundationDB::isExistsDetachedPart(const PartKey & part_key)
{
    const auto key = keys->getDetachedPartKey(part_key.table_uuid, part_key.part_name);
    return isExists(key);
}
/// Dictionary

bool MetadataStoreFoundationDB::addOneDictionary(const DictInfo & dict, const std::string & dict_name)
{
    std::string key = keys->getDictInfoKeyFromName(dict_name);
    std::string value = dict.SerializeAsString();
    set(key, value, false);
    return true;
}

bool MetadataStoreFoundationDB::existOneDictionary(const std::string & dict_name)
{
    return isExists(keys->getDictInfoKeyFromName(dict_name));
}

bool MetadataStoreFoundationDB::deleteOneDictionary(const std::string & dict_name)
{
    std::string key = keys->getDictInfoKeyFromName(dict_name);
    return execTransaction([&](FDBTransaction * tr) {
        bool exists = isExists(tr, key);

        if (!exists)
            throw Exception(ErrorCodes::ACCESS_ENTITY_NOT_FOUND, "Access entity {} not found in fdb", fdb_print_key(key));

        if (exists)
            remove(tr, key);

        commitTransaction(tr);
        return exists;
    });
}

std::unique_ptr<Proto::DictInfo> MetadataStoreFoundationDB::getOneDictionary(const std::string & dict_name)
{
    return get<Proto::DictInfo>(keys->getDictInfoKeyFromName(dict_name));
}

bool MetadataStoreFoundationDB::updateOneDictionary(const std::string & dict_name, const DictInfo & new_dict)
{
    std::string key = keys->getDictInfoKeyFromName(dict_name);
    const auto value = new_dict.SerializeAsString();

    return execTransaction([&](FDBTransaction * tr) {
        bool exists = isExists(tr, key);

        if (!exists)
            throw Exception(ErrorCodes::ACCESS_ENTITY_NOT_FOUND, "Access entity {} not found in fdb", dict_name);

        if (exists)
            fdb_transaction_set(tr, FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));

        commitTransaction(tr);
        return exists;
    });
}

std::vector<std::unique_ptr<Proto::DictInfo>> MetadataStoreFoundationDB::getAllDictionaries()
{
    return listValues<Proto::DictInfo>(keys->dict_prefix);
}

void MetadataStoreFoundationDB::clearAllDictionaries()
{
    clearPrefix(keys->dict_prefix);
}


/// XmlFuncInfo

bool MetadataStoreFoundationDB::addOneFunction(const XmlFuncInfo & func, const std::string & func_name)
{
    std::string key = keys->getXmlFuncInfoKeyFromName(func_name);
    std::string value = func.SerializeAsString();
    set(key, value, false);
    return true;
}
bool MetadataStoreFoundationDB::existOneFunction(const std::string & func_name)
{
    return isExists(keys->getXmlFuncInfoKeyFromName(func_name));
}

bool MetadataStoreFoundationDB::deleteOneFunction(const std::string & func_name)
{
    std::string key = keys->getXmlFuncInfoKeyFromName(func_name);
    return execTransaction([&](FDBTransaction * tr) {
        bool exists = isExists(tr, key);

        if (!exists)
            throw Exception(ErrorCodes::ACCESS_ENTITY_NOT_FOUND, "Access entity {} not found in fdb", fdb_print_key(key));

        if (exists)
            remove(tr, key);

        commitTransaction(tr);
        return exists;
    });
}

std::unique_ptr<Proto::XmlFuncInfo> MetadataStoreFoundationDB::getOneFunction(const std::string & func_name)
{
    return get<Proto::XmlFuncInfo>(keys->getXmlFuncInfoKeyFromName(func_name));
}

bool MetadataStoreFoundationDB::updateOneFunction(const std::string & func_name, const XmlFuncInfo & new_func)
{
    std::string key = keys->getXmlFuncInfoKeyFromName(func_name);
    const auto value = new_func.SerializeAsString();

    return execTransaction([&](FDBTransaction * tr) {
        bool exists = isExists(tr, key);

        if (!exists)
            throw Exception(ErrorCodes::ACCESS_ENTITY_NOT_FOUND, "XmlFuncInfo entity {} not found in fdb", func_name);

        if (exists)
            fdb_transaction_set(tr, FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));

        commitTransaction(tr);
        return exists;
    });
}

std::vector<std::unique_ptr<Proto::XmlFuncInfo>> MetadataStoreFoundationDB::getAllFunctions()
{
    return listValues<Proto::XmlFuncInfo>(keys->func_prefix);
}

void MetadataStoreFoundationDB::clearAllFunctions()
{
    clearPrefix(keys->func_prefix);
}

/// SQLFuncInfo

bool MetadataStoreFoundationDB::addSQLFunction(const SqlFuncInfo & func, const std::string & func_name)
{
    std::string key = keys->getSqlFuncInfoKeyFromName(func_name);
    std::string value = func.SerializeAsString();
    set(key, value, false);
    return true;
}
bool MetadataStoreFoundationDB::existSQLFunction(const std::string & func_name)
{
    return isExists(keys->getSqlFuncInfoKeyFromName(func_name));
}

bool MetadataStoreFoundationDB::deleteSQLFunction(const std::string & func_name)
{
    std::string key = keys->getSqlFuncInfoKeyFromName(func_name);
    return execTransaction([&](FDBTransaction * tr) {
        bool exists = isExists(tr, key);

        if (!exists)
            throw Exception(ErrorCodes::ACCESS_ENTITY_NOT_FOUND, "SqlFuncInfo entity {} not found in fdb", fdb_print_key(key));

        if (exists)
            remove(tr, key);

        commitTransaction(tr);
        return exists;
    });
}

std::unique_ptr<Proto::SqlFuncInfo> MetadataStoreFoundationDB::getSQLFunction(const std::string & func_name)
{
    return get<Proto::SqlFuncInfo>(keys->getSqlFuncInfoKeyFromName(func_name));
}

bool MetadataStoreFoundationDB::updateSQLFunction(const std::string & func_name, const SqlFuncInfo & new_func)
{
    std::string key = keys->getSqlFuncInfoKeyFromName(func_name);
    const auto value = new_func.SerializeAsString();

    return execTransaction([&](FDBTransaction * tr) {
        bool exists = isExists(tr, key);

        if (!exists)
            throw Exception(ErrorCodes::ACCESS_ENTITY_NOT_FOUND, "SqlFuncInfo entity {} not found in fdb", func_name);

        if (exists)
            fdb_transaction_set(tr, FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));

        commitTransaction(tr);
        return exists;
    });
}

std::vector<std::unique_ptr<Proto::SqlFuncInfo>> MetadataStoreFoundationDB::getAllSqlFunctions()
{
    return listValues<Proto::SqlFuncInfo>(keys->sqlfunc_prefix);
}

void MetadataStoreFoundationDB::clearAllSqlFunctions()
{
    clearPrefix(keys->sqlfunc_prefix);
}
}

