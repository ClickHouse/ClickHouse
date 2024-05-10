#include <memory>
#include <Storages/StorageKeeperMap.h>

#include <Columns/ColumnString.h>

#include <Databases/DatabaseReplicated.h>

#include <Core/NamesAndTypes.h>
#include <Core/UUID.h>
#include <Core/ServerUUID.h>

#include <DataTypes/DataTypeString.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/MutationsInterpreter.h>

#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/formatAST.h>

#include <Processors/ISource.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/KVStorageUtils.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

#include <Backups/BackupEntriesCollector.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/IBackupEntriesLazyBatch.h>
#include <Backups/BackupEntryFromAppendOnlyFile.h>
#include <Backups/BackupEntryReference.h>
#include <Backups/IBackup.h>
#include <Backups/IRestoreCoordination.h>
#include <Backups/RestorerFromBackup.h>
#include <Backups/WithRetries.h>

#include <Disks/IO/createReadBufferFromFileBase.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <base/types.h>

#include <boost/algorithm/string/classification.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int KEEPER_EXCEPTION;
    extern const int LOGICAL_ERROR;
    extern const int LIMIT_EXCEEDED;
    extern const int CANNOT_RESTORE_TABLE;
}

namespace
{

constexpr std::string_view version_column_name = "_version";

std::string formattedAST(const ASTPtr & ast)
{
    if (!ast)
        return "";
    return serializeAST(*ast);
}

void verifyTableId(const StorageID & table_id)
{
    if (!table_id.hasUUID())
    {
        auto database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "KeeperMap cannot be used with '{}' database because it uses {} engine. Please use Atomic or Replicated database",
            table_id.getDatabaseName(),
            database->getEngineName());
    }
}

}

class StorageKeeperMapSink : public SinkToStorage
{
    StorageKeeperMap & storage;
    std::unordered_map<std::string, std::string> new_values;
    std::unordered_map<std::string, int32_t> versions;
    size_t primary_key_pos;
    ContextPtr context;

public:
    StorageKeeperMapSink(StorageKeeperMap & storage_, Block header, ContextPtr context_)
        : SinkToStorage(header), storage(storage_), context(std::move(context_))
    {
        auto primary_key = storage.getPrimaryKey();
        assert(primary_key.size() == 1);
        primary_key_pos = getHeader().getPositionByName(primary_key[0]);
    }

    std::string getName() const override { return "StorageKeeperMapSink"; }

    void consume(Chunk chunk) override
    {
        auto rows = chunk.getNumRows();
        auto block = getHeader().cloneWithColumns(chunk.detachColumns());

        WriteBufferFromOwnString wb_key;
        WriteBufferFromOwnString wb_value;

        for (size_t i = 0; i < rows; ++i)
        {
            wb_key.restart();
            wb_value.restart();

            size_t idx = 0;

            int32_t version = -1;
            for (const auto & elem : block)
            {
                if (elem.name == version_column_name)
                {
                    version = assert_cast<const ColumnVector<Int32> &>(*elem.column).getData()[i];
                    continue;
                }

                elem.type->getDefaultSerialization()->serializeBinary(*elem.column, i, idx == primary_key_pos ? wb_key : wb_value, {});
                ++idx;
            }

            auto key = base64Encode(wb_key.str(), /* url_encoding */ true);

            if (version != -1)
                versions[key] = version;

            new_values[std::move(key)] = std::move(wb_value.str());
        }
    }

    void onFinish() override
    {
        finalize<false>(/*strict*/ context->getSettingsRef().keeper_map_strict_mode);
    }

    template <bool for_update>
    void finalize(bool strict)
    {
        auto zookeeper = storage.getClient();

        auto keys_limit = storage.keysLimit();

        size_t current_keys_num = 0;
        size_t new_keys_num = 0;

        // We use keys limit as a soft limit so we ignore some cases when it can be still exceeded
        // (e.g if parallel insert queries are being run)
        if (keys_limit != 0)
        {
            Coordination::Stat data_stat;
            zookeeper->get(storage.dataPath(), &data_stat);
            current_keys_num = data_stat.numChildren;
        }

        std::vector<std::string> key_paths;
        key_paths.reserve(new_values.size());
        for (const auto & [key, _] : new_values)
            key_paths.push_back(storage.fullPathForKey(key));

        zkutil::ZooKeeper::MultiExistsResponse results;

        if constexpr (!for_update)
        {
            if (!strict)
                results = zookeeper->exists(key_paths);
        }

        Coordination::Requests requests;
        requests.reserve(key_paths.size());
        for (size_t i = 0; i < key_paths.size(); ++i)
        {
            auto key = fs::path(key_paths[i]).filename();

            if constexpr (for_update)
            {
                int32_t version = -1;
                if (strict)
                    version = versions.at(key);

                requests.push_back(zkutil::makeSetRequest(key_paths[i], new_values[key], version));
            }
            else
            {
                if (!strict && results[i].error == Coordination::Error::ZOK)
                {
                    requests.push_back(zkutil::makeSetRequest(key_paths[i], new_values[key], -1));
                }
                else
                {
                    requests.push_back(zkutil::makeCreateRequest(key_paths[i], new_values[key], zkutil::CreateMode::Persistent));
                    ++new_keys_num;
                }
            }
        }

        if (new_keys_num != 0)
        {
            auto will_be = current_keys_num + new_keys_num;
            if (keys_limit != 0 && will_be > keys_limit)
                throw Exception(
                    ErrorCodes::LIMIT_EXCEEDED,
                    "Limit would be exceeded by inserting {} new key(s). Limit is {}, while the number of keys would be {}",
                    new_keys_num,
                    keys_limit,
                    will_be);
        }

        zookeeper->multi(requests, /* check_session_valid */ true);
    }
};

template <typename KeyContainer>
class StorageKeeperMapSource : public ISource
{
    const StorageKeeperMap & storage;
    size_t max_block_size;

    using KeyContainerPtr = std::shared_ptr<KeyContainer>;
    KeyContainerPtr container;
    using KeyContainerIter = typename KeyContainer::const_iterator;
    KeyContainerIter it;
    KeyContainerIter end;

    bool with_version_column = false;

    static Block getHeader(Block header, bool with_version_column)
    {
        if (with_version_column)
            header.insert(
                    {DataTypeInt32{}.createColumn(),
                    std::make_shared<DataTypeInt32>(), std::string{version_column_name}});

        return header;
    }

public:
    StorageKeeperMapSource(
        const StorageKeeperMap & storage_,
        const Block & header,
        size_t max_block_size_,
        KeyContainerPtr container_,
        KeyContainerIter begin_,
        KeyContainerIter end_,
        bool with_version_column_)
        : ISource(getHeader(header, with_version_column_)), storage(storage_), max_block_size(max_block_size_), container(std::move(container_)), it(begin_), end(end_)
        , with_version_column(with_version_column_)
    {
    }

    std::string getName() const override { return "StorageKeeperMapSource"; }

    Chunk generate() override
    {
        if (it >= end)
        {
            it = {};
            return {};
        }

        using KeyType = typename KeyContainer::value_type;
        if constexpr (std::same_as<KeyType, Field>)
        {
            const auto & sample_block = getPort().getHeader();
            const auto & key_column_type = sample_block.getByName(storage.getPrimaryKey().at(0)).type;
            auto raw_keys = serializeKeysToRawString(it, end, key_column_type, max_block_size);

            for (auto & raw_key : raw_keys)
                raw_key = base64Encode(raw_key, /* url_encoding */ true);

            return storage.getBySerializedKeys(raw_keys, nullptr, with_version_column);
        }
        else
        {
            size_t elem_num = std::min(max_block_size, static_cast<size_t>(end - it));
            auto chunk = storage.getBySerializedKeys(std::span{it, it + elem_num}, nullptr, with_version_column);
            it += elem_num;
            return chunk;
        }
    }
};

StorageKeeperMap::StorageKeeperMap(
    ContextPtr context_,
    const StorageID & table_id,
    const StorageInMemoryMetadata & metadata,
    bool attach,
    std::string_view primary_key_,
    const std::string & zk_root_path_,
    UInt64 keys_limit_)
    : IStorage(table_id)
    , WithContext(context_->getGlobalContext())
    , zk_root_path(zkutil::extractZooKeeperPath(zk_root_path_, false))
    , primary_key(primary_key_)
    , zookeeper_name(zkutil::extractZooKeeperName(zk_root_path_))
    , keys_limit(keys_limit_)
    , log(getLogger(fmt::format("StorageKeeperMap ({})", table_id.getNameForLogs())))
{
    std::string path_prefix = context_->getConfigRef().getString("keeper_map_path_prefix", "");
    if (path_prefix.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "KeeperMap is disabled because 'keeper_map_path_prefix' config is not defined");

    verifyTableId(table_id);

    setInMemoryMetadata(metadata);

    VirtualColumnsDescription virtuals;
    virtuals.addEphemeral(String(version_column_name), std::make_shared<DataTypeInt32>(), "");
    setVirtuals(std::move(virtuals));

    WriteBufferFromOwnString out;
    out << "KeeperMap metadata format version: 1\n"
        << "columns: " << metadata.columns.toString()
        << "primary key: " << formattedAST(metadata.getPrimaryKey().expression_list_ast) << "\n";
    metadata_string = out.str();

    if (zk_root_path.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "zk_root_path should not be empty");
    if (!zk_root_path.starts_with('/'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "zk_root_path should start with '/'");

    auto config_keys_limit = context_->getConfigRef().getUInt64("keeper_map_keys_limit", 0);
    if (config_keys_limit != 0 && (keys_limit == 0 || keys_limit > config_keys_limit))
    {
        LOG_WARNING(
            log,
            "Keys limit defined by argument ({}) is larger than the one defined by 'keeper_map_keys_limit' config ({}). Will use "
            "config defined value",
            keys_limit,
            config_keys_limit);
        keys_limit = config_keys_limit;
    }
    else if (keys_limit > 0)
    {
        LOG_INFO(log, "Keys limit will be set to {}", keys_limit);
    }

    auto zk_root_path_fs = fs::path(path_prefix) / std::string_view{zk_root_path}.substr(1);
    zk_root_path = zk_root_path_fs.generic_string();

    zk_data_path = zk_root_path_fs / "data";

    auto metadata_path_fs = zk_root_path_fs / "metadata";
    zk_metadata_path = metadata_path_fs;
    zk_tables_path = metadata_path_fs / "tables";

    table_unique_id = toString(table_id.uuid) + toString(ServerUUID::get());
    zk_table_path = fs::path(zk_tables_path) / table_unique_id;

    zk_dropped_path = metadata_path_fs / "dropped";
    zk_dropped_lock_path = fs::path(zk_dropped_path) / "lock";

    if (attach)
    {
        checkTable<false>();
        return;
    }

    auto client = getClient();

    if (zk_root_path != "/" && !client->exists(zk_root_path))
    {
        LOG_TRACE(log, "Creating root path {}", zk_root_path);
        client->createAncestors(zk_root_path);
        client->createIfNotExists(zk_root_path, "");
    }

    for (size_t i = 0; i < 1000; ++i)
    {
        std::string stored_metadata_string;
        auto exists = client->tryGet(zk_metadata_path, stored_metadata_string);

        if (exists)
        {
            // this requires same name for columns
            // maybe we can do a smarter comparison for columns and primary key expression
            if (stored_metadata_string != metadata_string)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Path {} is already used but the stored table definition doesn't match. Stored metadata: {}",
                    zk_root_path,
                    stored_metadata_string);

            auto code = client->tryCreate(zk_table_path, "", zkutil::CreateMode::Persistent);

            // tables_path was removed with drop
            if (code == Coordination::Error::ZNONODE)
            {
                LOG_INFO(log, "Metadata nodes were removed by another server, will retry");
                continue;
            }
            else if (code != Coordination::Error::ZOK)
            {
                throw zkutil::KeeperException(code, "Failed to create table on path {} because a table with same UUID already exists", zk_root_path);
            }

            return;
        }

        if (client->exists(zk_dropped_path))
        {
            LOG_INFO(log, "Removing leftover nodes");
            auto code = client->tryCreate(zk_dropped_lock_path, "", zkutil::CreateMode::Ephemeral);

            if (code == Coordination::Error::ZNONODE)
            {
                LOG_INFO(log, "Someone else removed leftover nodes");
            }
            else if (code == Coordination::Error::ZNODEEXISTS)
            {
                LOG_INFO(log, "Someone else is removing leftover nodes");
                continue;
            }
            else if (code != Coordination::Error::ZOK)
            {
                throw Coordination::Exception::fromPath(code, zk_dropped_lock_path);
            }
            else
            {
                auto metadata_drop_lock = zkutil::EphemeralNodeHolder::existing(zk_dropped_lock_path, *client);
                if (!dropTable(client, metadata_drop_lock))
                    continue;
            }
        }

        Coordination::Requests create_requests
        {
            zkutil::makeCreateRequest(zk_metadata_path, metadata_string, zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest(zk_data_path, metadata_string, zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest(zk_tables_path, "", zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest(zk_table_path, "", zkutil::CreateMode::Persistent),
        };

        Coordination::Responses create_responses;
        auto code = client->tryMulti(create_requests, create_responses);
        if (code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_INFO(log, "It looks like a table on path {} was created by another server at the same moment, will retry", zk_root_path);
            continue;
        }
        else if (code != Coordination::Error::ZOK)
        {
            zkutil::KeeperMultiException::check(code, create_requests, create_responses);
        }


        table_is_valid = true;
        return;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Cannot create metadata for table, because it is removed concurrently or because "
                    "of wrong zk_root_path ({})", zk_root_path);
}


Pipe StorageKeeperMap::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    checkTable<true>();
    storage_snapshot->check(column_names);

    FieldVectorPtr filtered_keys;
    bool all_scan;

    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    auto primary_key_type = sample_block.getByName(primary_key).type;
    std::tie(filtered_keys, all_scan) = getFilterKeys(primary_key, primary_key_type, query_info, context_);

    bool with_version_column = false;
    for (const auto & column : column_names)
    {
        if (column == version_column_name)
        {
            with_version_column = true;
            break;
        }
    }

    const auto process_keys = [&]<typename KeyContainerPtr>(KeyContainerPtr keys) -> Pipe
    {
        if (keys->empty())
            return {};

        ::sort(keys->begin(), keys->end());
        keys->erase(std::unique(keys->begin(), keys->end()), keys->end());

        Pipes pipes;

        size_t num_keys = keys->size();
        size_t num_threads = std::min<size_t>(num_streams, keys->size());

        assert(num_keys <= std::numeric_limits<uint32_t>::max());
        assert(num_threads <= std::numeric_limits<uint32_t>::max());

        for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx)
        {
            size_t begin = num_keys * thread_idx / num_threads;
            size_t end = num_keys * (thread_idx + 1) / num_threads;

            using KeyContainer = typename KeyContainerPtr::element_type;
            pipes.emplace_back(std::make_shared<StorageKeeperMapSource<KeyContainer>>(
                *this, sample_block, max_block_size, keys, keys->begin() + begin, keys->begin() + end, with_version_column));
        }
        return Pipe::unitePipes(std::move(pipes));
    };

    auto client = getClient();
    if (all_scan)
        return process_keys(std::make_shared<std::vector<std::string>>(client->getChildren(zk_data_path)));

    return process_keys(std::move(filtered_keys));
}

SinkToStoragePtr StorageKeeperMap::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    checkTable<true>();
    return std::make_shared<StorageKeeperMapSink>(*this, metadata_snapshot->getSampleBlock(), local_context);
}

void StorageKeeperMap::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    checkTable<true>();
    auto client = getClient();
    client->tryRemoveChildrenRecursive(zk_data_path, true);
}

bool StorageKeeperMap::dropTable(zkutil::ZooKeeperPtr zookeeper, const zkutil::EphemeralNodeHolder::Ptr & metadata_drop_lock)
{
    zookeeper->removeChildrenRecursive(zk_data_path);

    bool completely_removed = false;
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(metadata_drop_lock->getPath(), -1));
    ops.emplace_back(zkutil::makeRemoveRequest(zk_dropped_path, -1));
    ops.emplace_back(zkutil::makeRemoveRequest(zk_data_path, -1));
    ops.emplace_back(zkutil::makeRemoveRequest(zk_metadata_path, -1));

    Coordination::Responses responses;
    auto code = zookeeper->tryMulti(ops, responses);
    using enum Coordination::Error;
    switch (code)
    {
        case ZOK:
        {
            metadata_drop_lock->setAlreadyRemoved();
            completely_removed = true;
            LOG_INFO(log, "Metadata ({}) and data ({}) was successfully removed from ZooKeeper", zk_metadata_path, zk_data_path);
            break;
        }
        case ZNONODE:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "There is a race condition between creation and removal of metadata. It's a bug");
        case ZNOTEMPTY:
            LOG_ERROR(log, "Metadata was not completely removed from ZooKeeper");
            break;
        default:
            zkutil::KeeperMultiException::check(code, ops, responses);
            break;
    }
    return completely_removed;
}

void StorageKeeperMap::drop()
{
    checkTable<true>();
    auto client = getClient();

    // we allow ZNONODE in case we got hardware error on previous drop
    if (auto code = client->tryRemove(zk_table_path); code == Coordination::Error::ZNOTEMPTY)
    {
        throw zkutil::KeeperException(
            code, "{} contains children which shouldn't happen. Please DETACH the table if you want to delete it", zk_table_path);
    }

    std::vector<std::string> children;
    // if the tables_path is not found, some other table removed it
    // if there are children, some other tables are still using this path as storage
    if (auto code = client->tryGetChildren(zk_tables_path, children);
        code != Coordination::Error::ZOK || !children.empty())
        return;

    Coordination::Requests ops;
    Coordination::Responses responses;

    ops.emplace_back(zkutil::makeRemoveRequest(zk_tables_path, -1));
    ops.emplace_back(zkutil::makeCreateRequest(zk_dropped_path, "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(zk_dropped_lock_path, "", zkutil::CreateMode::Ephemeral));

    auto code = client->tryMulti(ops, responses);

    if (code == Coordination::Error::ZNONODE || code == Coordination::Error::ZNODEEXISTS)
    {
        LOG_INFO(log, "Metadata is being removed by another table");
        return;
    }
    else if (code == Coordination::Error::ZNOTEMPTY)
    {
        LOG_WARNING(log, "Another table is using the same path, metadata will not be deleted");
        return;
    }
    else if (code != Coordination::Error::ZOK)
        zkutil::KeeperMultiException::check(code, ops, responses);

    auto metadata_drop_lock = zkutil::EphemeralNodeHolder::existing(zk_dropped_lock_path, *client);
    dropTable(client, metadata_drop_lock);
}

namespace
{

/// contains serialized key value pairs for all the data nodes used by this KeeperMap table
constexpr std::string_view backup_data_filename = "data.bin";

class KeeperMapBackup : public IBackupEntriesLazyBatch, boost::noncopyable
{
public:
    KeeperMapBackup(
        const std::string & data_zookeeper_path_,
        const std::string & data_path_in_backup,
        const DiskPtr & temp_disk_,
        UInt64 max_compress_block_size_,
        std::shared_ptr<WithRetries> with_retries_)
        : data_zookeeper_path(data_zookeeper_path_)
        , temp_disk(temp_disk_)
        , max_compress_block_size(max_compress_block_size_)
        , with_retries(std::move(with_retries_))
    {
        file_path = fs::path(data_path_in_backup) / backup_data_filename;
    }

private:
    size_t getSize() const override
    {
        return 1;
    }

    const String & getName(size_t i) const override
    {
        chassert(i == 0);
        return file_path;
    }

    BackupEntries generate() override
    {
        temp_dir_owner.emplace(temp_disk);
        fs::path temp_dir = temp_dir_owner->getRelativePath();
        temp_disk->createDirectories(temp_dir);

        auto data_file_path = temp_dir / fs::path{file_path}.filename();
        auto data_out_compressed = temp_disk->writeFile(data_file_path);
        auto data_out = std::make_unique<CompressedWriteBuffer>(*data_out_compressed, CompressionCodecFactory::instance().getDefaultCodec(), max_compress_block_size);
        std::vector<std::string> data_children;
        {
            auto holder = with_retries->createRetriesControlHolder("getKeeperMapDataKeys");
            holder.retries_ctl.retryLoop(
            [&, &zk = holder.faulty_zookeeper]()
            {
                with_retries->renewZooKeeper(zk);
                data_children = zk->getChildren(data_zookeeper_path);
            });
        }

        const auto write_rows = [&](std::span<std::string> keys)
        {
            std::vector<std::string> keys_full_path;
            keys_full_path.reserve(keys.size());

            for (const auto & key : keys)
                keys_full_path.push_back(data_zookeeper_path / key);

            zkutil::ZooKeeper::MultiTryGetResponse data;
            auto holder = with_retries->createRetriesControlHolder("getKeeperMapDataKeys");
            holder.retries_ctl.retryLoop(
            [&, &zk = holder.faulty_zookeeper]
            {
                with_retries->renewZooKeeper(zk);
                data = zk->tryGet(keys_full_path);
                data.waitForResponses();
            });

            for (size_t i = 0; i < keys.size(); ++i)
            {
                auto & child_data = data[i];
                if (child_data.error != Coordination::Error::ZOK)
                    continue;

                writeStringBinary(keys[i], *data_out);
                writeStringBinary(child_data.data, *data_out);
            }
        };

        auto max_multiread_size = with_retries->getKeeperSettings().batch_size_for_keeper_multiread;

        auto keys_it = data_children.begin();
        while (keys_it != data_children.end())
        {
            auto step = std::min(static_cast<UInt64>(std::distance(keys_it, data_children.end())), max_multiread_size);
            write_rows(std::span{keys_it, keys_it + step});
            keys_it = keys_it + step;
        }

        data_out->finalize();
        data_out.reset();
        data_out_compressed->finalize();
        data_out_compressed.reset();

        return {{file_path, std::make_shared<BackupEntryFromAppendOnlyFile>(temp_disk, data_file_path)}};
    }

    fs::path data_zookeeper_path;
    DiskPtr temp_disk;
    std::optional<TemporaryFileOnDisk> temp_dir_owner;
    UInt64 max_compress_block_size;
    String file_path;
    std::shared_ptr<WithRetries> with_retries;
};
}

void StorageKeeperMap::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /*partitions*/)
{
    auto coordination = backup_entries_collector.getBackupCoordination();
    coordination->addKeeperMapTable(zk_root_path, table_unique_id, data_path_in_backup);

    /// This task will be executed after all tables have registered their root zk path and the coordination is ready to
    /// assign each path to a single table only.
    auto post_collecting_task = [coordination, &backup_entries_collector, my_data_path_in_backup = data_path_in_backup, this]
    {
        auto path_with_data = coordination->getKeeperMapDataPath(zk_root_path);
        if (path_with_data != my_data_path_in_backup)
        {
            std::string source_path = fs::path(my_data_path_in_backup) / backup_data_filename;
            std::string target_path = fs::path(path_with_data) / backup_data_filename;
            backup_entries_collector.addBackupEntries({{source_path, std::make_shared<BackupEntryReference>(std::move(target_path))}});
            return;
        }

        auto temp_disk = backup_entries_collector.getContext()->getGlobalTemporaryVolume()->getDisk(0);
        auto max_compress_block_size = backup_entries_collector.getContext()->getSettingsRef().max_compress_block_size;

        auto with_retries = std::make_shared<WithRetries>
        (
            getLogger(fmt::format("StorageKeeperMapBackup ({})", getStorageID().getNameForLogs())),
            [&] { return getClient(); },
            WithRetries::KeeperSettings::fromContext(backup_entries_collector.getContext()),
            backup_entries_collector.getContext()->getProcessListElement(),
            [](WithRetries::FaultyKeeper &) {}
        );

        backup_entries_collector.addBackupEntries(
            std::make_shared<KeeperMapBackup>(
                this->zk_data_path, path_with_data, temp_disk, max_compress_block_size, std::move(with_retries))
                ->getBackupEntries());
    };

    backup_entries_collector.addPostTask(post_collecting_task);
}

void StorageKeeperMap::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /*partitions*/)
{
    auto backup = restorer.getBackup();
    if (!backup->hasFiles(data_path_in_backup))
        return;

    if (!restorer.getRestoreCoordination()->acquireInsertingDataForKeeperMap(zk_root_path, table_unique_id))
    {
        /// Other table is already restoring the data for this Keeper path.
        /// Tables defined on the same path share data
        return;
    }

    auto with_retries = std::make_shared<WithRetries>
    (
        getLogger(fmt::format("StorageKeeperMapRestore ({})", getStorageID().getNameForLogs())),
        [&] { return getClient(); },
        WithRetries::KeeperSettings::fromContext(restorer.getContext()),
        restorer.getContext()->getProcessListElement(),
        [](WithRetries::FaultyKeeper &) {}
    );

    bool allow_non_empty_tables = restorer.isNonEmptyTableAllowed();
    if (!allow_non_empty_tables)
    {
        Coordination::Stat data_stats;

        auto holder = with_retries->createRetriesControlHolder("checkKeeperMapData");
        holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries->renewZooKeeper(zk);
            zk->get(zk_data_path, &data_stats);
        });

        if (data_stats.numChildren != 0)
            RestorerFromBackup::throwTableIsNotEmpty(getStorageID());
    }

    auto temp_disk = restorer.getContext()->getGlobalTemporaryVolume()->getDisk(0);

    /// only 1 table should restore data for a single path
    restorer.addDataRestoreTask(
        [storage = std::static_pointer_cast<StorageKeeperMap>(shared_from_this()),
         backup,
         data_path_in_backup,
         with_retries,
         allow_non_empty_tables,
         temp_disk] { storage->restoreDataImpl(backup, data_path_in_backup, with_retries, allow_non_empty_tables, temp_disk); });
}

void StorageKeeperMap::restoreDataImpl(
    const BackupPtr & backup,
    const String & data_path_in_backup,
    std::shared_ptr<WithRetries> with_retries,
    bool allow_non_empty_tables,
    const DiskPtr & temporary_disk)
{
    auto table_id = toString(getStorageID().uuid);

    fs::path data_path_in_backup_fs = data_path_in_backup;

    String data_file = data_path_in_backup_fs /  backup_data_filename;

    if (!backup->fileExists(data_file))
        throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "File {} in backup is required to restore table", data_file);

    /// should we store locally in temp file?
    auto in = backup->readFile(data_file);
    std::optional<TemporaryFileOnDisk> temp_data_file;
    if (!dynamic_cast<ReadBufferFromFileBase *>(in.get()))
    {
        temp_data_file.emplace(temporary_disk);
        auto out = std::make_unique<WriteBufferFromFile>(temp_data_file->getAbsolutePath());
        copyData(*in, *out);
        out.reset();
        in = createReadBufferFromFileBase(temp_data_file->getAbsolutePath(), {});
    }
    std::unique_ptr<ReadBufferFromFileBase> in_from_file{static_cast<ReadBufferFromFileBase *>(in.release())};
    CompressedReadBufferFromFile compressed_in{std::move(in_from_file)};
    fs::path data_path_fs(zk_data_path);

    auto max_multi_size = with_retries->getKeeperSettings().batch_size_for_keeper_multi;

    Coordination::Requests create_requests;
    const auto flush_create_requests = [&]
    {
        auto holder = with_retries->createRetriesControlHolder("addKeeperMapData");
        holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries->renewZooKeeper(zk);
            Coordination::Responses create_responses;
            if (auto res = zk->tryMulti(create_requests, create_responses);
                res != Coordination::Error::ZOK && res != Coordination::Error::ZNODEEXISTS)
                throw zkutil::KeeperMultiException(res, create_requests, create_responses);
        });
    };

    while (!compressed_in.eof())
    {
        std::string key;
        std::string value;
        readStringBinary(key, compressed_in);
        readStringBinary(value, compressed_in);

        /// if a table can be non empty we can have conflicting keys so we need to do single create for each row
        if (allow_non_empty_tables)
        {
            auto holder = with_retries->createRetriesControlHolder("addKeeperMapData");
            holder.retries_ctl.retryLoop(
            [&, &zk = holder.faulty_zookeeper]()
            {
                with_retries->renewZooKeeper(zk);
                if (auto res = zk->tryCreate(data_path_fs / key, value, zkutil::CreateMode::Persistent);
                    res != Coordination::Error::ZOK && res != Coordination::Error::ZNODEEXISTS)
                    throw zkutil::KeeperException::fromPath(res, data_path_fs / key);
            });
        }
        /// otherwise we can do multi requests
        else
        {
            create_requests.push_back(zkutil::makeCreateRequest(data_path_fs / key, value, zkutil::CreateMode::Persistent));

            if (create_requests.size() == max_multi_size)
            {
                flush_create_requests();
                create_requests.clear();
            }
        }
    }

    if (!create_requests.empty())
        flush_create_requests();
}

zkutil::ZooKeeperPtr StorageKeeperMap::getClient() const
{
    std::lock_guard lock{zookeeper_mutex};
    if (!zookeeper_client || zookeeper_client->expired())
    {
        zookeeper_client = nullptr;
        if (zookeeper_name == "default")
            zookeeper_client = getContext()->getZooKeeper();
        else
            zookeeper_client = getContext()->getAuxiliaryZooKeeper(zookeeper_name);

        zookeeper_client->sync(zk_root_path);
    }

    return zookeeper_client;
}

const std::string & StorageKeeperMap::dataPath() const
{
    return zk_data_path;
}

std::string StorageKeeperMap::fullPathForKey(const std::string_view key) const
{
    return fs::path(zk_data_path) / key;
}

UInt64 StorageKeeperMap::keysLimit() const
{
    return keys_limit;
}

std::optional<bool> StorageKeeperMap::isTableValid() const
{
    std::lock_guard lock{init_mutex};
    if (table_is_valid.has_value())
        return *table_is_valid;

    [&]
    {
        try
        {
            auto client = getClient();

            Coordination::Stat metadata_stat;
            auto stored_metadata_string = client->get(zk_metadata_path, &metadata_stat);

            if (metadata_stat.numChildren == 0)
            {
                table_is_valid = false;
                return;
            }

            if (metadata_string != stored_metadata_string)
            {
                LOG_ERROR(
                    log,
                    "Table definition does not match to the one stored in the path {}. Stored definition: {}",
                    zk_root_path,
                    stored_metadata_string);
                table_is_valid = false;
                return;
            }

            // validate all metadata and data nodes are present
            Coordination::Requests requests;
            requests.push_back(zkutil::makeCheckRequest(zk_table_path, -1));
            requests.push_back(zkutil::makeCheckRequest(zk_data_path, -1));
            requests.push_back(zkutil::makeCheckRequest(zk_dropped_path, -1));

            Coordination::Responses responses;
            client->tryMulti(requests, responses);

            table_is_valid = false;
            if (responses[0]->error != Coordination::Error::ZOK)
            {
                LOG_ERROR(log, "Table node ({}) is missing", zk_table_path);
                return;
            }

            if (responses[1]->error != Coordination::Error::ZOK)
            {
                LOG_ERROR(log, "Data node ({}) is missing", zk_data_path);
                return;
            }

            if (responses[2]->error == Coordination::Error::ZOK)
            {
                LOG_ERROR(log, "Tables with root node {} are being dropped", zk_root_path);
                return;
            }

            table_is_valid = true;
        }
        catch (const Coordination::Exception & e)
        {
            tryLogCurrentException(log);

            if (!Coordination::isHardwareError(e.code))
                table_is_valid = false;
        }
    }();

    return table_is_valid;
}

Chunk StorageKeeperMap::getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & null_map, const Names &) const
{
    if (keys.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StorageKeeperMap supports only one key, got: {}", keys.size());

    auto raw_keys = serializeKeysToRawString(keys[0]);

    if (raw_keys.size() != keys[0].column->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Assertion failed: {} != {}", raw_keys.size(), keys[0].column->size());

    return getBySerializedKeys(raw_keys, &null_map, /* version_column */ false);
}

Chunk StorageKeeperMap::getBySerializedKeys(const std::span<const std::string> keys, PaddedPODArray<UInt8> * null_map, bool with_version) const
{
    Block sample_block = getInMemoryMetadataPtr()->getSampleBlock();
    MutableColumns columns = sample_block.cloneEmptyColumns();
    MutableColumnPtr version_column = nullptr;

    if (with_version)
        version_column = ColumnVector<Int32>::create();

    size_t primary_key_pos = getPrimaryKeyPos(sample_block, getPrimaryKey());

    if (null_map)
    {
        null_map->clear();
        null_map->resize_fill(keys.size(), 1);
    }

    auto client = getClient();

    Strings full_key_paths;
    full_key_paths.reserve(keys.size());

    for (const auto & key : keys)
    {
        full_key_paths.emplace_back(fullPathForKey(key));
    }

    auto values = client->tryGet(full_key_paths);

    for (size_t i = 0; i < keys.size(); ++i)
    {
        auto response = values[i];

        Coordination::Error code = response.error;

        if (code == Coordination::Error::ZOK)
        {
            fillColumns(base64Decode(keys[i], true), response.data, primary_key_pos, sample_block, columns);

            if (version_column)
                version_column->insert(response.stat.version);
        }
        else if (code == Coordination::Error::ZNONODE)
        {
            if (null_map)
            {
                (*null_map)[i] = 0;
                for (size_t col_idx = 0; col_idx < sample_block.columns(); ++col_idx)
                    columns[col_idx]->insert(sample_block.getByPosition(col_idx).type->getDefault());

                if (version_column)
                    version_column->insert(-1);
            }
        }
        else
        {
            throw DB::Exception(ErrorCodes::KEEPER_EXCEPTION, "Failed to fetch value: {}", code);
        }
    }

    size_t num_rows = columns.at(0)->size();

    if (version_column)
        columns.push_back(std::move(version_column));

    return Chunk(std::move(columns), num_rows);
}

Block StorageKeeperMap::getSampleBlock(const Names &) const
{
    auto metadata = getInMemoryMetadataPtr();
    return metadata->getSampleBlock();
}

void StorageKeeperMap::checkTableCanBeRenamed(const StorageID & new_name) const
{
    verifyTableId(new_name);
}

void StorageKeeperMap::rename(const String & /*new_path_to_table_data*/, const StorageID & new_table_id)
{
    checkTableCanBeRenamed(new_table_id);
    renameInMemory(new_table_id);
}

void StorageKeeperMap::checkMutationIsPossible(const MutationCommands & commands, const Settings & /*settings*/) const
{
    if (commands.empty())
        return;

    if (commands.size() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mutations cannot be combined for KeeperMap");

    const auto command_type = commands.front().type;
    if (command_type != MutationCommand::Type::UPDATE && command_type != MutationCommand::Type::DELETE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only DELETE and UPDATE mutation supported for KeeperMap");
}

void StorageKeeperMap::mutate(const MutationCommands & commands, ContextPtr local_context)
{
    checkTable<true>();

    if (commands.empty())
        return;

    bool strict = local_context->getSettingsRef().keeper_map_strict_mode;

    assert(commands.size() == 1);

    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage = getStorageID();
    auto storage_ptr = DatabaseCatalog::instance().getTable(storage, local_context);

    if (commands.front().type == MutationCommand::Type::DELETE)
    {
        MutationsInterpreter::Settings settings(true);
        settings.return_all_columns = true;
        settings.return_mutated_rows = true;

        auto interpreter = std::make_unique<MutationsInterpreter>(
            storage_ptr,
            metadata_snapshot,
            commands,
            local_context,
            settings);

        auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
        PullingPipelineExecutor executor(pipeline);

        auto header = interpreter->getUpdatedHeader();
        auto primary_key_pos = header.getPositionByName(primary_key);
        auto version_position = header.getPositionByName(std::string{version_column_name});

        auto client = getClient();

        Block block;
        while (executor.pull(block))
        {
            auto & column_type_name = block.getByPosition(primary_key_pos);
            auto column = column_type_name.column;
            auto size = column->size();


            WriteBufferFromOwnString wb_key;
            Coordination::Requests delete_requests;

            for (size_t i = 0; i < size; ++i)
            {
                int32_t version = -1;
                if (strict)
                {
                    const auto & version_column = block.getByPosition(version_position).column;
                    version = assert_cast<const ColumnVector<Int32> &>(*version_column).getData()[i];
                }

                wb_key.restart();

                column_type_name.type->getDefaultSerialization()->serializeBinary(*column, i, wb_key, {});
                delete_requests.emplace_back(zkutil::makeRemoveRequest(fullPathForKey(base64Encode(wb_key.str(), true)), version));
            }

            Coordination::Responses responses;
            auto status = client->tryMulti(delete_requests, responses, /* check_session_valid */ true);

            if (status == Coordination::Error::ZOK)
                return;

            if (status != Coordination::Error::ZNONODE)
                throw zkutil::KeeperMultiException(status, delete_requests, responses);

            LOG_INFO(log, "Failed to delete all nodes at once, will try one by one");

            for (const auto & delete_request : delete_requests)
            {
                auto code = client->tryRemove(delete_request->getPath());
                if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
                    throw zkutil::KeeperException::fromPath(code, delete_request->getPath());
            }
        }

        return;
    }

    assert(commands.front().type == MutationCommand::Type::UPDATE);
    if (commands.front().column_to_update_expression.contains(primary_key))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Primary key cannot be updated (cannot update column {})", primary_key);

    MutationsInterpreter::Settings settings(true);
    settings.return_all_columns = true;
    settings.return_mutated_rows = true;

    auto interpreter = std::make_unique<MutationsInterpreter>(
        storage_ptr,
        metadata_snapshot,
        commands,
        local_context,
        settings);

    auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
    PullingPipelineExecutor executor(pipeline);

    auto sink = std::make_shared<StorageKeeperMapSink>(*this, executor.getHeader(), local_context);

    Block block;
    while (executor.pull(block))
        sink->consume(Chunk{block.getColumns(), block.rows()});

    sink->finalize<true>(strict);
}

namespace
{

StoragePtr create(const StorageFactory::Arguments & args)
{
    ASTs & engine_args = args.engine_args;
    if (engine_args.empty() || engine_args.size() > 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage KeeperMap requires 1-3 arguments:\n"
            "zk_root_path: path in the Keeper where the values will be stored (required)\n"
            "keys_limit: number of keys allowed to be stored, 0 is no limit (default: 0)");

    const auto zk_root_path_node = evaluateConstantExpressionAsLiteral(engine_args[0], args.getLocalContext());
    auto zk_root_path = checkAndGetLiteralArgument<std::string>(zk_root_path_node, "zk_root_path");

    UInt64 keys_limit = 0;
    if (engine_args.size() > 1)
        keys_limit = checkAndGetLiteralArgument<UInt64>(engine_args[1], "keys_limit");

    StorageInMemoryMetadata metadata;
    metadata.setColumns(args.columns);
    metadata.setConstraints(args.constraints);

    if (!args.storage_def->primary_key)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageKeeperMap requires one column in primary key");

    metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
    auto primary_key_names = metadata.getColumnsRequiredForPrimaryKey();
    if (primary_key_names.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageKeeperMap requires one column in primary key");

    return std::make_shared<StorageKeeperMap>(
        args.getContext(), args.table_id, metadata, args.query.attach, primary_key_names[0], zk_root_path, keys_limit);
}

}

void registerStorageKeeperMap(StorageFactory & factory)
{
    factory.registerStorage(
        "KeeperMap",
        create,
        {
            .supports_sort_order = true,
            .supports_parallel_insert = true,
        });
}

}
