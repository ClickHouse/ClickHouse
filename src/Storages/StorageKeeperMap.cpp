#include <Storages/StorageKeeperMap.h>

#include <Columns/ColumnString.h>

#include <Core/NamesAndTypes.h>

#include <DataTypes/DataTypeString.h>

#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>

#include <Processors/ISource.h>
#include <Processors/Sinks/SinkToStorage.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/KVStorageUtils.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include "Common/Exception.h"
#include "Common/ZooKeeper/IKeeper.h"
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include "Core/UUID.h"

#include <boost/algorithm/string/classification.hpp>
#include <Poco/Base64Decoder.h>
#include <Poco/Base64Encoder.h>
#include <Poco/MemoryStream.h>
#include <Poco/StreamCopier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int KEEPER_EXCEPTION;
    extern const int LOGICAL_ERROR;
}

namespace
{

std::string base64Encode(const std::string & decoded)
{
    std::ostringstream ostr; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ostr.exceptions(std::ios::failbit);
    Poco::Base64Encoder encoder(ostr, Poco::BASE64_URL_ENCODING);
    encoder.rdbuf()->setLineLength(0);
    encoder << decoded;
    encoder.close();
    return ostr.str();
}

std::string base64Decode(const std::string & encoded)
{
    std::string decoded;
    Poco::MemoryInputStream istr(encoded.data(), encoded.size());
    Poco::Base64Decoder decoder(istr, Poco::BASE64_URL_ENCODING);
    Poco::StreamCopier::copyToString(decoder, decoded);
    return decoded;
}

constexpr std::string_view default_host = "default";

std::string_view getBaseName(const std::string_view path)
{
    auto last_slash = path.find_last_of('/');
    if (last_slash == std::string_view::npos)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Failed to get basename of path '{}'", path);

    return path.substr(last_slash + 1);
}

}

class StorageKeeperMapSink : public SinkToStorage
{
    StorageKeeperMap & storage;
    std::unordered_map<std::string, std::string> new_values;
    size_t primary_key_pos;

public:
    StorageKeeperMapSink(StorageKeeperMap & storage_, const StorageMetadataPtr & metadata_snapshot)
        : SinkToStorage(metadata_snapshot->getSampleBlock()), storage(storage_)
    {
        auto primary_key = storage.getPrimaryKey();
        assert(primary_key.size() == 1);
        primary_key_pos = getHeader().getPositionByName(storage.getPrimaryKey()[0]);
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
            for (const auto & elem : block)
            {
                elem.type->getDefaultSerialization()->serializeBinary(*elem.column, i, idx == primary_key_pos ? wb_key : wb_value);
                ++idx;
            }

            auto key = base64Encode(wb_key.str());
            new_values[std::move(key)] = std::move(wb_value.str());
        }
    }

    void onFinish() override
    {
        auto & zookeeper = storage.getClient();

        Coordination::Requests requests;

        for (const auto & [key, value] : new_values)
        {
            auto path = storage.fullPathForKey(key);

            if (zookeeper->exists(path))
                requests.push_back(zkutil::makeSetRequest(path, value, -1));
            else
                requests.push_back(zkutil::makeCreateRequest(path, value, zkutil::CreateMode::Persistent));
        }

        zookeeper->multi(requests);
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

public:
    StorageKeeperMapSource(
        const StorageKeeperMap & storage_,
        const Block & header,
        size_t max_block_size_,
        KeyContainerPtr container_,
        KeyContainerIter begin_,
        KeyContainerIter end_)
        : ISource(header), storage(storage_), max_block_size(max_block_size_), container(std::move(container_)), it(begin_), end(end_)
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
                raw_key = base64Encode(raw_key);

            return storage.getBySerializedKeys(raw_keys, nullptr);
        }
        else
        {
            size_t elem_num = std::min(max_block_size, static_cast<size_t>(end - it));
            auto chunk = storage.getBySerializedKeys(std::span{it, it + elem_num}, nullptr);
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
    const std::string & root_path_,
    bool create_missing_root_path)
    : IStorage(table_id)
    , WithContext(context_->getGlobalContext())
    , root_path(zkutil::extractZooKeeperPath(root_path_, false))
    , primary_key(primary_key_)
    , zookeeper_name(zkutil::extractZooKeeperName(root_path_))
    , log(&Poco::Logger::get("StorageKeeperMap"))
{
    if (table_id.uuid == UUIDHelpers::Nil)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "KeeperMap cannot be used with '{}' database because UUID is needed. Please use Atomic or Replicated database", table_id.getDatabaseName());

    setInMemoryMetadata(metadata);

    if (root_path.empty())
        throw Exception("root_path should not be empty", ErrorCodes::BAD_ARGUMENTS);
    if (!root_path.starts_with('/'))
        throw Exception("root_path should start with '/'", ErrorCodes::BAD_ARGUMENTS);

    std::filesystem::path root_path_fs{root_path};
    auto metadata_path_fs = root_path_fs / "ch_metadata";
    metadata_path = metadata_path_fs;
    tables_path = metadata_path_fs / "tables";
    table_path = fs::path(tables_path) / toString(table_id.uuid);
    dropped_path = metadata_path_fs / "dropped";
    dropped_lock_path = fs::path(dropped_path) / "lock";

    if (attach)
    {
        checkTable<false>();
        return;
    }

    auto client = getClient();

    if (root_path != "/" && !client->exists(root_path))
    {
        if (!create_missing_root_path)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Path '{}' doesn't exist. Please create it or set 'create_missing_root_path' to true'",
                root_path_);
        }
        else
        {
            LOG_TRACE(log, "Creating root path {}", root_path);
            client->createAncestors(root_path);
            client->createIfNotExists(root_path, "");
        }
    }

    for (size_t i = 0; i < 1000; ++i)
    {
        if (client->exists(dropped_path))
        {
            LOG_INFO(log, "Removing leftover nodes");
            auto code = client->tryCreate(dropped_lock_path, "", zkutil::CreateMode::Ephemeral);

            if (code == Coordination::Error::ZNONODE ||  code == Coordination::Error::ZNODEEXISTS)
            {
                LOG_INFO(log, "Someone else removed leftovers");
            }
            else if (code != Coordination::Error::ZOK)
            {
                throw Coordination::Exception(code, dropped_lock_path);
            }
            else
            {
                auto metadata_drop_lock = zkutil::EphemeralNodeHolder::existing(dropped_lock_path, *client);
                if (!removeMetadataNodes(client, metadata_drop_lock))
                    continue;
            }
        }

        client->createIfNotExists(metadata_path, "");
        client->createIfNotExists(tables_path, "");

        auto code = client->tryCreate(table_path, "", zkutil::CreateMode::Persistent);

        if (code == Coordination::Error::ZOK)
            return;

        if (code == Coordination::Error::ZNONODE)
            LOG_INFO(log, "Metadata nodes were deleted in background, will retry");
        else 
            throw Coordination::Exception(code, table_path);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create metadata for table, becuase it is removed concurrently or because of wrong root_path ({})", root_path);
}


Pipe StorageKeeperMap::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    checkTable<true>();
    storage_snapshot->check(column_names);

    FieldVectorPtr filtered_keys;
    bool all_scan;

    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    auto primary_key_type = sample_block.getByName(primary_key).type;
    std::tie(filtered_keys, all_scan) = getFilterKeys(primary_key, primary_key_type, query_info, context_);

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
                *this, sample_block, max_block_size, keys, keys->begin() + begin, keys->begin() + end));
        }
        return Pipe::unitePipes(std::move(pipes));
    };

    auto & client = getClient();
    if (all_scan)
        return process_keys(std::make_shared<std::vector<std::string>>(client->getChildren(root_path)));

    return process_keys(std::move(filtered_keys));
}

SinkToStoragePtr StorageKeeperMap::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/)
{
    checkTable<true>();
    return std::make_shared<StorageKeeperMapSink>(*this, metadata_snapshot);
}

void StorageKeeperMap::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    checkTable<true>();
    auto client = getClient();
    client->tryRemoveChildrenRecursive(root_path, true, getBaseName(metadata_path));
}

bool StorageKeeperMap::removeMetadataNodes(zkutil::ZooKeeperPtr zookeeper, const zkutil::EphemeralNodeHolder::Ptr & metadata_drop_lock)
{
    bool completely_removed = false;
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(metadata_drop_lock->getPath(), -1));
    ops.emplace_back(zkutil::makeRemoveRequest(dropped_path, -1));
    ops.emplace_back(zkutil::makeRemoveRequest(metadata_path, -1));

    Coordination::Responses responses;
    auto code = zookeeper->tryMulti(ops, responses);
    using enum Coordination::Error;
    switch (code)
    {
        case ZOK:
        {
            metadata_drop_lock->setAlreadyRemoved();
            completely_removed = true;
            LOG_INFO(log, "Metadata in {} was successfully removed from ZooKeeper", metadata_path);
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

    client->remove(table_path);

    if (!client->getChildren(tables_path).empty())
        return;

    Coordination::Requests ops;
    Coordination::Responses responses;

    ops.emplace_back(zkutil::makeRemoveRequest(tables_path, -1));
    ops.emplace_back(zkutil::makeCreateRequest(dropped_path, "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(dropped_lock_path, "", zkutil::CreateMode::Ephemeral));

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

    client->removeChildrenRecursive(root_path, getBaseName(metadata_path));

    auto metadata_drop_lock = zkutil::EphemeralNodeHolder::existing(dropped_lock_path, *client);
    removeMetadataNodes(client, metadata_drop_lock);
}

zkutil::ZooKeeperPtr & StorageKeeperMap::getClient() const
{
    std::lock_guard lock{zookeeper_mutex};
    if (!zookeeper_client || zookeeper_client->expired())
    {
        zookeeper_client = nullptr;
        if (zookeeper_name == "default")
            zookeeper_client = getContext()->getZooKeeper();
        else
            zookeeper_client = getContext()->getAuxiliaryZooKeeper(zookeeper_name);
    }

    return zookeeper_client;
}

const std::string & StorageKeeperMap::rootKeeperPath() const
{
    return root_path;
}

std::string StorageKeeperMap::fullPathForKey(const std::string_view key) const
{
    return fmt::format("{}/{}", root_path, key);
}

std::optional<bool> StorageKeeperMap::isTableValid() const
{
    std::lock_guard lock{init_mutex};
    if (table_is_valid.has_value())
        return *table_is_valid;

    try
    {
        // validate all metadata nodes are present
        Coordination::Requests requests;
        requests.push_back(zkutil::makeCheckRequest(table_path, -1));

        Coordination::Responses responses;
        auto client = getClient();
        auto res = client->tryMulti(requests, responses);
        table_is_valid = res == Coordination::Error::ZOK;
    }
    catch (const Coordination::Exception & e)
    {
        tryLogCurrentException(log);

        if (!Coordination::isHardwareError(e.code))
            table_is_valid = false;
    }
    catch (const Exception &)
    {
        tryLogCurrentException(log);
        table_is_valid = false;
    }

    return table_is_valid;
}

Chunk StorageKeeperMap::getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & null_map, const Names &) const
{
    if (keys.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StorageKeeperMap supports only one key, got: {}", keys.size());

    auto raw_keys = serializeKeysToRawString(keys[0]);

    if (raw_keys.size() != keys[0].column->size())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Assertion failed: {} != {}", raw_keys.size(), keys[0].column->size());

    return getBySerializedKeys(raw_keys, &null_map);
}

Chunk StorageKeeperMap::getBySerializedKeys(const std::span<const std::string> keys, PaddedPODArray<UInt8> * null_map) const
{
    Block sample_block = getInMemoryMetadataPtr()->getSampleBlock();
    MutableColumns columns = sample_block.cloneEmptyColumns();
    size_t primary_key_pos = getPrimaryKeyPos(sample_block, getPrimaryKey());

    if (null_map)
    {
        null_map->clear();
        null_map->resize_fill(keys.size(), 1);
    }

    auto client = getClient();

    std::vector<std::future<Coordination::GetResponse>> values;
    values.reserve(keys.size());

    for (const auto & key : keys)
    {
        const auto full_path = fullPathForKey(key);
        if (full_path == metadata_path)
        {
            values.emplace_back();
            continue;
        }

        values.emplace_back(client->asyncTryGet(full_path));
    }

    auto wait_until = std::chrono::system_clock::now() + std::chrono::milliseconds(Coordination::DEFAULT_OPERATION_TIMEOUT_MS);

    for (size_t i = 0; i < keys.size(); ++i)
    {
        auto & value = values[i];
        if (!value.valid())
            continue;

        if (value.wait_until(wait_until) != std::future_status::ready)
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Failed to fetch values: timeout");

        auto response = value.get();
        Coordination::Error code = response.error;

        if (code == Coordination::Error::ZOK)
        {
            fillColumns(base64Decode(keys[i]), response.data, primary_key_pos, sample_block, columns);
        }
        else if (code == Coordination::Error::ZNONODE)
        {
            if (null_map)
            {
                (*null_map)[i] = 0;
                for (size_t col_idx = 0; col_idx < sample_block.columns(); ++col_idx)
                    columns[col_idx]->insert(sample_block.getByPosition(col_idx).type->getDefault());
            }
        }
        else
        {
            throw DB::Exception(ErrorCodes::KEEPER_EXCEPTION, "Failed to fetch value: {}", code);
        }
    }

    size_t num_rows = columns.at(0)->size();
    return Chunk(std::move(columns), num_rows);
}

Block StorageKeeperMap::getSampleBlock(const Names &) const
{
    auto metadata = getInMemoryMetadataPtr();
    return metadata ? metadata->getSampleBlock() : Block();
}

namespace
{
    StoragePtr create(const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        if (engine_args.empty() || engine_args.size() > 4)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage KeeperMap requires 1-5 arguments:\n"
                "root_path: path in the Keeper where the values will be stored (required)\n"
                "hosts: comma separated Keeper hosts, set to '{0}' to use the same Keeper as ClickHouse (default: '{0}')\n"
                "create_missing_root_path: 1 if the root path should be created if it's missing, otherwise throw exception (default: 1)\n",
                "remove_existing_data: true if children inside 'root_path' should be deleted, otherwise throw exception (default: 0)",
                default_host);

        auto root_path = checkAndGetLiteralArgument<std::string>(engine_args[0], "root_path");

        bool create_missing_root_path = true;
        if (engine_args.size() > 1)
            create_missing_root_path = checkAndGetLiteralArgument<UInt64>(engine_args[1], "create_missing_root_path");

        StorageInMemoryMetadata metadata;
        metadata.setColumns(args.columns);
        metadata.setConstraints(args.constraints);

        if (!args.storage_def->primary_key)
            throw Exception("StorageKeeperMap requires one column in primary key", ErrorCodes::BAD_ARGUMENTS);

        metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
        auto primary_key_names = metadata.getColumnsRequiredForPrimaryKey();
        if (primary_key_names.size() != 1)
            throw Exception("StorageKeeperMap requires one column in primary key", ErrorCodes::BAD_ARGUMENTS);

        return std::make_shared<StorageKeeperMap>(
            args.getContext(), args.table_id, metadata, args.query.attach, primary_key_names[0], root_path, create_missing_root_path);
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
