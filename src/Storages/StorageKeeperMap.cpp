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
#include <Storages/IKVStorage.h>
#include <Storages/KVStorageUtils.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

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

struct ZooKeeperLock
{
    explicit ZooKeeperLock(std::string lock_path_, zkutil::ZooKeeperPtr client_)
        : lock_path(std::move(lock_path_)), client(std::move(client_))
    {
        lock();
    }

    ~ZooKeeperLock()
    {
        if (locked)
            unlock();
    }

    void lock()
    {
        assert(!locked);
        sequence_path = client->create(std::filesystem::path(lock_path) / "lock-", "", zkutil::CreateMode::EphemeralSequential);
        auto node_name = getBaseName(sequence_path);

        while (true)
        {
            auto children = client->getChildren(lock_path);
            assert(!children.empty());
            ::sort(children.begin(), children.end());

            auto node_it = std::find(children.begin(), children.end(), node_name);
            if (node_it == children.begin())
            {
                locked = true;
                return;
            }

            client->waitForDisappear(*(node_it - 1));
        }
    }

    void unlock()
    {
        assert(locked);
        client->remove(sequence_path);
    }

private:
    std::string lock_path;
    std::string sequence_path;
    zkutil::ZooKeeperPtr client;
    bool locked{false};
};

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

        auto keys_limit = storage.keysLimit();

        Coordination::Requests requests;

        if (!keys_limit)
        {
            for (const auto & [key, value] : new_values)
            {
                auto path = storage.fullPathForKey(key);

                if (zookeeper->exists(path))
                    requests.push_back(zkutil::makeSetRequest(path, value, -1));
                else
                    requests.push_back(zkutil::makeCreateRequest(path, value, zkutil::CreateMode::Persistent));
            }
        }
        else
        {
            ZooKeeperLock lock(storage.lockPath(), zookeeper);

            auto children = zookeeper->getChildren(storage.rootKeeperPath());
            std::unordered_set<std::string_view> children_set(children.begin(), children.end());

            size_t created_nodes = 0;
            for (const auto & [key, value] : new_values)
            {
                auto path = storage.fullPathForKey(key);

                if (children_set.contains(key))
                {
                    requests.push_back(zkutil::makeSetRequest(path, value, -1));
                }
                else
                {
                    requests.push_back(zkutil::makeCreateRequest(path, value, zkutil::CreateMode::Persistent));
                    ++created_nodes;
                }
            }

            size_t keys_num_after_insert = children.size() - 1 + created_nodes;
            if (keys_limit && keys_num_after_insert > keys_limit)
            {
                throw Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS,
                    "Cannot insert values. {} key would be created setting the total keys number to {} exceeding the limit of {}",
                    created_nodes,
                    keys_num_after_insert,
                    keys_limit);
            }
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

namespace
{

zkutil::ZooKeeperPtr getZooKeeperClient(const std::string & hosts, const ContextPtr & context)
{
    if (hosts == default_host)
        return context->getZooKeeper()->startNewSession();

    return std::make_shared<zkutil::ZooKeeper>(hosts);
}

}

StorageKeeperMap::StorageKeeperMap(
    ContextPtr context,
    const StorageID & table_id,
    const StorageInMemoryMetadata & metadata,
    std::string_view primary_key_,
    std::string_view keeper_path_,
    const std::string & hosts,
    bool create_missing_root_path,
    size_t keys_limit_)
    : IKeyValueStorage(table_id), keeper_path(keeper_path_), primary_key(primary_key_), zookeeper_client(getZooKeeperClient(hosts, context))
{
    setInMemoryMetadata(metadata);

    if (keeper_path.empty())
        throw Exception("keeper_path should not be empty", ErrorCodes::BAD_ARGUMENTS);
    if (!keeper_path.starts_with('/'))
        throw Exception("keeper_path should start with '/'", ErrorCodes::BAD_ARGUMENTS);

    auto client = getClient();

    if (keeper_path != "/" && !client->exists(keeper_path))
    {
        if (!create_missing_root_path)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Path '{}' doesn't exist. Please create it or set 'create_missing_root_path' to true'",
                keeper_path_);
        }
        else
        {
            LOG_TRACE(&Poco::Logger::get("StorageKeeperMap"), "Creating root path {}", keeper_path);

            size_t cur_pos = 0;
            do
            {
                size_t search_start = cur_pos + 1;
                cur_pos = keeper_path.find('/', search_start);
                if (search_start == cur_pos)
                    throw Exception("keeper_path is invalid, contains subsequent '/'", ErrorCodes::BAD_ARGUMENTS);

                auto path = keeper_path.substr(0, cur_pos);
                client->createIfNotExists(path, "");
            } while (cur_pos != std::string_view::npos);
        }
    }

    // create metadata nodes
    std::filesystem::path root_path{keeper_path};

    auto metadata_path_fs = root_path / "__ch_metadata";
    metadata_path = metadata_path_fs;
    client->createIfNotExists(metadata_path, "");

    lock_path = metadata_path_fs / "lock";
    client->createIfNotExists(lock_path, "");

    auto keys_limit_path = metadata_path_fs / "keys_limit";
    auto status = client->tryCreate(keys_limit_path, toString(keys_limit_), zkutil::CreateMode::Persistent);
    if (status == Coordination::Error::ZNODEEXISTS)
    {
        auto data = client->get(keys_limit_path, nullptr, nullptr);
        UInt64 stored_keys_limit = parse<UInt64>(data);
        if (stored_keys_limit != keys_limit_)
        {
            keys_limit = stored_keys_limit;
            LOG_WARNING(
                &Poco::Logger::get("StorageKeeperMap"),
                "Keys limit is already set for {} to {}. Going to use already set value",
                keeper_path,
                stored_keys_limit);
        }
    }
    else if (status == Coordination::Error::ZOK)
    {
        keys_limit = keys_limit_;
    }
    else
    {
        throw zkutil::KeeperException(status, keys_limit_path);
    }
}


Pipe StorageKeeperMap::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    storage_snapshot->check(column_names);

    FieldVectorPtr filtered_keys;
    bool all_scan;

    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    auto primary_key_type = sample_block.getByName(primary_key).type;
    std::tie(filtered_keys, all_scan) = getFilterKeys(primary_key, primary_key_type, query_info, context);

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
        return process_keys(std::make_shared<std::vector<std::string>>(client->getChildren(keeper_path)));

    return process_keys(std::move(filtered_keys));
}

SinkToStoragePtr StorageKeeperMap::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/)
{
    return std::make_shared<StorageKeeperMapSink>(*this, metadata_snapshot);
}

zkutil::ZooKeeperPtr & StorageKeeperMap::getClient() const
{
    if (zookeeper_client->expired())
    {
        zookeeper_client = zookeeper_client->startNewSession();
        zookeeper_client->sync("/");
    }

    return zookeeper_client;
}

const std::string & StorageKeeperMap::rootKeeperPath() const
{
    return keeper_path;
}

std::string StorageKeeperMap::fullPathForKey(const std::string_view key) const
{
    return fmt::format("{}/{}", keeper_path, key);
}

const std::string & StorageKeeperMap::lockPath() const
{
    return lock_path;
}

UInt64 StorageKeeperMap::keysLimit() const
{
    return keys_limit;
}

Chunk StorageKeeperMap::getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & null_map) const
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

namespace
{
StoragePtr create(const StorageFactory::Arguments & args)
{
    ASTs & engine_args = args.engine_args;
    if (engine_args.empty() || engine_args.size() > 4)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage KeeperMap requires 1-4 arguments:\n"
            "keeper_path: path in the Keeper where the values will be stored (required)\n"
            "keys_limit: number of keys allowed, set to 0 for no limit (default: 0)\n"
            "hosts: comma separated Keeper hosts, set to '{0}' to use the same Keeper as ClickHouse (default: '{0}')\n"
            "create_missing_root_path: true if the root path should be created if it's missing (default: 1)",
            default_host);

    auto keeper_path = checkAndGetLiteralArgument<std::string>(engine_args[0], "keeper_path");

    std::string hosts = "default";
    if (engine_args.size() > 1)
        hosts = checkAndGetLiteralArgument<std::string>(engine_args[1], "hosts");

    size_t keys_limit = 0;
    if (engine_args.size() > 2)
        keys_limit = checkAndGetLiteralArgument<UInt64>(engine_args[2], "keys_limit");

    bool create_missing_root_path = true;
    if (engine_args.size() > 3)
        create_missing_root_path = checkAndGetLiteralArgument<UInt64>(engine_args[3], "create_missing_root_path");

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
        args.getContext(), args.table_id, metadata, primary_key_names[0], keeper_path, hosts, create_missing_root_path, keys_limit);
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
