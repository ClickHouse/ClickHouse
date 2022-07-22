#include <Storages/StorageKeeperMap.h>
#include <Storages/StorageFactory.h>
#include "Storages/ColumnsDescription.h"
#include "Storages/checkAndGetLiteralArgument.h"
#include <DataTypes/DataTypeString.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StorageKeeperMap::StorageKeeperMap(
        std::string_view keeper_path_,
        ContextPtr context,
        const StorageID & table_id)
    : IStorage(table_id)
    , keeper_path(keeper_path_)
    , zookeeper_client(context->getZooKeeper()->startNewSession())
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription{getNamesAndTypes()});
    setInMemoryMetadata(storage_metadata);

    if (keeper_path.empty())
        throw Exception("keeper_path should not be empty", ErrorCodes::BAD_ARGUMENTS);
    if (!keeper_path.starts_with('/'))
        throw Exception("keeper_path should start with '/'", ErrorCodes::BAD_ARGUMENTS);

    if (keeper_path != "/")
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
            LOG_TRACE(&Poco::Logger::get("StorageKeeperMap"), "Creating root path {}", path);
            auto status = getClient()->tryCreate(path, "", zkutil::CreateMode::Persistent);
            if (status != Coordination::Error::ZOK && status != Coordination::Error::ZNODEEXISTS)
                throw zkutil::KeeperException(status, path);
        } while (cur_pos != std::string_view::npos);
    }
}

NamesAndTypesList StorageKeeperMap::getNamesAndTypes()
{
    return {
        {"key", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeString>()}
    };
}

class StorageKeeperMapSink : public SinkToStorage
{
    StorageKeeperMap & storage;
    std::unordered_map<std::string, std::string> new_values;
public:
    StorageKeeperMapSink(const Block & header, StorageKeeperMap & storage_) : SinkToStorage(header), storage(storage_)
    {}

    std::string getName() const override { return "StorageKeeperMapSink"; }

    void consume(Chunk chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());

        size_t rows = block.rows();
        for (size_t i = 0; i < rows; ++i)
        {
            std::string key = block.getByPosition(0).column->getDataAt(i).toString();

            if (key.find('/') != std::string::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key cannot contain '/'. Key: '{}'", key);

            std::string value = block.getByPosition(1).column->getDataAt(i).toString();

            new_values[std::move(key)] = std::move(value);
        }
    }

    void onFinish() override
    {
        auto & zookeeper = storage.getClient();
        Coordination::Requests requests;
        for (const auto & [key, value] : new_values)
        {
            auto path = fmt::format("{}/{}", storage.rootKeeperPath(), key);

            if (zookeeper->exists(path))
                requests.push_back(zkutil::makeSetRequest(path, value, -1));
            else
                requests.push_back(zkutil::makeCreateRequest(path, value, zkutil::CreateMode::Persistent));
        }

        zookeeper->multi(requests);
    }
};

Pipe StorageKeeperMap::read(
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned /*num_streams*/)
{
    return {};
}

SinkToStoragePtr StorageKeeperMap::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    ContextPtr /*context*/)
{
    auto columns = getNamesAndTypes();
    Block write_header;
    for (const auto & [name, type] : columns)
    {
        write_header.insert(ColumnWithTypeAndName(type, name));
    }

    return std::make_shared<StorageKeeperMapSink>(write_header, *this);
}

zkutil::ZooKeeperPtr & StorageKeeperMap::getClient()
{
    if (zookeeper_client->expired())
        zookeeper_client = zookeeper_client->startNewSession();

    return zookeeper_client;
}

const std::string & StorageKeeperMap::rootKeeperPath() const
{
    return keeper_path;
}

void registerStorageKeeperMap(StorageFactory & factory)
{
    factory.registerStorage(
        "KeeperMap",
        [](const StorageFactory::Arguments & args)
        {
            if (!args.attach && !args.columns.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage KeeperMap does not accept column definition as it has predefined columns (key String, value String)");

            ASTs & engine_args = args.engine_args;
            if (engine_args.empty() || engine_args.size() > 1)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Storage KeeperMap requires 1 argument: "
                        "keeper_path, path in the Keeper where the values will be stored");

            auto keeper_path = checkAndGetLiteralArgument<String>(engine_args[0], "keeper_path");

            return std::make_shared<StorageKeeperMap>(keeper_path, args.getContext(), args.table_id);
        },
        {});
}

}
