#include <Storages/StorageKeeperMap.h>
#include <Storages/StorageFactory.h>
#include "Storages/checkAndGetLiteralArgument.h"
#include <DataTypes/DataTypeString.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include "Common/ZooKeeper/KeeperException.h"
#include "Common/ZooKeeper/Types.h"

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
    ColumnsDescription columns;
    columns.add({"key", std::make_shared<DataTypeString>()});
    columns.add({"value", std::make_shared<DataTypeString>()});
    storage_metadata.setColumns(std::move(columns));
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
            cur_pos = keeper_path.find('/', cur_pos + 1);
            auto path = keeper_path.substr(0, cur_pos);
            LOG_TRACE(&Poco::Logger::get("StorageKeeperMap"), "Creating root path {}", path);
            auto status = getClient()->tryCreate(path, "", zkutil::CreateMode::Persistent);
            if (status != Coordination::Error::ZOK && status != Coordination::Error::ZNODEEXISTS)
                throw zkutil::KeeperException(status, path);
        } while (cur_pos != std::string_view::npos);
    }
}

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
    return nullptr;
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
