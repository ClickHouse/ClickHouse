#include <memory>
#include "Access/Common/AccessType.h"
#include "StorageZooKeeper.h"
#include "Storages/ExternalDataSourceConfiguration.h"
#include "Storages/IStorage.h"
#include "Storages/StorageFactory.h"
#include "ZooKeeperSource.h"
#include <Interpreters/evaluateConstantExpression.h>
#include "Common/ZooKeeper/Common.h"
#include "Common/ZooKeeper/ZooKeeper.h"
#include "Common/parseAddress.h"
#include <Common/logger_useful.h>



namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int BAD_ARGUMENTS;
}

StorageZooKeeper::StorageZooKeeper(
    const StorageID & table_id,
    const std::string & host_,
    const UInt16 & port_,
    const std::string & path_,
    const std::string & options_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage(table_id)
    , host(host_)
    , port(port_)
    , path(path_)
    , options(options_)
    , zookeeper(host_ + ':' + std::to_string(port), "", Coordination::DEFAULT_SESSION_TIMEOUT_MS, 
    Coordination::DEFAULT_OPERATION_TIMEOUT_MS, path, "zookeeper")
{
    LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", port);

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

void registerStorageZooKeeper(StorageFactory & factory)
{
    factory.registerStorage("ZooKeeper", [](const StorageFactory::Arguments & args)
    {
        LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "регистер");

        auto configuration = StorageZooKeeper::getConfiguration(args.engine_args, args.getLocalContext());
        return std::make_shared<StorageZooKeeper>(
            args.table_id,
            configuration.host,
            configuration.port,
            configuration.database,
            configuration.options,
            args.columns,
            args.constraints,
            args.comment);
    },
    {
        .source_access_type = AccessType::CASSANDRA,
    });
}

StorageZooKeeperConfiguration StorageZooKeeper::getConfiguration(ASTs engine_args, ContextPtr context)
{
    LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "гет конфигурэйшн");

    StorageZooKeeperConfiguration configuration;
    if (auto named_collection = getExternalDataSourceConfiguration(engine_args, context))
    {
        auto [common_configuration, storage_specific_args, _] = named_collection.value();

        configuration.set(common_configuration);

        for (const auto & [arg_name, arg_value] : storage_specific_args)
        {
            if (arg_name == "options")
                configuration.options = arg_value->as<ASTLiteral>()->value.safeGet<String>();
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Unexpected key-value argument."
                        "Got: {}, but expected one of:"
                        "host, port, path, options ", arg_name);
        }
    }
    else
    {
        if (engine_args.size() < 2 || engine_args.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Storage ZooKeeper requires from 3 to 4 parameters: "
                            "ZooKeeper('host:port', 'path', [, 'options']. Got: {}",
                            engine_args.size());

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        
        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 2181);

        configuration.host = parsed_host_port.first;
        configuration.port = parsed_host_port.second;
        configuration.database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        if (engine_args.size() == 3)
            configuration.options = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
    }
    context->getRemoteHostFilter().checkHostAndPort(configuration.host, toString(configuration.port));

    return configuration;
}

Pipe StorageZooKeeper::read(
    const Names & column_names_,
    const StorageSnapshotPtr & storage_snapshot_,
    SelectQueryInfo & query_info_,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage_*/,
    size_t /*max_block_size_*/,
    unsigned /*num_streams_*/)
{
    LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "рид");
    if (zookeeper.impl) {
        LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "stor impl");

    }
    // тут где-то должна быть установка соединения 
    connection = getSession();
    LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "рид 1");

    storage_snapshot_->check(column_names_);

    Block sample_block;
    for (const String & column_name : column_names_)
    {
        auto column_data = storage_snapshot_->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({ column_data.type, column_data.name });
    }
    LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "рид 2");

    return Pipe(std::make_shared<ZooKeeperSource>(connection, context_, query_info_, sample_block, path));
}

zkutil::ZooKeeperPtr StorageZooKeeper::getSession()
{
    std::lock_guard lock(zookeeper_mutex);
    LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "сейшн");

    // if (zookeeper.expired()) {
    LOG_FATAL(&Poco::Logger::root(), "AOOAOAOOAAOO  {}", "сейшн 1");
    return zookeeper.startNewSession();
    // }
    // return connection;
}
}
