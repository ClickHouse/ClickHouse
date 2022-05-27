#include "StorageMongoDB.h"
#include "StorageMongoDBSocketFactory.h"

#include <Poco/MongoDB/Connection.h>
#include <Poco/MongoDB/Cursor.h>
#include <Poco/MongoDB/Database.h>
#include <Poco/Version.h>
#include <Storages/StorageFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/parseAddress.h>
#include <IO/Operators.h>
#include <Parsers/ASTLiteral.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Transforms/MongoDBSource.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int MONGODB_CANNOT_AUTHENTICATE;
    extern const int BAD_ARGUMENTS;
}

StorageMongoDB::StorageMongoDB(
    const StorageID & table_id_,
    const std::string & host_,
    uint16_t port_,
    const std::string & database_name_,
    const std::string & collection_name_,
    const std::string & username_,
    const std::string & password_,
    const std::string & options_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage(table_id_)
    , database_name(database_name_)
    , collection_name(collection_name_)
    , username(username_)
    , password(password_)
    , uri("mongodb://" + host_ + ":" + std::to_string(port_) + "/" + database_name_ + "?" + options_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}


void StorageMongoDB::connectIfNotConnected()
{
    std::lock_guard lock{connection_mutex};
    if (!connection)
    {
        StorageMongoDBSocketFactory factory;
        connection = std::make_shared<Poco::MongoDB::Connection>(uri, factory);
    }

    if (!authenticated)
    {
        Poco::URI poco_uri(uri);
        auto query_params = poco_uri.getQueryParameters();
        auto auth_source = std::find_if(query_params.begin(), query_params.end(),
                                        [&](const std::pair<std::string, std::string> & param) { return param.first == "authSource"; });
        auto auth_db = database_name;
        if (auth_source != query_params.end())
            auth_db = auth_source->second;
#if POCO_VERSION >= 0x01070800
        if (!username.empty() && !password.empty())
        {
            Poco::MongoDB::Database poco_db(auth_db);
            if (!poco_db.authenticate(*connection, username, password, Poco::MongoDB::Database::AUTH_SCRAM_SHA1))
                throw Exception("Cannot authenticate in MongoDB, incorrect user or password", ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);
        }
#else
        authenticate(*connection, database_name, username, password);
#endif
        authenticated = true;
    }
}


Pipe StorageMongoDB::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned)
{
    connectIfNotConnected();

    storage_snapshot->check(column_names);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({ column_data.type, column_data.name });
    }

    return Pipe(std::make_shared<MongoDBSource>(connection, createCursor(database_name, collection_name, sample_block), sample_block, max_block_size));
}


StorageMongoDBConfiguration StorageMongoDB::getConfiguration(ASTs engine_args, ContextPtr context)
{
    StorageMongoDBConfiguration configuration;
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
                        "host, port, username, password, database, table, options.", arg_name);
        }
    }
    else
    {
        if (engine_args.size() < 5 || engine_args.size() > 6)
            throw Exception(
                "Storage MongoDB requires from 5 to 6 parameters: MongoDB('host:port', database, collection, 'user', 'password' [, 'options']).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        /// 27017 is the default MongoDB port.
        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 27017);

        configuration.host = parsed_host_port.first;
        configuration.port = parsed_host_port.second;
        configuration.database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        configuration.table = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        configuration.username = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
        configuration.password = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();

        if (engine_args.size() >= 6)
            configuration.options = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();

    }

    context->getRemoteHostFilter().checkHostAndPort(configuration.host, toString(configuration.port));

    return configuration;
}


void registerStorageMongoDB(StorageFactory & factory)
{
    factory.registerStorage("MongoDB", [](const StorageFactory::Arguments & args)
    {
        auto configuration = StorageMongoDB::getConfiguration(args.engine_args, args.getLocalContext());

        return std::make_shared<StorageMongoDB>(
            args.table_id,
            configuration.host,
            configuration.port,
            configuration.database,
            configuration.table,
            configuration.username,
            configuration.password,
            configuration.options,
            args.columns,
            args.constraints,
            args.comment);
    },
    {
        .source_access_type = AccessType::MONGO,
    });
}

}
