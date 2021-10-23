#include "StorageMongoDB.h"

#include <Poco/MongoDB/Connection.h>
#include <Poco/MongoDB/Cursor.h>
#include <Poco/MongoDB/Database.h>
#include <Poco/Version.h>
#include <Storages/StorageFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Common/parseAddress.h>
#include <IO/Operators.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>
#include <DataStreams/MongoDBBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int MONGODB_CANNOT_AUTHENTICATE;
}

StorageMongoDB::StorageMongoDB(
    const StorageID & table_id_,
    const std::string & host_,
    uint16_t port_,
    const std::string & database_name_,
    const std::string & collection_name_,
    const std::string & username_,
    const std::string & password_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage(table_id_)
    , host(host_)
    , port(port_)
    , database_name(database_name_)
    , collection_name(collection_name_)
    , username(username_)
    , password(password_)
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
        connection = std::make_shared<Poco::MongoDB::Connection>(host, port);

    if (!authenticated)
    {
#       if POCO_VERSION >= 0x01070800
            Poco::MongoDB::Database poco_db(database_name);
            if (!poco_db.authenticate(*connection, username, password, Poco::MongoDB::Database::AUTH_SCRAM_SHA1))
                throw Exception("Cannot authenticate in MongoDB, incorrect user or password", ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);
#       else
            authenticate(*connection, database_name, username, password);
#       endif
        authenticated = true;
    }
}


Pipe StorageMongoDB::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned)
{
    connectIfNotConnected();

    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = metadata_snapshot->getColumns().getPhysical(column_name);
        sample_block.insert({ column_data.type, column_data.name });
    }

    return Pipe(std::make_shared<SourceFromInputStream>(
            std::make_shared<MongoDBBlockInputStream>(connection, createCursor(database_name, collection_name, sample_block), sample_block, max_block_size, true)));
}

void registerStorageMongoDB(StorageFactory & factory)
{
    factory.registerStorage("MongoDB", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 5)
            throw Exception(
                "Storage MongoDB requires 5 parameters: MongoDB('host:port', database, collection, 'user', 'password').",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

        /// 27017 is the default MongoDB port.
        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 27017);

        const String & remote_database = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        const String & collection = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        const String & username = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
        const String & password = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();

        return StorageMongoDB::create(
            args.table_id,
            parsed_host_port.first,
            parsed_host_port.second,
            remote_database,
            collection,
            username,
            password,
            args.columns,
            args.constraints,
            args.comment);
    },
    {
        .source_access_type = AccessType::MONGO,
    });
}

}
