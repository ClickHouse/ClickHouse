#pragma once

#include "config.h"

#if USE_MONGODB

#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>

#include <mongocxx/client.hpp>

namespace DB
{
/* Implements storage in the MongoDB database.
 * Use ENGINE = MongoDB(host:port, database, collection, user, password [, options]);
 * Read only.
 * One stream only.
 */

class StorageMongoDB final : public IStorage
{
public:
    StorageMongoDB(
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
        const String & comment);

    std::string getName() const override { return "MongoDB"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr context,
        bool async_insert) override;

    struct Configuration
    {
        std::string host;
        UInt16 port;
        std::string username;
        std::string password;
        std::string database;
        std::string table;
        std::string options;
    };

    static Configuration getConfiguration(ASTs engine_args, ContextPtr context);

private:
    const std::string database_name;
    const std::string collection_name;
    mongocxx::uri uri;

    LoggerPtr log;

    static bsoncxx::types::bson_value::value toBSONValue(const Field * field);
    static String getFuncName(const String & func);
    static bsoncxx::document::value visitFunction(const ASTFunction * func);
    bsoncxx::document::value createMongoDBQuery(mongocxx::options::find * options, SelectQueryInfo * query);
};

}
#endif
