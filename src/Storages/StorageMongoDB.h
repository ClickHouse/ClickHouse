#pragma once

#include "config.h"

#if USE_MONGODB

#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>

#include <mongocxx/instance.hpp>
#include <mongocxx/client.hpp>

namespace DB
{
/* Implements storage in the MongoDB database.
 * Use ENGINE = MongoDB(host:port, database, collection, user, password [, options]);
 *              MongoDB(uri, collection);
 * Read only.
 * One stream only.
 */

inline mongocxx::instance inst{};

class StorageMongoDB final : public IStorage
{
public:
    struct Configuration
    {
        std::shared_ptr<mongocxx::uri> uri;
        String collection;
    };
    static Configuration getConfiguration(ASTs engine_args, ContextPtr context);

    StorageMongoDB(
        const StorageID & table_id_,
        const Configuration & configuration_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    std::string getName() const override { return "MongoDB"; }
    bool isRemote() const override { return true; }

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

private:
    static bsoncxx::types::bson_value::value toBSONValue(const Field * field);
    static String getMongoFuncName(const String & func);
    static bsoncxx::document::value visitWhereFunction(const ASTFunction * func);
    bsoncxx::document::value buildMongoDBQuery(mongocxx::options::find * options, SelectQueryInfo * query, const Block & sample_block);

    Configuration configuration;
    LoggerPtr log;
};

}
#endif
