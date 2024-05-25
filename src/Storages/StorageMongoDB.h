#pragma once

#include "config.h"

#if USE_MONGODB

#include <Interpreters/Context.h>

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

struct MongoDBConfiguration
{
    std::unique_ptr<mongocxx::uri> uri;
    String collection;

    void checkHosts(ContextPtr context) const
    {
        // Because domain records will be resolved inside the driver, we can't check IPs for our restrictions.
        for (const auto & host : uri->hosts())
            context->getRemoteHostFilter().checkHostAndPort(host.name, toString(host.port));
    }
};

class StorageMongoDB final : public IStorage
{
public:
    static MongoDBConfiguration getConfiguration(ASTs engine_args, ContextPtr context);

    StorageMongoDB(
        const StorageID & table_id_,
        MongoDBConfiguration configuration_,
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

private:
    static bsoncxx::document::value visitWhereFunction(ContextPtr context, const ASTFunction * func);
    bsoncxx::document::value buildMongoDBQuery(ContextPtr context, mongocxx::options::find & options, const SelectQueryInfo & query, const Block & sample_block);

    const MongoDBConfiguration configuration;
    LoggerPtr log;
};

}
#endif
