#pragma once

#include "config.h"

#if USE_MONGODB
#include <Common/RemoteHostFilter.h>

#include <Analyzer/JoinNode.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>

#include <mongocxx/instance.hpp>
#include <mongocxx/client.hpp>

namespace DB
{

inline mongocxx::instance inst{};

struct MongoDBConfiguration
{
    std::unique_ptr<mongocxx::uri> uri;
    String collection;

    void checkHosts(const ContextPtr & context) const
    {
        // Because domain records will be resolved inside the driver, we can't check IPs for our restrictions.
        for (const auto & host : uri->hosts())
            context->getRemoteHostFilter().checkHostAndPort(host.name, toString(host.port));
    }
};

/** Implements storage in the MongoDB database.
 *  Use ENGINE = MongoDB(host:port, database, collection, user, password [, options]);
 *               MongoDB(uri, collection);
 *  Read only.
 *  One stream only.
 */
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
    template <typename OnError>
    std::optional<bsoncxx::document::value> visitWhereFunction(
        const ContextPtr & context,
        const FunctionNode * func,
        const JoinNode * join_node,
        OnError on_error);

    bsoncxx::document::value buildMongoDBQuery(
        const ContextPtr & context,
        mongocxx::options::find & options,
        const SelectQueryInfo & query,
        const Block & sample_block);

    const MongoDBConfiguration configuration;
    LoggerPtr log;
};

}
#endif
