#pragma once

#include "config.h"

#if USE_MONGODB
#include <Common/RemoteHostFilter.h>

#include <Analyzer/JoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>

#include <mongocxx/instance.hpp>
#include <mongocxx/client.hpp>

namespace DB
{

/// As per MongoDB CXX driver documentation:
/// You must create a mongocxx::instance object before you use the C++ driver,
/// and this object must remain alive for as long as any other MongoDB objects are in scope.
///
/// mongocxx::instance must not be created more than once, therefore we use a singleton.
class MongoDBInstanceHolder final : public boost::noncopyable
{
public:
    MongoDBInstanceHolder(MongoDBInstanceHolder const &) = delete;
    void operator=(MongoDBInstanceHolder const &) = delete;

    static MongoDBInstanceHolder & instance()
    {
        static MongoDBInstanceHolder instance;
        return instance;
    }
private:
    MongoDBInstanceHolder() = default;
    mongocxx::instance inst;
};

struct MongoDBConfiguration
{
    std::unique_ptr<mongocxx::uri> uri;
    String collection;
    std::unordered_set<String> oid_fields = {"_id"};

    void checkHosts(const ContextPtr & context) const;

    bool isOidColumn(const std::string & name) const
    {
        return oid_fields.contains(name);
    }
};

/** Implements storage in the MongoDB database.
 *  Use ENGINE = MongoDB(host:port, database, collection, user, password[, options[, oid_columns]]);
 *               MongoDB(uri, collection[, oid columns]);
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
    bool isExternalDatabase() const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    MongoDBInstanceHolder & instance_holder = MongoDBInstanceHolder::instance();

    std::optional<bsoncxx::document::value> visitWhereConstant(
        const ContextPtr & context,
        const ConstantNode * const_node,
        const JoinNode * join_node);

    std::optional<bsoncxx::document::value> visitWhereFunction(
        const ContextPtr & context,
        const FunctionNode * func,
        const JoinNode * join_node);

    std::optional<bsoncxx::document::value> visitWhereFunctionArguments(
        const ColumnNode * column_node,
        const ConstantNode * const_node,
        const FunctionNode * func,
        bool invert_comparison);

    std::optional<bsoncxx::document::value> visitWhereNode(
        const ContextPtr & context,
        const QueryTreeNodePtr & where_node,
        const JoinNode * join_node);

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
