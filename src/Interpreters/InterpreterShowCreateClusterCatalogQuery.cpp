#include <Interpreters/InterpreterShowCreateClusterCatalogQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Common/Clusters/ClusterFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTShowCreateClusterCatalogQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

BlockIO InterpreterShowCreateClusterCatalogQuery::execute()
{
    auto & query = query_ptr->as<ASTShowCreateClusterCatalogQuery &>();
    const bool is_cluster = query.kind == ASTShowCreateClusterCatalogQuery::Kind::Cluster;
    getContext()->checkAccess(is_cluster ? AccessType::SHOW_CREATE_CLUSTER : AccessType::SHOW_CREATE_SHARD);

    String stmt = is_cluster
        ? ClusterFactory::instance().getShowCreateCluster(query.name)
        : ClusterFactory::instance().getShowCreateShard(query.name);

    MutableColumnPtr column = ColumnString::create();
    column->insert(std::move(stmt));

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(Block{{
        std::move(column),
        std::make_shared<DataTypeString>(),
        "statement"}})));
    return res;
}

void registerInterpreterShowCreateClusterCatalogQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterShowCreateClusterCatalogQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterShowCreateClusterCatalogQuery>(args.query, args.context); });
}

}
