#include <Interpreters/InterpreterShowCreateClusterQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Common/Clusters/ClusterFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTShowCreateClusterQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

BlockIO InterpreterShowCreateClusterQuery::execute()
{
    auto & query = query_ptr->as<ASTShowCreateClusterQuery &>();
    getContext()->checkAccess(AccessType::SHOW_CREATE_CLUSTER);

    String stmt = ClusterFactory::instance().getShowCreateCluster(query.cluster_name);

    MutableColumnPtr column = ColumnString::create();
    column->insert(std::move(stmt));

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(Block{{
        std::move(column),
        std::make_shared<DataTypeString>(),
        "statement"}})));
    return res;
}

void registerInterpreterShowCreateClusterQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterShowCreateClusterQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterShowCreateClusterQuery>(args.query, args.context); });
}

}
