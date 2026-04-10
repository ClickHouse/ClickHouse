#include <Interpreters/InterpreterShowCreateShardQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Common/Clusters/ClusterFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTShowCreateShardQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

BlockIO InterpreterShowCreateShardQuery::execute()
{
    auto & query = query_ptr->as<ASTShowCreateShardQuery &>();
    getContext()->checkAccess(AccessType::SHOW_CREATE_SHARD);

    String stmt = ClusterFactory::instance().getShowCreateShard(query.shard_name);

    MutableColumnPtr column = ColumnString::create();
    column->insert(std::move(stmt));

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(Block{{
        std::move(column),
        std::make_shared<DataTypeString>(),
        "statement"}})));
    return res;
}

void registerInterpreterShowCreateShardQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterShowCreateShardQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterShowCreateShardQuery>(args.query, args.context); });
}

}
