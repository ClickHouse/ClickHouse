#include <Interpreters/ClusterProxy/AlterStreamFactory.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/LazyBlockInputStream.h>

namespace DB
{

namespace ClusterProxy
{

void AlterStreamFactory::createForShard(
        const Cluster::ShardInfo & shard_info,
        const String & query, const ASTPtr & query_ast,
        const Context & context, const ThrottlerPtr & throttler,
        BlockInputStreams & res)
{
    if (shard_info.isLocal())
    {
        /// The ALTER query may be a resharding query that is a part of a distributed
        /// job. Since the latter heavily relies on synchronization among its participating
        /// nodes, it is very important to defer the execution of a local query so as
        /// to prevent any deadlock.
        auto interpreter = std::make_shared<InterpreterAlterQuery>(query_ast, context);
        res.emplace_back(std::make_shared<LazyBlockInputStream>(
            [interpreter]() mutable
            {
                return interpreter->execute().in;
            }));
    }
    else
    {
        auto stream = std::make_shared<RemoteBlockInputStream>(shard_info.pool, query, context, nullptr, throttler);
        stream->setPoolMode(PoolMode::GET_ONE);
        res.emplace_back(std::move(stream));
    }
}

}
}
