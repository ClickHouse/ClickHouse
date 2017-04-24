#include <Interpreters/ClusterProxy/DescribeQueryConstructor.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/BlockExtraInfoInputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>

namespace DB
{

namespace
{

constexpr PoolMode pool_mode = PoolMode::GET_ALL;

BlockExtraInfo toBlockExtraInfo(const Cluster::Address & address)
{
    BlockExtraInfo block_extra_info;
    block_extra_info.host = address.host_name;
    block_extra_info.resolved_address = address.resolved_address.toString();
    block_extra_info.port = address.port;
    block_extra_info.user = address.user;
    block_extra_info.is_valid = true;
    return block_extra_info;
}

}

namespace ClusterProxy
{

BlockInputStreamPtr DescribeQueryConstructor::createLocal(ASTPtr query_ast, const Context & context, const Cluster::Address & address)
{
    InterpreterDescribeQuery interpreter{query_ast, context};
    BlockInputStreamPtr stream = interpreter.execute().in;

    /** Materialization is needed, since from remote servers the constants come materialized.
      * If you do not do this, different types (Const and non-Const) columns will be produced in different threads,
      * And this is not allowed, since all code is based on the assumption that in the block stream all types are the same.
      */
    BlockInputStreamPtr materialized_stream = std::make_shared<MaterializingBlockInputStream>(stream);

    return std::make_shared<BlockExtraInfoInputStream>(materialized_stream, toBlockExtraInfo(address));
}

BlockInputStreamPtr DescribeQueryConstructor::createRemote(
        const ConnectionPoolWithFailoverPtr & pool, const std::string & query,
        const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
    auto stream = std::make_shared<RemoteBlockInputStream>(pool, query, &settings, throttler);
    stream->setPoolMode(pool_mode);
    stream->appendExtraInfo();
    return stream;
}

BlockInputStreamPtr DescribeQueryConstructor::createRemote(
        ConnectionPoolWithFailoverPtrs && pools, const std::string & query,
        const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
    auto stream =  std::make_shared<RemoteBlockInputStream>(std::move(pools), query, &settings, throttler);
    stream->setPoolMode(pool_mode);
    stream->appendExtraInfo();
    return stream;
}

PoolMode DescribeQueryConstructor::getPoolMode() const
{
    return pool_mode;
}


}

}
