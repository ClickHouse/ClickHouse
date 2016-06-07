#include <DB/Interpreters/ClusterProxy/AlterQueryConstructor.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>

namespace DB
{

namespace
{

constexpr PoolMode pool_mode = PoolMode::GET_ONE;

}

namespace ClusterProxy
{

BlockInputStreamPtr AlterQueryConstructor::createLocal(ASTPtr query_ast, const Context & context, const Cluster::Address & address)
{
	InterpreterAlterQuery interpreter{query_ast, context};
	return interpreter.execute().in;
}

BlockInputStreamPtr AlterQueryConstructor::createRemote(IConnectionPool * pool, const std::string & query,
	const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
	auto stream = new RemoteBlockInputStream{pool, query, &settings, throttler};
	stream->setPoolMode(pool_mode);
	return stream;
}

BlockInputStreamPtr AlterQueryConstructor::createRemote(ConnectionPoolsPtr & pools, const std::string & query,
	const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
	auto stream = new RemoteBlockInputStream{pools, query, &settings, throttler};
	stream->setPoolMode(pool_mode);
	return stream;
}

PoolMode AlterQueryConstructor::getPoolMode() const
{
	return pool_mode;
}

}

}
