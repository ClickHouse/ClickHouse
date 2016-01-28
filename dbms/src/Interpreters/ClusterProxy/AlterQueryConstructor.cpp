#include <DB/Interpreters/ClusterProxy/AlterQueryConstructor.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/DataStreams/MaterializingBlockInputStream.h>
#include <DB/DataStreams/BlockExtraInfoInputStream.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>

namespace DB
{

namespace ClusterProxy
{

BlockInputStreamPtr AlterQueryConstructor::createLocal(ASTPtr query_ast, const Context & context, const Cluster::Address & address)
{
	InterpreterAlterQuery interpreter(query_ast, context);
	return interpreter.execute().in;
}

BlockInputStreamPtr AlterQueryConstructor::createRemote(IConnectionPool * pool, const std::string & query,
	const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
	return new RemoteBlockInputStream{pool, query, &settings, throttler};
}

BlockInputStreamPtr AlterQueryConstructor::createRemote(ConnectionPoolsPtr & pools, const std::string & query,
	const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
	return new RemoteBlockInputStream{pools, query, &settings, throttler};
}

bool AlterQueryConstructor::isInclusive() const
{
	return false;
}

}

}
