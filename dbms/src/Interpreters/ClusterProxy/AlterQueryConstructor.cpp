#include <DB/Interpreters/ClusterProxy/AlterQueryConstructor.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/DataStreams/PreSendCallbackInputStream.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>

namespace DB
{

namespace ClusterProxy
{

BlockInputStreamPtr AlterQueryConstructor::createLocal(ASTPtr query_ast, const Context & context, const Cluster::Address & address)
{
	if (pre_send_hook)
	{
		Poco::SharedPtr<IInterpreter> interpreter = new InterpreterAlterQuery(query_ast, context);
		auto callback = pre_send_hook.makeCallback();
		return new PreSendCallbackInputStream(interpreter, callback);
	}
	else
	{
		InterpreterAlterQuery interpreter(query_ast, context);
		return interpreter.execute().in;
	}
}

BlockInputStreamPtr AlterQueryConstructor::createRemote(IConnectionPool * pool, const std::string & query,
	const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
	auto stream = new RemoteBlockInputStream{pool, query, &settings, throttler};
	stream->setPoolMode(PoolMode::GET_ONE);

	if (pre_send_hook)
	{
		auto callback = pre_send_hook.makeCallback();
		stream->attachPreSendCallback(callback);
	}

	return stream;
}

BlockInputStreamPtr AlterQueryConstructor::createRemote(ConnectionPoolsPtr & pools, const std::string & query,
	const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
	auto stream = new RemoteBlockInputStream{pools, query, &settings, throttler};
	stream->setPoolMode(PoolMode::GET_ONE);

	if (pre_send_hook)
	{
		auto callback = pre_send_hook.makeCallback();
		stream->attachPreSendCallback(callback);
	}

	return stream;
}

bool AlterQueryConstructor::isInclusive() const
{
	return false;
}

}

}
