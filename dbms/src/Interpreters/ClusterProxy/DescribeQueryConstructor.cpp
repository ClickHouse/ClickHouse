#include <DB/Interpreters/ClusterProxy/DescribeQueryConstructor.h>
#include <DB/Interpreters/InterpreterDescribeQuery.h>
#include <DB/DataStreams/MaterializingBlockInputStream.h>
#include <DB/DataStreams/BlockExtraInfoInputStream.h>
#include <DB/DataStreams/PreSendCallbackInputStream.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>

namespace DB
{

namespace
{
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
	BlockInputStreamPtr stream;

	if (pre_send_hook)
	{
		Poco::SharedPtr<IInterpreter> interpreter = new InterpreterDescribeQuery(query_ast, context);
		auto callback = pre_send_hook.makeCallback();
		stream = new PreSendCallbackInputStream(interpreter, callback);
	}
	else
	{
		InterpreterDescribeQuery interpreter(query_ast, context);
		stream = interpreter.execute().in;
	}

	/** Материализация нужна, так как с удалённых серверов константы приходят материализованными.
	* Если этого не делать, то в разных потоках будут получаться разные типы (Const и не-Const) столбцов,
	* а это не разрешено, так как весь код исходит из допущения, что в потоке блоков все типы одинаковые.
	*/
	BlockInputStreamPtr materialized_stream = new MaterializingBlockInputStream(stream);

	return new BlockExtraInfoInputStream(materialized_stream, toBlockExtraInfo(address));
}

BlockInputStreamPtr DescribeQueryConstructor::createRemote(IConnectionPool * pool, const std::string & query,
	const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
	auto stream = new RemoteBlockInputStream{pool, query, &settings, throttler};
	stream->setPoolMode(PoolMode::GET_ALL);
	stream->appendExtraInfo();

	if (pre_send_hook)
	{
		auto callback = pre_send_hook.makeCallback();
		stream->attachPreSendCallback(callback);
	}

	return stream;
}

BlockInputStreamPtr DescribeQueryConstructor::createRemote(ConnectionPoolsPtr & pools, const std::string & query,
	const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
	auto stream =  new RemoteBlockInputStream{pools, query, &settings, throttler};
	stream->setPoolMode(PoolMode::GET_ALL);
	stream->appendExtraInfo();

	if (pre_send_hook)
	{
		auto callback = pre_send_hook.makeCallback();
		stream->attachPreSendCallback(callback);
	}

	return stream;
}

bool DescribeQueryConstructor::isInclusive() const
{
	return true;
}

}

}
