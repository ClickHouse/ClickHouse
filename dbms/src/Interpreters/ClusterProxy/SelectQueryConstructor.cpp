#include <DB/Interpreters/ClusterProxy/SelectQueryConstructor.h>
#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/DataStreams/PreSendCallbackInputStream.h>
#include <DB/DataStreams/MaterializingBlockInputStream.h>

namespace DB
{

namespace ClusterProxy
{

SelectQueryConstructor::SelectQueryConstructor(const QueryProcessingStage::Enum & processed_stage_,
	const Tables & external_tables_)
	: processed_stage(processed_stage_), external_tables(external_tables_)
{
}

BlockInputStreamPtr SelectQueryConstructor::createLocal(ASTPtr query_ast, const Context & context, const Cluster::Address & address)
{
	BlockInputStreamPtr stream;

	if (pre_send_hook)
	{
		Poco::SharedPtr<IInterpreter> interpreter = new InterpreterSelectQuery(query_ast, context, processed_stage);
		auto callback = pre_send_hook.makeCallback();
		stream = new PreSendCallbackInputStream(interpreter, callback);
	}
	else
	{
		InterpreterSelectQuery interpreter(query_ast, context, processed_stage);
		stream = interpreter.execute().in;
	}

	/** Материализация нужна, так как с удалённых серверов константы приходят материализованными.
	* Если этого не делать, то в разных потоках будут получаться разные типы (Const и не-Const) столбцов,
	* а это не разрешено, так как весь код исходит из допущения, что в потоке блоков все типы одинаковые.
	*/
	return new MaterializingBlockInputStream(stream);
}

BlockInputStreamPtr SelectQueryConstructor::createRemote(IConnectionPool * pool, const std::string & query,
	const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
	auto stream = new RemoteBlockInputStream{pool, query, &settings, throttler, external_tables, processed_stage, context};
	stream->setPoolMode(PoolMode::GET_MANY);

	if (pre_send_hook)
	{
		auto callback = pre_send_hook.makeCallback();
		stream->attachPreSendCallback(callback);
	}

	return stream;
}

BlockInputStreamPtr SelectQueryConstructor::createRemote(ConnectionPoolsPtr & pools, const std::string & query,
	const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
	auto stream = new RemoteBlockInputStream{pools, query, &settings, throttler, external_tables, processed_stage, context};
	stream->setPoolMode(PoolMode::GET_MANY);

	if (pre_send_hook)
	{
		auto callback = pre_send_hook.makeCallback();
		stream->attachPreSendCallback(callback);
	}

	return stream;
}

bool SelectQueryConstructor::isInclusive() const
{
	return false;
}

}

}
