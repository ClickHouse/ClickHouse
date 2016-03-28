#include <DB/Interpreters/ClusterProxy/SelectQueryConstructor.h>
#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/DataStreams/MaterializingBlockInputStream.h>

namespace DB
{

namespace
{

constexpr PoolMode pool_mode = PoolMode::GET_MANY;

}

namespace ClusterProxy
{

SelectQueryConstructor::SelectQueryConstructor(const QueryProcessingStage::Enum & processed_stage_,
	const Tables & external_tables_)
	: processed_stage{processed_stage_}, external_tables{external_tables_}
{
}

BlockInputStreamPtr SelectQueryConstructor::createLocal(ASTPtr query_ast, const Context & context, const Cluster::Address & address)
{
	InterpreterSelectQuery interpreter{query_ast, context, processed_stage};
	BlockInputStreamPtr stream = interpreter.execute().in;

	/** Материализация нужна, так как с удалённых серверов константы приходят материализованными.
	* Если этого не делать, то в разных потоках будут получаться разные типы (Const и не-Const) столбцов,
	* а это не разрешено, так как весь код исходит из допущения, что в потоке блоков все типы одинаковые.
	*/
	return new MaterializingBlockInputStream{stream};
}

BlockInputStreamPtr SelectQueryConstructor::createRemote(IConnectionPool * pool, const std::string & query,
	const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
	auto stream = new RemoteBlockInputStream{pool, query, &settings, throttler, external_tables, processed_stage, context};
	stream->setPoolMode(pool_mode);
	return stream;
}

BlockInputStreamPtr SelectQueryConstructor::createRemote(ConnectionPoolsPtr & pools, const std::string & query,
	const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
	auto stream = new RemoteBlockInputStream{pools, query, &settings, throttler, external_tables, processed_stage, context};
	stream->setPoolMode(pool_mode);
	return stream;
}

PoolMode SelectQueryConstructor::getPoolMode() const
{
	return pool_mode;
}

}

}
