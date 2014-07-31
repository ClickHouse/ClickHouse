#include <DB/Parsers/formatAST.h>

#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/DataStreams/RemoveColumnsBlockInputStream.h>

#include <DB/Storages/StorageDistributed.h>
#include <DB/Storages/VirtualColumnFactory.h>

#include <Poco/Net/NetworkInterface.h>
#include <DB/Client/ConnectionPool.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <boost/bind.hpp>
#include <DB/Core/Field.h>

namespace DB
{

StorageDistributed::StorageDistributed(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & remote_database_,
	const String & remote_table_,
	Cluster & cluster_,
	const Context & context_)
	: name(name_), columns(columns_),
	remote_database(remote_database_), remote_table(remote_table_),
	context(context_),
	cluster(cluster_)
{
}

StoragePtr StorageDistributed::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & remote_database_,
	const String & remote_table_,
	const String & cluster_name,
	Context & context_)
{
	context_.initClusters();
	return (new StorageDistributed(name_, columns_, remote_database_, remote_table_, context_.getCluster(cluster_name), context_))->thisPtr();
}


StoragePtr StorageDistributed::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & remote_database_,
	const String & remote_table_,
	SharedPtr<Cluster> & owned_cluster_,
	Context & context_)
{
	auto res = new StorageDistributed(name_, columns_, remote_database_, remote_table_, *owned_cluster_, context_);

	/// Захватываем владение объектом-кластером.
	res->owned_cluster = owned_cluster_;

	return res->thisPtr();
}

ASTPtr StorageDistributed::rewriteQuery(ASTPtr query)
{
	/// Создаем копию запроса.
	ASTPtr modified_query_ast = query->clone();

	/// Меняем имена таблицы и базы данных
	ASTSelectQuery & select = typeid_cast<ASTSelectQuery &>(*modified_query_ast);
	select.database = new ASTIdentifier(StringRange(), remote_database, ASTIdentifier::Database);
	select.table 	= new ASTIdentifier(StringRange(), remote_table, 	ASTIdentifier::Table);

	return modified_query_ast;
}

static String selectToString(ASTPtr query)
{
	ASTSelectQuery & select = typeid_cast<ASTSelectQuery &>(*query);
	std::stringstream s;
	formatAST(select, s, 0, false, true);
	return s.str();
}

BlockInputStreams StorageDistributed::read(
	const Names & column_names,
	ASTPtr query,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	Settings new_settings = settings;
	new_settings.queue_max_wait_ms = Cluster::saturate(new_settings.queue_max_wait_ms, settings.limits.max_execution_time);

	size_t result_size = cluster.pools.size() + cluster.getLocalNodesNum();

	processed_stage = result_size == 1
		? QueryProcessingStage::Complete
		: QueryProcessingStage::WithMergeableState;

	BlockInputStreams res;
	ASTPtr modified_query_ast = rewriteQuery(query);

	/// Цикл по шардам.
	for (auto & conn_pool : cluster.pools)
	{
		String modified_query = selectToString(modified_query_ast);

		res.push_back(new RemoteBlockInputStream(
			conn_pool,
			modified_query,
			&new_settings,
			external_tables,
			processed_stage));
	}

	/// Добавляем запросы к локальному ClickHouse.
	if (cluster.getLocalNodesNum() > 0)
	{
		DB::Context new_context = context;
		new_context.setSettings(new_settings);
		for (auto & it : external_tables)
			if (!new_context.tryGetExternalTable(it.first))
				new_context.addExternalTable(it.first, it.second);

		for(size_t i = 0; i < cluster.getLocalNodesNum(); ++i)
		{
			InterpreterSelectQuery interpreter(modified_query_ast, new_context, processed_stage);
				res.push_back(interpreter.execute());
		}
	}

	external_tables.clear();
	return res;
}

void StorageDistributed::alter(const AlterCommands & params, const String & database_name, const String & table_name, Context & context)
{
	auto lock = lockStructureForAlter();
	params.apply(*columns);
	InterpreterAlterQuery::updateMetadata(database_name, table_name, *columns, context);
}

NameAndTypePair StorageDistributed::getColumn(const String & column_name) const
{
	auto type = VirtualColumnFactory::tryGetType(column_name);
	if (type)
		return NameAndTypePair(column_name, type);

	return getRealColumn(column_name);
}

bool StorageDistributed::hasColumn(const String & column_name) const
{
	return VirtualColumnFactory::hasColumn(column_name) || hasRealColumn(column_name);
}

}
