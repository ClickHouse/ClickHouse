#include <DB/Parsers/formatAST.h>

#include <DB/DataStreams/RemoteBlockInputStream.h>

#include <DB/Storages/StorageDistributed.h>

#include <Poco/Net/NetworkInterface.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <boost/bind.hpp>

namespace DB
{

StorageDistributed::StorageDistributed(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & remote_database_,
	const String & remote_table_,
	Cluster & cluster_,
	const DataTypeFactory & data_type_factory_,
	const Settings & settings,
	const Context & context_,
	const String & sign_column_name_)
	: name(name_), columns(columns_),
	remote_database(remote_database_), remote_table(remote_table_),
	data_type_factory(data_type_factory_),
	sign_column_name(sign_column_name_),
	context(context_),
	cluster(cluster_)
{
}

StoragePtr StorageDistributed::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & remote_database_,
	const String & remote_table_,
	Cluster & cluster_,
	const DataTypeFactory & data_type_factory_,
	const Settings & settings,
	const Context & context_,
	const String & sign_column_name_)
{
	return (new StorageDistributed(name_, columns_, remote_database_, remote_table_, cluster_, data_type_factory_, settings, context_, sign_column_name_))->thisPtr();
}

BlockInputStreams StorageDistributed::read(
	const Names & column_names,
	ASTPtr query,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	processed_stage = (cluster.pools.size() + cluster.getLocalNodesNum()) == 1
		? QueryProcessingStage::Complete
		: QueryProcessingStage::WithMergeableState;

	/// Заменим в запросе имена БД и таблицы.
	ASTPtr modified_query_ast = query->clone();
	ASTSelectQuery & select = dynamic_cast<ASTSelectQuery &>(*modified_query_ast);
	select.database = new ASTIdentifier(StringRange(), remote_database, ASTIdentifier::Database);
	select.table 	= new ASTIdentifier(StringRange(), remote_table, 	ASTIdentifier::Table);

	/// Установим sign_rewrite = 0, чтобы второй раз не переписывать запрос
	Settings new_settings = settings;
	new_settings.sign_rewrite = false;
	new_settings.queue_max_wait_ms = Cluster::saturation(new_settings.queue_max_wait_ms, settings.limits.max_execution_time);

	std::stringstream s;
	formatAST(select, s, 0, false, true);
	String modified_query = s.str();

	BlockInputStreams res;

	for (ConnectionPools::iterator it = cluster.pools.begin(); it != cluster.pools.end(); ++it)
		res.push_back(new RemoteBlockInputStream((*it)->get(), modified_query, &new_settings, processed_stage));


	/// добавляем запросы к локальному clickhouse
	DB::Context new_context = context;
	new_context.setSettings(new_settings);
	{
		DB::Context new_context = context;
		new_context.setSettings(new_settings);
		for(size_t i = 0; i < cluster.getLocalNodesNum(); ++i)
		{
			InterpreterSelectQuery interpreter(modified_query_ast, new_context, processed_stage);
			res.push_back(interpreter.execute());
		}
	}

	return res;
}

void StorageDistributed::alter(const ASTAlterQuery::Parameters &params)
{
	alterColumns(params, columns, context);
}
}
