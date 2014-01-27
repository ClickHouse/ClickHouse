#include <DB/Parsers/formatAST.h>

#include <DB/DataStreams/RemoteBlockInputStream.h>

#include <DB/Storages/StorageDistributed.h>

#include <Poco/Net/NetworkInterface.h>
#include <DB/Client/ConnectionPool.h>

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
	std::vector<String> virtual_columns;
	virtual_columns.push_back("_host");
	virtual_columns.push_back("_port");
	String suffix = VirtualColumnUtils::chooseSuffixForSet(getColumnsList(), virtual_columns);
	_host_column_name = virtual_columns[0] + suffix;
	_port_column_name = virtual_columns[1] + suffix;
}

StoragePtr StorageDistributed::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & remote_database_,
	const String & remote_table_,
	const String & cluster_name,
	const DataTypeFactory & data_type_factory_,
	const Settings & settings,
	Context & context_,
	const String & sign_column_name_)
{
	context_.initClusters();
	return (new StorageDistributed(name_, columns_, remote_database_, remote_table_, context_.getCluster(cluster_name), data_type_factory_, settings, context_, sign_column_name_))->thisPtr();
}

NameAndTypePair StorageDistributed::getColumn(const String &column_name) const
{
	if (column_name == _host_column_name) return std::make_pair(_host_column_name, new DataTypeString);
	if (column_name == _port_column_name) return std::make_pair(_port_column_name, new DataTypeUInt16);
	return getRealColumn(column_name);
}

bool StorageDistributed::hasColumn(const String &column_name) const
{
	if (column_name == _host_column_name) return true;
	if (column_name == _port_column_name) return true;
	return hasRealColumn(column_name);
}

ASTPtr StorageDistributed::remakeQuery(ASTPtr query, const String & host, size_t port)
{
	/// Создаем копию запроса.
	ASTPtr modified_query_ast = query->clone();

	/// Добавляем в запрос значения хоста и порта
	VirtualColumnUtils::rewriteEntityInAst(modified_query_ast, _host_column_name, host);
	VirtualColumnUtils::rewriteEntityInAst(modified_query_ast, _port_column_name, port);

	/// Меняем имена таблицы и базы данных
	ASTSelectQuery & select = dynamic_cast<ASTSelectQuery &>(*modified_query_ast);
	select.database = new ASTIdentifier(StringRange(), remote_database, ASTIdentifier::Database);
	select.table 	= new ASTIdentifier(StringRange(), remote_table, 	ASTIdentifier::Table);

	return modified_query_ast;
}

static String selectToString(ASTPtr query)
{
	ASTSelectQuery & select = dynamic_cast<ASTSelectQuery &>(*query);
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
	Names real_column_names, virt_column_names(2);
	for (const auto & it : column_names)
		if (it == _host_column_name)
			virt_column_names[0] = _host_column_name;
		else if (it == _port_column_name)
			virt_column_names[1] = _port_column_name;
		else
			real_column_names.push_back(it);

	processed_stage = (cluster.pools.size() + cluster.getLocalNodesNum()) == 1
		? QueryProcessingStage::Complete
		: QueryProcessingStage::WithMergeableState;

	/// Установим sign_rewrite = 0, чтобы второй раз не переписывать запрос
	Settings new_settings = settings;
	new_settings.sign_rewrite = false;
	new_settings.queue_max_wait_ms = Cluster::saturation(new_settings.queue_max_wait_ms, settings.limits.max_execution_time);

	BlockInputStreams res;

	for (ConnectionPools::iterator it = cluster.pools.begin(); it != cluster.pools.end(); ++it)
	{
		String modified_query = selectToString(remakeQuery(query, (*it)->get()->getHost(), (*it)->get()->getPort()));
		res.push_back(new RemoteBlockInputStream((*it)->get(&new_settings), modified_query, &new_settings, virt_column_names[0], virt_column_names[1], processed_stage));
	}

	/// Localhost and 9000 - временное решение, будет испрвлено в ближайшее время.
	ASTPtr modified_query_ast = remakeQuery(query, "localhost", 9000);

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
