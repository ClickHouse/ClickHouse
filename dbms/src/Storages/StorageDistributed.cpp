#include <DB/Parsers/formatAST.h>

#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/DataStreams/RemoveColumnsBlockInputStream.h>

#include <DB/Storages/StorageDistributed.h>

#include <Poco/Net/NetworkInterface.h>
#include <DB/Client/ConnectionPool.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
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


StoragePtr StorageDistributed::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & remote_database_,
	const String & remote_table_,
	Cluster & cluster_,
	const DataTypeFactory & data_type_factory_,
	const Settings & settings,
	Context & context_,
	const String & sign_column_name_)
{
	return (new StorageDistributed(name_, columns_, remote_database_, remote_table_, cluster_, data_type_factory_, settings, context_, sign_column_name_))->thisPtr();
}

NameAndTypePair StorageDistributed::getColumn(const String &column_name) const
{
	if (column_name == _host_column_name)
		return std::make_pair(_host_column_name, new DataTypeString);
	if (column_name == _port_column_name)
		return std::make_pair(_port_column_name, new DataTypeUInt16);

	return getRealColumn(column_name);
}

bool StorageDistributed::hasColumn(const String &column_name) const
{
	if (column_name == _host_column_name)
		return true;
	if (column_name == _port_column_name)
		return true;

	return hasRealColumn(column_name);
}

ASTPtr StorageDistributed::remakeQuery(ASTPtr query, const String & host, size_t port)
{
	/// Создаем копию запроса.
	ASTPtr modified_query_ast = query->clone();

	/// Добавляем в запрос значения хоста и порта, если требуется.
	if (!host.empty())
		VirtualColumnUtils::rewriteEntityInAst(modified_query_ast, _host_column_name, host);
	if (port != 0)
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
	/// Узнаем на каком порту слушает сервер
	UInt16 clickhouse_port = Poco::Util::Application::instance().config().getInt("tcp_port", 0);

	/// Установим sign_rewrite = 0, чтобы второй раз не переписывать запрос
	Settings new_settings = settings;
	new_settings.sign_rewrite = false;
	new_settings.queue_max_wait_ms = Cluster::saturation(new_settings.queue_max_wait_ms, settings.limits.max_execution_time);

	/** Запрошены ли виртуальные столбцы?
	  * Если да - будем добавлять их в виде констант в запрос, предназначенный для выполнения на удалённом сервере,
	  *  а также при получении результата с удалённого сервера.
	  */
	bool need_host_column = false;
	bool need_port_column = false;
	for (const auto & it : column_names)
	{
		if (it == _host_column_name)
			need_host_column = true;
		else if (it == _port_column_name)
			need_port_column = true;
	}

	/** Есть ли виртуальные столбцы в секции селект?
	  * Если нет - в случае вычисления запроса до стадии Complete, необходимо удалить их из блока.
	  */
	bool select_host_column = false;
	bool select_port_column = false;
	const ASTExpressionList & select_list = dynamic_cast<const ASTExpressionList &>(*(dynamic_cast<const ASTSelectQuery &>(*query)).select_expression_list);
	for (const auto & it : select_list.children)
	{
		if (const ASTIdentifier * identifier = dynamic_cast<const ASTIdentifier *>(&*it))
		{
			if (identifier->name == _host_column_name)
				select_host_column = true;
			else if (identifier->name == _port_column_name)
				select_port_column = true;
		}
	}

	Names columns_to_remove;
	if (!select_host_column)
		columns_to_remove.push_back(_host_column_name);
	if (!select_port_column)
		columns_to_remove.push_back(_port_column_name);

	Block virtual_columns_block = getBlockWithVirtualColumns();
	BlockInputStreamPtr virtual_columns =
		VirtualColumnUtils::getVirtualColumnsBlocks(query->clone(), virtual_columns_block, context);
	std::set< std::pair<String, UInt16> > values =
		VirtualColumnUtils::extractTwoValuesFromBlocks<String, UInt16>(virtual_columns, _host_column_name, _port_column_name);
	bool all_inclusive = (values.size() == virtual_columns_block.rows());

	size_t result_size = values.size();
	if (values.find(std::make_pair("localhost", clickhouse_port)) != values.end())
		result_size += cluster.getLocalNodesNum() - 1;

	processed_stage = result_size == 1
		? QueryProcessingStage::Complete
		: QueryProcessingStage::WithMergeableState;

	BlockInputStreams res;

	for (ConnectionPools::iterator it = cluster.pools.begin(); it != cluster.pools.end(); ++it)
	{
		String current_host = (*it)->get()->getHost();
		UInt16 current_port = (*it)->get()->getPort();

		if (!all_inclusive && values.find(std::make_pair(current_host, current_port)) == values.end())
			continue;

		String modified_query = selectToString(remakeQuery(
			query,
			need_host_column ? current_host : "",
			need_port_column ? current_port : 0));

		BlockInputStreamPtr temp = new RemoteBlockInputStream(
			(*it)->get(&new_settings),
			modified_query,
			&new_settings,
			need_host_column ? _host_column_name : "",
			need_port_column ? _port_column_name : "",
			processed_stage);

		if (processed_stage == QueryProcessingStage::WithMergeableState)
			res.push_back(temp);
		else
			res.push_back(new RemoveColumnsBlockInputStream(temp, columns_to_remove));
	}

	if (all_inclusive || values.find(std::make_pair("localhost", clickhouse_port)) != values.end())
	{
		ASTPtr modified_query_ast = remakeQuery(
			query,
			need_host_column ? "localhost" : "",
			need_port_column ? clickhouse_port : 0);

		/// добавляем запросы к локальному ClickHouse
		DB::Context new_context = context;
		new_context.setSettings(new_settings);

		for(size_t i = 0; i < cluster.getLocalNodesNum(); ++i)
		{
			InterpreterSelectQuery interpreter(modified_query_ast, new_context, processed_stage);
			if (processed_stage == QueryProcessingStage::WithMergeableState)
				res.push_back(interpreter.execute());
			else
				res.push_back(new RemoveColumnsBlockInputStream(interpreter.execute(), columns_to_remove));
		}
	}
	
	return res;
}

/// Построить блок состоящий только из возможных значений виртуальных столбцов
Block StorageDistributed::getBlockWithVirtualColumns()
{
	Block res;
	ColumnWithNameAndType _host(new ColumnString, new DataTypeString, _host_column_name);
	ColumnWithNameAndType _port(new ColumnUInt16, new DataTypeUInt16, _port_column_name);

	for (ConnectionPools::iterator it = cluster.pools.begin(); it != cluster.pools.end(); ++it)
	{
		_host.column->insert((*it)->get()->getHost());
		_port.column->insert(static_cast<uint64>((*it)->get()->getPort()));
	}

	if (cluster.getLocalNodesNum() > 0)
	{
		/// Узнаем на каком порту слушает сервер
		UInt64 clickhouse_port = Poco::Util::Application::instance().config().getInt("tcp_port", 0);
		String clockhouse_host = "localhost";
		_host.column->insert(clockhouse_host);
		_port.column->insert(clickhouse_port);
	}
	res.insert(_host);
	res.insert(_port);

	return res;
}

void StorageDistributed::alter(const ASTAlterQuery::Parameters &params)
{
	alterColumns(params, columns, context);
}

}
