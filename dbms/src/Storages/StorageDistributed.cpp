#include <DB/Parsers/formatAST.h>

#include <DB/DataStreams/RemoteBlockInputStream.h>

#include <DB/Storages/StorageDistributed.h>


namespace DB
{

StorageDistributed::StorageDistributed(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const StorageDistributed::Addresses & addresses,
	const String & remote_database_,
	const String & remote_table_,
	const DataTypeFactory & data_type_factory_,
	const Settings & settings)
	: name(name_), columns(columns_),
	remote_database(remote_database_), remote_table(remote_table_),
	data_type_factory(data_type_factory_)
{
	for (Addresses::const_iterator it = addresses.begin(); it != addresses.end(); ++it)
		pools.push_back(new ConnectionPool(
			settings.distributed_connections_pool_size,
			it->host().toString(), it->port(), "", data_type_factory, "server", Protocol::Compression::Enable,
			settings.connect_timeout, settings.receive_timeout, settings.send_timeout));
}

StorageDistributed::StorageDistributed(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const StorageDistributed::AddressesWithFailover & addresses,
	const String & remote_database_,
	const String & remote_table_,
	const DataTypeFactory & data_type_factory_,
	const Settings & settings)
	: name(name_), columns(columns_),
	remote_database(remote_database_), remote_table(remote_table_),
	data_type_factory(data_type_factory_)
{
	for (AddressesWithFailover::const_iterator it = addresses.begin(); it != addresses.end(); ++it)
	{
		ConnectionPools replicas;
		replicas.reserve(it->size());

		for (Addresses::const_iterator jt = it->begin(); jt != it->end(); ++jt)
			replicas.push_back(new ConnectionPool(
				settings.distributed_connections_pool_size,
				jt->host().toString(), jt->port(), "", data_type_factory, "server", Protocol::Compression::Enable,
				settings.connect_timeout, settings.receive_timeout, settings.send_timeout));

		pools.push_back(new ConnectionPoolWithFailover(replicas, settings.connections_with_failover_max_tries));
	}
}


BlockInputStreams StorageDistributed::read(
	const Names & column_names,
	ASTPtr query,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	processed_stage = pools.size() == 1
		? QueryProcessingStage::Complete
		: QueryProcessingStage::WithMergeableState;
	
	/// Заменим в запросе имена БД и таблицы.
	ASTPtr modified_query_ast = query->clone();
	ASTSelectQuery & select = dynamic_cast<ASTSelectQuery &>(*modified_query_ast);
	select.database = new ASTIdentifier(StringRange(), remote_database, ASTIdentifier::Database);
	select.table 	= new ASTIdentifier(StringRange(), remote_table, 	ASTIdentifier::Table);

	std::stringstream s;
	formatAST(select, s, 0, false, true);
	String modified_query = s.str();

	BlockInputStreams res;

	for (ConnectionPools::iterator it = pools.begin(); it != pools.end(); ++it)
		res.push_back(new RemoteBlockInputStream((*it)->get(), modified_query, processed_stage));

	return res;
}

}
