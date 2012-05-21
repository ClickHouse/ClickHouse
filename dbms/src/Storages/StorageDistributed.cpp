#include <DB/Parsers/formatAST.h>

#include <DB/DataStreams/RemoteBlockInputStream.h>

#include <DB/Storages/StorageDistributed.h>


namespace DB
{

StorageDistributed::StorageDistributed(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const StorageDistributed::Addresses & addresses_,
	const String & remote_database_,
	const String & remote_table_,
	DataTypeFactory & data_type_factory_)
	: name(name_), columns(columns_), addresses(addresses_),
	remote_database(remote_database_), remote_table(remote_table_),
	data_type_factory(data_type_factory_)
{
	for (Addresses::const_iterator it = addresses.begin(); it != addresses.end(); ++it)
		connections.push_back(new Connection(it->host().toString(), it->port(), data_type_factory));
}


BlockInputStreams StorageDistributed::read(
	const Names & column_names,
	ASTPtr query,
	size_t max_block_size,
	unsigned max_threads)
{
	/// Заменим в запросе имена БД и таблицы.
	ASTPtr modified_query_ast = query->clone();
	ASTSelectQuery & select = dynamic_cast<ASTSelectQuery &>(*modified_query_ast);
	select.database = new ASTIdentifier(StringRange(), remote_database, ASTIdentifier::Database);
	select.table 	= new ASTIdentifier(StringRange(), remote_table, 	ASTIdentifier::Table);

	std::stringstream s;
	formatAST(select, s, 0, false);
	String modified_query = s.str();

	BlockInputStreams res;

	for (Connections::iterator it = connections.begin(); it != connections.end(); ++it)
		res.push_back(new RemoteBlockInputStream(**it, modified_query, QueryProcessingStage::WithMergeableState));

	return res;
}

}
