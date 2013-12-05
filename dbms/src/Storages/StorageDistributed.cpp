#include <DB/Parsers/formatAST.h>

#include <DB/DataStreams/RemoteBlockInputStream.h>

#include <DB/Storages/StorageDistributed.h>

#include <Poco/Net/NetworkInterface.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <boost/bind.hpp>

namespace DB
{


static Poco::Timespan saturation(const Poco::Timespan & v, const Poco::Timespan & limit)
{
	if (limit.totalMicroseconds() == 0)
		return v;
	else
		return v > limit ? limit : v;
}

StorageDistributed::StorageDistributed(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const StorageDistributed::Addresses & addresses,
	const String & remote_database_,
	const String & remote_table_,
	const DataTypeFactory & data_type_factory_,
	const Settings & settings,
	const Context & context_,
	const String & sign_column_name_)
	: name(name_), columns(columns_),
	remote_database(remote_database_), remote_table(remote_table_),
	data_type_factory(data_type_factory_),
	sign_column_name(sign_column_name_),
	context(context_),
	local_replics_num(0)
{

	for (Addresses::const_iterator it = addresses.begin(); it != addresses.end(); ++it)
	{
		if (checkLocalReplics(*it))
		{
			++local_replics_num;
		}
		else
		{
			pools.push_back(new ConnectionPool(
				settings.distributed_connections_pool_size,
				it->host_port.host().toString(), it->host_port.port(), "", it->user, it->password, data_type_factory, "server", Protocol::Compression::Enable,
				saturation(settings.connect_timeout, settings.limits.max_execution_time),
				saturation(settings.receive_timeout, settings.limits.max_execution_time),
				saturation(settings.send_timeout, settings.limits.max_execution_time)));
		}
	}
}

StorageDistributed::StorageDistributed(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const StorageDistributed::AddressesWithFailover & addresses,
	const String & remote_database_,
	const String & remote_table_,
	const DataTypeFactory & data_type_factory_,
	const Settings & settings,
	const Context & context_,
	const String & sign_column_name_)
	: name(name_), columns(columns_),
	remote_database(remote_database_), remote_table(remote_table_),
	data_type_factory(data_type_factory_),
	sign_column_name(sign_column_name_),
	context(context_),
	local_replics_num(0)
{
	for (AddressesWithFailover::const_iterator it = addresses.begin(); it != addresses.end(); ++it)
	{
		ConnectionPools replicas;
		replicas.reserve(it->size());

		bool has_local_replics = false;
		for (Addresses::const_iterator jt = it->begin(); jt != it->end(); ++jt)
		{
			if (checkLocalReplics(*jt))
			{
				has_local_replics = true;
				break;
			}
			else
			{
				replicas.push_back(new ConnectionPool(
					settings.distributed_connections_pool_size,
					jt->host_port.host().toString(), jt->host_port.port(), "", jt->user, jt->password, data_type_factory, "server", Protocol::Compression::Enable,
					saturation(settings.connect_timeout_with_failover_ms, settings.limits.max_execution_time),
					saturation(settings.receive_timeout, settings.limits.max_execution_time),
					saturation(settings.send_timeout, settings.limits.max_execution_time)));
			}
		}

		if (has_local_replics)
			++local_replics_num;
		else
			pools.push_back(new ConnectionPoolWithFailover(replicas, settings.connections_with_failover_max_tries));
	}
}

static bool interfaceEqual(const Poco::Net::NetworkInterface & interface, Poco::Net::IPAddress & address)
{
	return interface.address() == address;
}

bool StorageDistributed::checkLocalReplics(const Address & address)
{
	///	Если среди реплик существует такая, что:
	/// - её порт совпадает с портом, который слушает сервер;
	/// - её хост резолвится в набор адресов, один из которых совпадает с одним из адресов сетевых интерфейсов сервера
	/// то нужно всегда ходить на этот шард без межпроцессного взаимодействия
	UInt16 clickhouse_port = Poco::Util::Application::instance().config().getInt("tcp_port", 0);
	Poco::Net::NetworkInterface::NetworkInterfaceList interfaces = Poco::Net::NetworkInterface::list();

	if (clickhouse_port == address.host_port.port() &&
		interfaces.end() != std::find_if(interfaces.begin(), interfaces.end(),
										 boost::bind(interfaceEqual, _1, address.host_port.host())))
	{
		LOG_INFO(&Poco::Util::Application::instance().logger(), "Replica with address " << address.host_port.toString() << " will be processed as local.");
		return true;
	}
	return false;
}

StoragePtr StorageDistributed::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const StorageDistributed::Addresses & addresses,
	const String & remote_database_,
	const String & remote_table_,
	const DataTypeFactory & data_type_factory_,
	const Settings & settings,
	const Context & context_,
	const String & sign_column_name_)
{
	return (new StorageDistributed(name_, columns_, addresses, remote_database_, remote_table_, data_type_factory_, settings, context_, sign_column_name_))->thisPtr();
}

StoragePtr StorageDistributed::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const StorageDistributed::AddressesWithFailover & addresses,
	const String & remote_database_,
	const String & remote_table_,
	const DataTypeFactory & data_type_factory_,
	const Settings & settings,
	const Context  & context_,
	const String & sign_column_name_)
{
	return (new StorageDistributed(name_, columns_, addresses, remote_database_, remote_table_, data_type_factory_, settings, context_, sign_column_name_))->thisPtr();
}


BlockInputStreams StorageDistributed::read(
	const Names & column_names,
	ASTPtr query,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	processed_stage = (pools.size() + local_replics_num) == 1
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
	new_settings.queue_max_wait_ms = saturation(new_settings.queue_max_wait_ms, settings.limits.max_execution_time);

	std::stringstream s;
	formatAST(select, s, 0, false, true);
	String modified_query = s.str();

	BlockInputStreams res;

	for (ConnectionPools::iterator it = pools.begin(); it != pools.end(); ++it)
		res.push_back(new RemoteBlockInputStream((*it)->get(), modified_query, &new_settings, processed_stage));


	/// добавляем запросы к локальному clickhouse
	DB::Context new_context = context;
	new_context.setSettings(new_settings);
	{
		DB::Context new_context = context;
		new_context.setSettings(new_settings);
		for(size_t i = 0; i < local_replics_num; ++i)
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
