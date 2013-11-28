#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Client/ConnectionPool.h>
#include <DB/Client/ConnectionPoolWithFailover.h>
#include <DB/Interpreters/Settings.h>
#include <DB/Interpreters/Context.h>


namespace DB
{

/** Распределённая таблица, находящаяся на нескольких серверах.
  * Использует данные заданной БД и таблицы на каждом сервере.
  *
  * Можно передать один адрес, а не несколько.
  * В этом случае, таблицу можно считать удалённой, а не распределённой.
  */
class StorageDistributed : public IStorage
{
public:
	struct Address
	{
		Poco::Net::SocketAddress host_port;
		String user;
		String password;

		Address(const Poco::Net::SocketAddress & host_port_, const String & user_, const String & password_)
			: host_port(host_port_), user(user_), password(password_) {}
	};
	
	/// Массив шардов. Каждый шард - адреса одного сервера.
	typedef std::vector<Address> Addresses;

	/// Массив шардов. Для каждого шарда - массив адресов реплик (серверов, считающихся идентичными).
	typedef std::vector<Addresses> AddressesWithFailover;
	
	static StoragePtr create(
		const std::string & name_,			/// Имя таблицы.
		NamesAndTypesListPtr columns_,		/// Список столбцов.
		const Addresses & addresses,		/// Адреса удалённых серверов.
		const String & remote_database_,	/// БД на удалённых серверах.
		const String & remote_table_,		/// Имя таблицы на удалённых серверах.
		const DataTypeFactory & data_type_factory_,
		const Settings & settings,
		const Context & context_,
		const String & sign_column_name_ = "");
	
	/// Использовать реплики для отказоустойчивости.
	static StoragePtr create(
		const std::string & name_,					/// Имя таблицы.
		NamesAndTypesListPtr columns_,				/// Список столбцов.
		const AddressesWithFailover & addresses,	/// Адреса удалённых серверов с учётом реплик.
		const String & remote_database_,			/// БД на удалённых серверах.
		const String & remote_table_,				/// Имя таблицы на удалённых серверах.
		const DataTypeFactory & data_type_factory_,
		const Settings & settings,
		const Context & context_,
		const String & sign_column_name_ = "");

	std::string getName() const { return "Distributed"; }
	std::string getTableName() const { return name; }
	std::string getSignColumnName() const { return sign_column_name; };
	bool supportsSampling() const { return true; }
	bool supportsFinal() const { return !sign_column_name.empty(); }

	const NamesAndTypesList & getColumnsList() const { return *columns; }

	bool isRemote() const { return true; }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);

	void dropImpl() {}
	void rename(const String & new_path_to_db, const String & new_name) { name = new_name; }
	/// в подтаблицах добавлять и удалять столбы нужно вручную
	/// структура подтаблиц не проверяется
	void alter(const ASTAlterQuery::Parameters &params);

private:
	StorageDistributed(
		const std::string & name_,
		NamesAndTypesListPtr columns_,
		const Addresses & addresses,
		const String & remote_database_,
		const String & remote_table_,
		const DataTypeFactory & data_type_factory_,
		const Settings & settings,
		const Context & context_,
		const String & sign_column_name_ = "");
	
	/// Использовать реплики для отказоустойчивости.
	StorageDistributed(
		const std::string & name_,
		NamesAndTypesListPtr columns_,
		const AddressesWithFailover & addresses,
		const String & remote_database_,
		const String & remote_table_,
		const DataTypeFactory & data_type_factory_,
		const Settings & settings,
		const Context & context_,
		const String & sign_column_name_ = "");

	bool checkLocalReplics(const Address & address);
	
	String name;
	NamesAndTypesListPtr columns;
	String remote_database;
	String remote_table;
	const DataTypeFactory & data_type_factory;
	String sign_column_name;

	const Context & context;
	/// Соединения с удалёнными серверами.
	ConnectionPools pools;

	/// количество реплик clickhouse сервера, расположенных локально
	/// к локальным репликам обращаемся напрямую
	size_t local_replics_num;
};

}
