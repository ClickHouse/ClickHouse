#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Client/ConnectionPool.h>
#include <DB/Client/ConnectionPoolWithFailover.h>
#include <DB/Interpreters/Settings.h>


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
	/// Массив шардов. Каждый шард - адреса одного сервера.
	typedef std::vector<Poco::Net::SocketAddress> Addresses;

	/// Массив шардов. Для каждого шарда - массив адресов реплик (серверов, считающихся идентичными).
	typedef std::vector<Addresses> AddressesWithFailover;

	StorageDistributed(
		const std::string & name_,			/// Имя таблицы.
		NamesAndTypesListPtr columns_,		/// Список столбцов.
		const Addresses & addresses,		/// Адреса удалённых серверов.
		const String & remote_database_,	/// БД на удалённых серверах.
		const String & remote_table_,		/// Имя таблицы на удалённых серверах.
		const DataTypeFactory & data_type_factory_,
		const Settings & settings);

	/// Использовать реплики для отказоустойчивости.
	StorageDistributed(
		const std::string & name_,					/// Имя таблицы.
		NamesAndTypesListPtr columns_,				/// Список столбцов.
		const AddressesWithFailover & addresses,	/// Адреса удалённых серверов с учётом реплик.
		const String & remote_database_,			/// БД на удалённых серверах.
		const String & remote_table_,				/// Имя таблицы на удалённых серверах.
		const DataTypeFactory & data_type_factory_,
		const Settings & settings);

	std::string getName() const { return "Distributed"; }
	std::string getTableName() const { return name; }
	bool supportsSampling() const { return true; }

	const NamesAndTypesList & getColumnsList() const { return *columns; }

	bool isRemote() const { return true; }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);

	void dropImpl() {}
	void rename(const String & new_path_to_db, const String & new_name) { name = new_name; }

private:
	String name;
	NamesAndTypesListPtr columns;
	String remote_database;
	String remote_table;
	const DataTypeFactory & data_type_factory;

	/// Соединения с удалёнными серверами.
	ConnectionPools pools;
};

}
