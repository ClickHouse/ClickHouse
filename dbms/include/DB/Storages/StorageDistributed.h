#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Client/ConnectionPool.h>
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
	typedef std::vector<Poco::Net::SocketAddress> Addresses;
	
	StorageDistributed(
		const std::string & name_,			/// Имя таблицы.
		NamesAndTypesListPtr columns_,		/// Список столбцов.
		const Addresses & addresses_,		/// Адреса удалённых серверов.
		const String & remote_database_,	/// БД на удалённых серверах.
		const String & remote_table_,		/// Имя таблицы на удалённых серверах.
		const DataTypeFactory & data_type_factory_,
		const Settings & settings);

	std::string getName() const { return "Distributed"; }
	std::string getTableName() const { return name; }

	const NamesAndTypesList & getColumnsList() const { return *columns; }

	bool isRemote() const { return true; }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);

	void drop() {}
	void rename(const String & new_path_to_db, const String & new_name) { name = new_name; }

private:
	String name;
	NamesAndTypesListPtr columns;
	Addresses addresses;
	String remote_database;
	String remote_table;
	const DataTypeFactory & data_type_factory;

	/// Соединения с удалёнными серверами.
	ConnectionPools pools;
};

}
