#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Client/Connection.h>


namespace DB
{

/** Распределённая таблица, находящаяся на нескольких серверах.
  * Использует данные заданной БД и таблицы на каждом сервере.
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
		DataTypeFactory & data_type_factory_);

	std::string getName() const { return "Distributed"; }
	std::string getTableName() const { return name; }

	const NamesAndTypesList & getColumnsList() const { return *columns; }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned max_threads = 1);

	void drop() {}

private:
	const String name;
	NamesAndTypesListPtr columns;
	Addresses addresses;
	String remote_database;
	String remote_table;
	DataTypeFactory & data_type_factory;

	/// Соединения с удалёнными серверами.
	Connections connections;
};

}
