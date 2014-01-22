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
	static StoragePtr create(
		const std::string & name_,			/// Имя таблицы.
		NamesAndTypesListPtr columns_,		/// Список столбцов.
		const String & remote_database_,	/// БД на удалённых серверах.
		const String & remote_table_,		/// Имя таблицы на удалённых серверах.
		const String & cluster_name,
		const DataTypeFactory & data_type_factory_,
		const Settings & settings,
		Context & context_,
		const String & sign_column_name_ = "");

	std::string getName() const { return "Distributed"; }
	std::string getTableName() const { return name; }
	std::string getSignColumnName() const { return sign_column_name; };
	bool supportsSampling() const { return true; }
	bool supportsFinal() const { return !sign_column_name.empty(); }
	bool supportsPrewhere() const { return true; }

	const NamesAndTypesList & getColumnsList() const { return *columns; }
	NameAndTypePair getColumn(const String &column_name) const;
	bool hasColumn(const String &column_name) const;

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
		const String & remote_database_,
		const String & remote_table_,
		Cluster & cluster_,
		const DataTypeFactory & data_type_factory_,
		const Settings & settings,
		const Context & context_,
		const String & sign_column_name_ = "");
	
	String name;
	NamesAndTypesListPtr columns;
	String remote_database;
	String remote_table;
	const DataTypeFactory & data_type_factory;
	String sign_column_name;

	String _host_column_name;
	String _port_column_name;

	const Context & context;
	Cluster & cluster;
};

}
