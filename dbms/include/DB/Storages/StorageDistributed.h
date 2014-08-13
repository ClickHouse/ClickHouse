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
		Context & context_,
		const ASTPtr & sharding_key_,
		const String & data_path_);

	static StoragePtr create(
		const std::string & name_,			/// Имя таблицы.
		NamesAndTypesListPtr columns_,		/// Список столбцов.
		const String & remote_database_,	/// БД на удалённых серверах.
		const String & remote_table_,		/// Имя таблицы на удалённых серверах.
		SharedPtr<Cluster> & owned_cluster_,
		Context & context_);

	std::string getName() const { return "Distributed"; }
	std::string getTableName() const { return name; }
	bool supportsSampling() const { return true; }
	bool supportsFinal() const { return true; }
	bool supportsPrewhere() const { return true; }

	const NamesAndTypesList & getColumnsList() const { return *columns; }
	NameAndTypePair getColumn(const String &column_name) const;
	bool hasColumn(const String &column_name) const;

	bool isRemote() const { return true; }
	/// Сохранить временные таблицы, чтобы при следующем вызове метода read переслать их на удаленные серверы.
	void storeExternalTables(const Tables & tables_) { external_tables = tables_; }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);

	virtual BlockOutputStreamPtr write(ASTPtr query) override;

	void drop() override {}
	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) { name = new_table_name; }
	/// в подтаблицах добавлять и удалять столбы нужно вручную
	/// структура подтаблиц не проверяется
	void alter(const AlterCommands & params, const String & database_name, const String & table_name, Context & context);

	const ExpressionActionsPtr & getShardingKeyExpr() const { return sharding_key_expr; }
	const String & getShardingKeyColumnName() const { return sharding_key_column_name; }
	const String & getPath() const { return path; }

private:
	StorageDistributed(
		const std::string & name_,
		NamesAndTypesListPtr columns_,
		const String & remote_database_,
		const String & remote_table_,
		Cluster & cluster_,
		const Context & context_,
		const ASTPtr & sharding_key_ = nullptr,
		const String & data_path_ = String{});

	/// Создает копию запроса, меняет имена базы данных и таблицы.
	ASTPtr rewriteQuery(ASTPtr query);

	String name;
	NamesAndTypesListPtr columns;
	String remote_database;
	String remote_table;

	const Context & context;

	/// Временные таблицы, которые необходимо отправить на сервер. Переменная очищается после каждого вызова метода read
	/// Для подготовки к отправке нужно использовтаь метод storeExternalTables
	Tables external_tables;

	/// Используется только, если таблица должна владеть объектом Cluster, которым больше никто не владеет - для реализации TableFunctionRemote.
	SharedPtr<Cluster> owned_cluster;

	/// Соединения с удалёнными серверами.
	Cluster & cluster;

	ExpressionActionsPtr sharding_key_expr;
	String sharding_key_column_name;
	bool write_enabled;
	String path;
};

}
