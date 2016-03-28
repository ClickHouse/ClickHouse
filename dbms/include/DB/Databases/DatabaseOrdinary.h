#pragma once

#include <threadpool.hpp>
#include <DB/Databases/IDatabase.h>


namespace DB
{

/** Движок баз данных по-умолчанию.
  * Хранит список таблиц в локальной файловой системе в виде .sql файлов,
  *  содержащих определение таблицы в виде запроса ATTACH TABLE.
  */
class DatabaseOrdinary : public IDatabase
{
private:
	const String name;
	const String path;
	mutable std::mutex mutex;
	Tables tables;

	Logger * log;

public:
	DatabaseOrdinary(const String & name_, const String & path_, Context & context, boost::threadpool::pool * thread_pool);

	String getEngineName() const override { return "Ordinary"; }

	bool isTableExist(const String & table_name) const override;
	StoragePtr tryGetTable(const String & table_name) override;

	DatabaseIteratorPtr getIterator() override;

	bool empty() const override;

	void createTable(const String & table_name, const StoragePtr & table, const ASTPtr & query, const String & engine) override;
	void removeTable(const String & table_name) override;

	void attachTable(const String & table_name, const StoragePtr & table) override;
	StoragePtr detachTable(const String & table_name) override;

	void renameTable(const Context & context, const String & table_name, IDatabase & to_database, const String & to_table_name) override;

	ASTPtr getCreateQuery(const String & table_name) const override;

	void shutdown() override;
	void drop() override;
};

}
