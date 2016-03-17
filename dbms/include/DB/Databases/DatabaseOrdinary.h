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
	std::mutex mutex;
	Tables tables;

public:
	DatabaseOrdinary(const String & name_, const String & path_, boost::threadpool::pool * thread_pool_);

	bool isTableExist(const String & name) const override;

	StoragePtr tryGetTable(const String & name) override;

	DatabaseIteratorPtr getIterator() override;

	bool empty() const override;

	void createTable(const String & name, StoragePtr & table, const ASTPtr & query, const String & engine) override;

	StoragePtr removeTable(const String & name) override;

	void attachTable(const String & name, StoragePtr & table) override;

	StoragePtr detachTable(const String & name) override;

	ASTPtr getCreateQuery(const String & name) const override;

	void shutdown() override;
};

}
