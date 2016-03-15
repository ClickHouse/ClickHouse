#pragma once

#include <threadpool.hpp>
#include <DB/DatabaseEngines/IDatabaseEngine.h>


namespace DB
{

/** Движок баз данных по-умолчанию.
  * Хранит список таблиц в локальной файловой системе в виде .sql файлов,
  *  содержащих определение таблицы в виде запроса ATTACH TABLE.
  */
class DatabaseEngineOrdinary : public IDatabaseEngine
{
private:
	const String path;
	std::mutex mutex;
	Tables tables;

public:
	DatabaseEngineOrdinary(const String & path_, boost::threadpool::pool & thread_pool_);

	bool isTableExist(const String & name) const override;

	StoragePtr tryGetTable(const String & name) override;

	DatabaseIteratorPtr getIterator() override;

	void addTable(const String & name, StoragePtr & table, const ASTPtr & query, const String & engine) override;

	StoragePtr detachTable(const String & name, bool remove_metadata) override;

	ASTPtr getCreateQuery(const String & name) const override;
};

}
