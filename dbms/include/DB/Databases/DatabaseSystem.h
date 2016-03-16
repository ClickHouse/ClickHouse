#pragma once

#include <threadpool.hpp>
#include <DB/Databases/IDatabase.h>


namespace DB
{

/** Реализует фиксированный список системных таблиц.
  */
class DatabaseSystem : public IDatabase
{
	Tables tables;

public:
	DatabaseSystem();

	bool isTableExist(const String & name) const override;

	StoragePtr tryGetTable(const String & name) override;

	DatabaseIteratorPtr getIterator() override;

	void addTable(const String & name, StoragePtr & table, const ASTPtr & query, const String & engine) override;

	StoragePtr detachTable(const String & name, bool remove_metadata) override;

	ASTPtr getCreateQuery(const String & name) const override;
};

}
