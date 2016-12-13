#pragma once

#include <DB/Databases/IDatabase.h>
#include <DB/Storages/IStorage.h>


namespace DB
{

/** A non-persistent database to store temporary data.
  * It doesn't make any manipulations with filesystem.
  * All tables are created by calling code.
  * TODO: Maybe DatabaseRuntime is more suitable class name.
  */
class DatabaseMemory : public IDatabase
{
protected:
	const String name;
	mutable std::mutex mutex;
	Tables tables;

	Logger * log;

public:

	DatabaseMemory(const String & name_) : name(name_) {}

	String getEngineName() const override { return "Memory"; }

	void loadTables(Context & context, ThreadPool * thread_pool, bool has_force_restore_data_flag) override;

	bool isTableExist(const String & table_name) const override;
	StoragePtr tryGetTable(const String & table_name) override;

	DatabaseIteratorPtr getIterator() override;

	bool empty() const override;

	void createTable(const String & table_name, const StoragePtr & table, const ASTPtr & query, const String & engine) override;
	void removeTable(const String & table_name) override;

	void attachTable(const String & table_name, const StoragePtr & table) override;
	StoragePtr detachTable(const String & table_name) override;

	void renameTable(const Context & context, const String & table_name, IDatabase & to_database, const String & to_table_name) override;

	time_t getTableMetadataModificationTime(const String & table_name) override;

	ASTPtr getCreateQuery(const String & table_name) const override;

	void shutdown() override;
	void drop() override;

	void alterTable(
		const Context & context,
		const String & name,
		const NamesAndTypesList & columns,
		const NamesAndTypesList & materialized_columns,
		const NamesAndTypesList & alias_columns,
		const ColumnDefaults & column_defaults,
		const ASTModifier & engine_modifier) override;
};

}
