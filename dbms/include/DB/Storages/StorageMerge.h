#pragma once

#include <ext/shared_ptr_helper.hpp>

#include <DB/Common/OptimizedRegularExpression.h>
#include <DB/Storages/IStorage.h>


namespace DB
{

/** Таблица, представляющая собой объединение произвольного количества других таблиц.
  * У всех таблиц должна быть одинаковая структура.
  */
class StorageMerge : private ext::shared_ptr_helper<StorageMerge>, public IStorage
{
friend class ext::shared_ptr_helper<StorageMerge>;

public:
	static StoragePtr create(
		const std::string & name_,			/// Имя таблицы.
		NamesAndTypesListPtr columns_,		/// Список столбцов.
		const String & source_database_,	/// В какой БД искать таблицы-источники.
		const String & table_name_regexp_,	/// Регексп имён таблиц-источников.
		const Context & context_);			/// Известные таблицы.

	static StoragePtr create(
		const std::string & name_,			/// Имя таблицы.
		NamesAndTypesListPtr columns_,		/// Список столбцов.
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_,
		const String & source_database_,	/// В какой БД искать таблицы-источники.
		const String & table_name_regexp_,	/// Регексп имён таблиц-источников.
		const Context & context_);			/// Известные таблицы.

	std::string getName() const override { return "Merge"; }
	std::string getTableName() const override { return name; }

	/// Проверка откладывается до метода read. Там проверяется поддержка у использующихся таблиц.
	bool supportsSampling() const override { return true; }
	bool supportsPrewhere() const override { return true; }
	bool supportsFinal() const override { return true; }
	bool supportsIndexForIn() const override { return true; }

	const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }
	NameAndTypePair getColumn(const String & column_name) const override;
	bool hasColumn(const String & column_name) const override;

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1) override;

	void drop() override {}
	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override { name = new_table_name; }

	/// в подтаблицах добавлять и удалять столбы нужно вручную
	/// структура подтаблиц не проверяется
	void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context) override;

private:
	String name;
	NamesAndTypesListPtr columns;
	String source_database;
	OptimizedRegularExpression table_name_regexp;
	const Context & context;

	StorageMerge(
		const std::string & name_,
		NamesAndTypesListPtr columns_,
		const String & source_database_,
		const String & table_name_regexp_,
		const Context & context_);

	StorageMerge(
		const std::string & name_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_,
		const String & source_database_,
		const String & table_name_regexp_,
		const Context & context_);

	using StorageListWithLocks = std::list<std::pair<StoragePtr, TableStructureReadLockPtr>>;

	StorageListWithLocks getSelectedTables() const;

	Block getBlockWithVirtualColumns(const StorageListWithLocks & selected_tables) const;
};

}
