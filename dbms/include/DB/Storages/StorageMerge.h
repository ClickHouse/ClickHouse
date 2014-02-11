#pragma once

#include <statdaemons/OptimizedRegularExpression.h>

#include <DB/Interpreters/Context.h>
#include <DB/Storages/IStorage.h>


namespace DB
{

class StorageMerge;
typedef Poco::SharedPtr<StorageMerge> StorageMergePtr;

/** Таблица, представляющая собой объединение произвольного количества других таблиц.
  * У всех таблиц должна быть одинаковая структура.
  */
class StorageMerge : public IStorage
{
typedef std::vector<StoragePtr> SelectedTables;

public:
	static StoragePtr create(
		const std::string & name_,			/// Имя таблицы.
		NamesAndTypesListPtr columns_,		/// Список столбцов.
		const String & source_database_,	/// В какой БД искать таблицы-источники.
		const String & table_name_regexp_,	/// Регексп имён таблиц-источников.
		const Context & context_);			/// Известные таблицы.

	std::string getName() const { return "Merge"; }
	std::string getTableName() const { return name; }
	bool supportsSampling() const { return true; }

	const NamesAndTypesList & getColumnsList() const { return *columns; }
	NameAndTypePair getColumn(const String &column_name) const;
	bool hasColumn(const String &column_name) const;

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);

	void dropImpl() {}
	void rename(const String & new_path_to_db, const String & new_name) { name = new_name; }
	
	void getSelectedTables(StorageVector & selected_tables);

	/// в подтаблицах добавлять и удалять столбы нужно вручную
	/// структура подтаблиц не проверяется
	void alter(const ASTAlterQuery::Parameters & params);

	Block getBlockWithVirtualColumns(const std::vector<StoragePtr> & selected_tables) const;
private:
	String name;
	NamesAndTypesListPtr columns;
	String source_database;
	OptimizedRegularExpression table_name_regexp;
	const Context & context;
	
	/// Название виртуального столбца, отвечающего за имя таблицы, из которой идет чтение. (Например "_table")
	String _table_column_name;
	
	StorageMerge(
		const std::string & name_,
		NamesAndTypesListPtr columns_,
		const String & source_database_,
		const String & table_name_regexp_,
		const Context & context_);
};

}
