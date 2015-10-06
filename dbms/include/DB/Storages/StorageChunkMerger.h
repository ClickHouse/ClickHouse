#pragma once

#include <DB/Common/OptimizedRegularExpression.h>

#include <DB/Interpreters/Context.h>
#include <DB/Storages/IStorage.h>
#include <DB/Storages/MergeTree/BackgroundProcessingPool.h>


namespace DB
{

/** То и дело объединяет таблицы, подходящие под регэксп, в таблицы типа Chunks.
  * После объндинения заменяет исходные таблицы таблицами типа ChunkRef.
  * При чтении ведет себя как таблица типа Merge.
  */
class StorageChunkMerger : public IStorage
{
typedef std::vector<StoragePtr> Storages;
public:
	static StoragePtr create(
		const std::string & this_database_,/// Имя БД для этой таблицы.
		const std::string & name_,			/// Имя таблицы.
		NamesAndTypesListPtr columns_,		/// Список столбцов.
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_,
		const String & source_database_,	/// В какой БД искать таблицы-источники.
		const String & table_name_regexp_,	/// Регексп имён таблиц-источников.
		const std::string & destination_name_prefix_, /// Префикс имен создаваемых таблиц типа Chunks.
		size_t chunks_to_merge_,			/// Сколько чанков сливать в одну группу.
		Context & context_);			/// Известные таблицы.

	std::string getName() const override { return "ChunkMerger"; }
	std::string getTableName() const override { return name; }

	bool supportsParallelReplicas() const override { return true; }

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

	void shutdown() override;

	~StorageChunkMerger() override;

private:
	const String this_database;
	const String name;
	NamesAndTypesListPtr columns;
	const String source_database;
	OptimizedRegularExpression table_name_regexp;
	std::string destination_name_prefix;
	const size_t chunks_to_merge;
	Context & context;
	Settings settings;

	Logger * log;

	/// Название виртуального столбца, отвечающего за имя таблицы, из которой идет чтение. (Например "_table")
	String _table_column_name;

	class MergeTask;
	using MergeTaskPtr = std::shared_ptr<MergeTask>;
	MergeTaskPtr merge_task;
	BackgroundProcessingPool::TaskHandle merge_task_handle;

	StorageChunkMerger(
		const std::string & this_database_,
		const std::string & name_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_,
		const String & source_database_,
		const String & table_name_regexp_,
		const std::string & destination_name_prefix_,
		size_t chunks_to_merge_,
		Context & context_);

	Block getBlockWithVirtualColumns(const Storages & selected_tables) const;

	typedef std::set<std::string> TableNames;
	/// Какие таблицы типа Chunks сейчас пишет хоть один ChunkMerger.
	/// Нужно смотреть, залочив mutex из контекста.
	static TableNames currently_written_groups;
};

}
