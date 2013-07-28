#pragma once

#include <statdaemons/OptimizedRegularExpression.h>

#include <DB/Interpreters/Context.h>
#include <DB/Storages/IStorage.h>


namespace DB
{
	
/** То и дело объединяет таблицы, подходящие под регэксп, в таблицы типа Chunks.
  * После объндинения заменяет исходные таблицы таблицами типа ChunkRef.
  * При чтении ведет себя как таблица типа Merge.
  */
class StorageChunkMerger : public IStorage
{
public:
	static StoragePtr create(
		const std::string & this_database_,/// Имя БД для этой таблицы.
		const std::string & name_,			/// Имя таблицы.
		NamesAndTypesListPtr columns_,		/// Список столбцов.
		const String & source_database_,	/// В какой БД искать таблицы-источники.
		const String & table_name_regexp_,	/// Регексп имён таблиц-источников.
		const std::string & destination_name_prefix_, /// Префикс имен создаваемых таблиц типа Chunks.
		size_t chunks_to_merge_,			/// Сколько чанков сливать в одну группу.
		Context & context_);			/// Известные таблицы.
	
	std::string getName() const { return "ChunkMerger"; }
	std::string getTableName() const { return name; }
	
	const NamesAndTypesList & getColumnsList() const { return *columns; }
	
	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);
	
	~StorageChunkMerger();
	
private:
	typedef std::vector<StoragePtr> Storages;
	
	String this_database;
	String name;
	NamesAndTypesListPtr columns;
	String source_database;
	OptimizedRegularExpression table_name_regexp;
	std::string destination_name_prefix;
	size_t chunks_to_merge;
	Context & context;
	
	boost::thread merge_thread;
	Poco::Event cancel_merge_thread;
	
	Logger * log;
	
	StorageChunkMerger(
		const std::string & this_database_,
		const std::string & name_,
		NamesAndTypesListPtr columns_,
		const String & source_database_,
		const String & table_name_regexp_,
		const std::string & destination_name_prefix_,
		size_t chunks_to_merge_,
		Context & context_);
	
	void mergeThread();
	bool maybeMergeSomething();
	Storages selectChunksToMerge();
	bool mergeChunks(const Storages & chunks);

	typedef std::set<std::string> TableNames;
	/// Какие таблицы типа Chunks сейчас пишет хоть один ChunkMerger.
	/// Нужно смотреть, залочив mutex из контекста.
	static TableNames currently_written_groups;
};
	
}
