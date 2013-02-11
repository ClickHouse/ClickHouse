#pragma once

#include <DB/Storages/StorageLog.h>
#include <DB/Interpreters/Context.h>
#include <statdaemons/CounterInFile.h>


namespace DB
{
	
/** Хранит несколько кусков данных. Читает из всех кусков.
  * Запись не поддерживается. Для записи используются таблицы типа ChunkMerger.
  * Таблицы типа ChunkRef могут ссылаться на отдельные куски внутри таблицы типа Chunks.
  * Хранит количество ссылающихся таблиц ChunkRef и удаляет себя, когда оно становится нулевым.
  * После создания счетчик ссылок имеет значение 1.
  */
class StorageChunks : public StorageLog
{
public:
	static StoragePtr create(const std::string & path_,
							const std::string & name_,
							const std::string & database_name_,
							NamesAndTypesListPtr columns_,
							Context & context,
							bool attach);
	
	void addReference();
	void removeReference();
	
	std::string getName() const { return "Chunks"; }
	
	BlockInputStreams readFromChunk(
		const std::string & chunk_name,
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);
	
	BlockOutputStreamPtr writeToNewChunk(
		const std::string & chunk_name);
	
	/// Если бы запись была разрешена, непонятно, как назвать новый чанк.
	BlockOutputStreamPtr write(
		ASTPtr query)
	{
		throw Exception("Table doesn't support writing", ErrorCodes::NOT_IMPLEMENTED);
	}
	
	/// Переименование испортило бы целостность количества ссылок из таблиц ChunkRef.
	void rename(const String & new_path_to_db, const String & new_name)
	{
		throw Exception("Table doesn't support renaming", ErrorCodes::NOT_IMPLEMENTED);
	}
private:
	typedef std::vector<size_t> Marks;
	typedef std::map<String, size_t> ChunkIndices;
	
	String database_name;
	
	bool index_loaded;
	Marks marks;
	ChunkIndices chunk_indices;
	
	CounterInFile reference_counter;
	Context context;
	
	StorageChunks(const std::string & path_,
				const std::string & name_,
				const std::string & database_name_,
				NamesAndTypesListPtr columns_,
				Context & context_,
				bool attach);
	
	void dropThis();
	
	void loadIndex();
	void appendChunkToIndex(const std::string & chunk_name, size_t mark);
};
	
}
