#pragma once

#include <DB/Storages/StorageLog.h>
#include <DB/Interpreters/Context.h>


namespace DB
{

/** Хранит несколько кусков данных. Читает из всех кусков.
  * Запись не поддерживается. Для записи используются таблицы типа ChunkMerger.
  * Таблицы типа ChunkRef могут ссылаться на отдельные куски внутри таблицы типа Chunks.
  * Хранит количество ссылающихся таблиц ChunkRef и удаляет себя, когда оно становится нулевым.
  * Сразу после создания CREATE-ом, счетчик ссылок имеет значение 1
  *  (потом, движок ChunkMerger добавляет ссылки от созданных ChunkRef-ов и затем вычитает 1).
  */
class StorageChunks : public StorageLog
{
using StorageLog::read;
public:
	static StoragePtr create(const std::string & path_,
							 const std::string & name_,
							 const std::string & database_name_,
							 NamesAndTypesListPtr columns_,
							 const NamesAndTypesList & materialized_columns_,
							 const NamesAndTypesList & alias_columns_,
							 const ColumnDefaults & column_defaults_,
							 Context & context_,
							 bool attach);

	void addReference();
	void removeReference();

	std::string getName() const override { return "Chunks"; }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1) override;

	BlockInputStreams readFromChunk(
		const std::string & chunk_name,
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);

	NameAndTypePair getColumn(const String & column_name) const override;
	bool hasColumn(const String & column_name) const override;

	BlockOutputStreamPtr writeToNewChunk(
		const std::string & chunk_name);

	/// Если бы запись была разрешена, непонятно, как назвать новый чанк.
	BlockOutputStreamPtr write(ASTPtr query, const Settings & settings) override
	{
		throw Exception("Table doesn't support writing", ErrorCodes::NOT_IMPLEMENTED);
	}

	/// Переименование испортило бы целостность количества ссылок из таблиц ChunkRef.
	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override
	{
		throw Exception("Table doesn't support renaming", ErrorCodes::NOT_IMPLEMENTED);
	}

protected:
	/// Виртуальная функция из StorageLog
	/// По номеру засечки получить имя таблицы, из которой идет чтение и номер последней засечки из этой таблицы.
	std::pair<String, size_t> getTableFromMark(size_t mark) const override;
private:
	/// Имя чанка - номер (в последовательности, как чанки записаны в таблице).
	typedef std::map<String, size_t> ChunkIndices;
	/// Номер чанка - засечка, с которой начинаются данные таблицы.
	typedef std::vector<size_t> ChunkNumToMark;
	/// Номер чанка - имя чанка.
	typedef std::vector<String> ChunkNumToChunkName;

	String database_name;

	ChunkNumToMark chunk_num_to_marks;
	ChunkIndices chunk_indices;
	ChunkNumToChunkName chunk_names;

	size_t refcount = 0;
	std::mutex refcount_mutex;

	Context & context;

	Logger * log;

	StorageChunks(const std::string & path_,
				  const std::string & name_,
				  const std::string & database_name_,
				  NamesAndTypesListPtr columns_,
				  const NamesAndTypesList & materialized_columns_,
				  const NamesAndTypesList & alias_columns_,
				  const ColumnDefaults & column_defaults_,
				  Context & context_,
				  bool attach);

	void dropThis();

	void loadIndex();
	void appendChunkToIndex(const std::string & chunk_name, size_t mark);

	Block getBlockWithVirtualColumns() const;
};

}
