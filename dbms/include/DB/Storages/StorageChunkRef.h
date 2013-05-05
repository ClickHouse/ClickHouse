#pragma once

#include <DB/Storages/StorageChunks.h>


namespace DB
{
	
/** Ссылка на кусок данных в таблице типа Chunks.
	* Запись не поддерживается.
	*/
class StorageChunkRef : public IStorage
{
public:
	static StoragePtr create(const std::string & name_, NamesAndTypesListPtr columns_, const Context & context_, const std::string & source_database_name_, const std::string & source_table_name_, bool attach);
	
	std::string getName() const { return "ChunkRef"; }
	std::string getTableName() const { return name; }
	
	const NamesAndTypesList & getColumnsList() const { return *columns; }
	
	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);
	
	void dropImpl();
	
	String source_database_name;
	String source_table_name;
	
private:
	String name;
	NamesAndTypesListPtr columns;
	const Context & context;
	
	StorageChunkRef(const std::string & name_, NamesAndTypesListPtr columns_, const Context & context_, const std::string & source_database_name_, const std::string & source_table_name_, bool attach);
	
	StorageChunks * getSource();
};
	
}
