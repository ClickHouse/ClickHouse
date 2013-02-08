#include <DB/Storages/StorageChunkRef.h>


namespace DB
{
	
StoragePtr StorageChunkRef::create(const std::string & name_, NamesAndTypesListPtr columns_, Context & context_, const std::string & source_database_name_, const std::string & source_table_name_, bool attach)
{
	return (new StorageChunkRef(name_, columns_, context_, source_database_name_, source_table_name_, attach))->thisPtr();
}

BlockInputStreams StorageChunkRef::read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size,
		unsigned threads)
{
	StorageChunks * chunks = getSource();
	if (chunks == NULL)
		throw Exception("Referenced table " + source_table_name + " in database " + source_database_name + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);
	return chunks->readFromChunk(name, column_names, query, settings, processed_stage, max_block_size, threads);
}
	
void StorageChunkRef::dropImpl()
{
	StorageChunks * chunks = getSource();
	if (chunks == NULL)
		LOG_ERROR(&Logger::get("StorageChunkRef"), "Referenced table " + source_table_name + " in database " + source_database_name + " doesn't exist");
	chunks->removeReference();
}
	
StorageChunkRef::StorageChunkRef(const std::string & name_, NamesAndTypesListPtr columns_, Context & context_, const std::string & source_database_name_, const std::string & source_table_name_, bool attach)
	: source_database_name(source_database_name_), source_table_name(source_table_name_), name(name_), columns(columns_), context(context_)
{
	if (!attach)
	{
		StorageChunks * chunks = getSource();
		if (chunks == NULL)
			throw Exception("Referenced table " + source_table_name + " in database " + source_database_name + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);
		chunks->addReference();
	}
}

StorageChunks * StorageChunkRef::getSource()
{
	StoragePtr table_ptr = context.getTable(source_database_name, source_table_name);
	StorageChunks * chunks = dynamic_cast<StorageChunks *>(&*table_ptr);
	return chunks;
}
	
}
