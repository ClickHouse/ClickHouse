#include <DB/Storages/StorageChunkRef.h>


namespace DB
{
	
StoragePtr StorageChunkRef::create(const std::string & name_, const Context & context_, const std::string & source_database_name_, const std::string & source_table_name_, bool attach)
{
	return (new StorageChunkRef(name_, context_, source_database_name_, source_table_name_, attach))->thisPtr();
}

BlockInputStreams StorageChunkRef::read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size,
		unsigned threads)
{
	return getSource().readFromChunk(name, column_names, query, settings, processed_stage, max_block_size, threads);
}

void StorageChunkRef::dropImpl()
{
	try
	{
		getSource().removeReference();
	}
	catch (const DB::Exception & e)
	{
		if (e.code() != ErrorCodes::UNKNOWN_TABLE)
			throw;

		LOG_ERROR(&Logger::get("StorageChunkRef"), e.displayText());
		/// Если таблицы с данными не существует - дополнительных действий при удалении не требуется.
	}
}

StorageChunkRef::StorageChunkRef(const std::string & name_, const Context & context_, const std::string & source_database_name_, const std::string & source_table_name_, bool attach)
	: source_database_name(source_database_name_), source_table_name(source_table_name_), name(name_), context(context_)
{
	if (!attach)	/// TODO: не понял, как гарантируется правильность учёта ссылок при старте сервера.
		getSource().addReference();
}

StorageChunks & StorageChunkRef::getSource()
{
	StoragePtr table_ptr = context.getTable(source_database_name, source_table_name);
	StorageChunks * chunks = dynamic_cast<StorageChunks *>(&*table_ptr);

	if (chunks == NULL)
		throw Exception("Referenced table " + source_table_name + " in database " + source_database_name + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);

	return *chunks;
}

const StorageChunks & StorageChunkRef::getSource() const
{
	const StoragePtr table_ptr = context.getTable(source_database_name, source_table_name);
	const StorageChunks * chunks = dynamic_cast<const StorageChunks *>(&*table_ptr);

	if (chunks == NULL)
		throw Exception("Referenced table " + source_table_name + " in database " + source_database_name + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);

	return *chunks;
}
	
}
