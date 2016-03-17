#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Storages/StorageChunkRef.h>
#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_TABLE;
}


StoragePtr StorageChunkRef::create(const std::string & name_, const Context & context_, const std::string & source_database_name_, const std::string & source_table_name_, bool attach)
{
	return (new StorageChunkRef(name_, context_, source_database_name_, source_table_name_, attach))->thisPtr();
}

BlockInputStreams StorageChunkRef::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	return typeid_cast<StorageChunks &>(*getSource()).readFromChunk(
		name, column_names, query,
		context, settings, processed_stage,
		max_block_size, threads);
}

void StorageChunkRef::drop()
{
	try
	{
		typeid_cast<StorageChunks &>(*getSource()).removeReference();
	}
	catch (const Exception & e)
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
	if (!attach)
		typeid_cast<StorageChunks &>(*getSource()).addReference();
}

StoragePtr StorageChunkRef::getSource()
{
	return context.getTable(source_database_name, source_table_name);
}

const StoragePtr StorageChunkRef::getSource() const
{
	const StoragePtr table_ptr = context.getTable(source_database_name, source_table_name);

	if (!table_ptr)
		throw Exception("Referenced table " + source_table_name + " in database " + source_database_name + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);

	return table_ptr;
}

bool StorageChunkRef::checkData() const
{
	return getSource()->checkData();
}


}
