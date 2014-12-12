#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
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
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	return getSource().readFromChunk(name, column_names, query,
									 context, settings, processed_stage,
									 max_block_size, threads);
}

ASTPtr StorageChunkRef::getCustomCreateQuery(const Context & context) const
{
	/// Берём CREATE запрос для таблицы, на которую эта ссылается, и меняем в ней имя и движок.
	ASTPtr res = context.getCreateQuery(source_database_name, source_table_name);
	ASTCreateQuery & res_create = typeid_cast<ASTCreateQuery &>(*res);

	res_create.database.clear();
	res_create.table = name;

	res_create.storage = new ASTFunction;
	ASTFunction & storage_ast = static_cast<ASTFunction &>(*res_create.storage);
	storage_ast.name = "ChunkRef";
	storage_ast.arguments = new ASTExpressionList;
	storage_ast.children.push_back(storage_ast.arguments);
	ASTExpressionList & args_ast = static_cast<ASTExpressionList &>(*storage_ast.arguments);
	args_ast.children.push_back(new ASTIdentifier(StringRange(), source_database_name, ASTIdentifier::Database));
	args_ast.children.push_back(new ASTIdentifier(StringRange(), source_table_name, ASTIdentifier::Table));

	return res;
}

void StorageChunkRef::drop()
{
	try
	{
		getSource().removeReference();
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
		getSource().addReference();
}

StorageChunks & StorageChunkRef::getSource()
{
	return typeid_cast<StorageChunks &>(*context.getTable(source_database_name, source_table_name));
}

const StorageChunks & StorageChunkRef::getSource() const
{
	const StoragePtr table_ptr = context.getTable(source_database_name, source_table_name);
	const StorageChunks * chunks = typeid_cast<const StorageChunks *>(&*table_ptr);

	if (chunks == nullptr)
		throw Exception("Referenced table " + source_table_name + " in database " + source_database_name + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);

	return *chunks;
}

bool StorageChunkRef::checkData() const
{
	return getSource().checkData();
}


}
