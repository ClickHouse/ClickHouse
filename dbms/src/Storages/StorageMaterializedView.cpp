#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ParserCreateQuery.h>

#include <DB/Storages/StorageChunkMerger.h>
#include <DB/Storages/StorageMaterializedView.h>
#include <DB/Storages/StorageFactory.h>

#include <DB/Interpreters/InterpreterCreateQuery.h>

namespace DB
{

StoragePtr StorageMaterializedView::create(const String & table_name_, const String & database_name_,
	Context & context_,	ASTPtr & query_, NamesAndTypesListPtr columns_, bool attach_)
{
	return (new StorageMaterializedView(table_name_, database_name_, context_, query_, columns_, attach_))->thisPtr();
}

StorageMaterializedView::StorageMaterializedView(const String & table_name_, const String & database_name_,
	Context & context_,	ASTPtr & query_, NamesAndTypesListPtr columns_,	bool attach_):
	StorageView(table_name_, database_name_, context_, query_, columns_)
{
	ASTCreateQuery & create = dynamic_cast<ASTCreateQuery &>(*query_);

	/// Если не указан в запросе тип хранилища попробовать извлечь его из запроса Select
	if (!create.inner_storage)
		inner_storage_name = context.getTable(select_database_name, select_table_name)->getName();
	else
		inner_storage_name = dynamic_cast<ASTFunction &>(*(create.inner_storage)).name;

	/// Составим запрос для создания внутреннего хранилища.
	ASTPtr ast_create_query;
	if (attach_)
	{
		ast_create_query = context.getCreateQuery(database_name_, getInnerTableName());
	}
	else
	{
		String formatted_columns = formatColumnsForCreateQuery(*columns);
		String create_query = "CREATE TABLE " + database_name_ + "." + getInnerTableName() + " " + formatted_columns + " ENGINE = " + inner_storage_name;
		/// Распарсим запрос.
		const char * begin = create_query.data();
		const char * end = begin + create_query.size();
		const char * pos = begin;
		ParserCreateQuery parser;
		String expected;
		bool parse_res = parser.parse(pos, end, ast_create_query, expected);
		/// Распарсенный запрос должен заканчиваться на конец входных данных.
		if (!parse_res || pos != end)
			throw Exception("Syntax error while parsing create query made by StorageMaterializedView."
				" The query is \"" + create_query + "\"."
				+ " Failed at position " + toString(pos - begin) + ": "
				+ std::string(pos, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, end - pos))
				+ ", expected " + (parse_res ? "end of query" : expected) + ".",
				DB::ErrorCodes::LOGICAL_ERROR);
	}

	/// Выполним запрос.
	InterpreterCreateQuery create_interpreter(ast_create_query, context_);
	data = create_interpreter.execute();
}

BlockInputStreams StorageMaterializedView::read(
	const Names & column_names,
	ASTPtr query,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	return data->read(column_names, query, settings, processed_stage, max_block_size, threads);
}

BlockOutputStreamPtr StorageMaterializedView::write(ASTPtr query)
{
	return data->write(query);
}

void StorageMaterializedView::dropImpl() {
	context.removeDependency(DatabaseAndTableName(select_database_name, select_table_name), DatabaseAndTableName(database_name, table_name));
	data->dropImpl();
}

bool StorageMaterializedView::optimize() {
	return data->optimize();
}


}
