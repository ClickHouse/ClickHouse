#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Storages/StorageFactory.h>
#include <DB/Databases/DatabasesCommon.h>


namespace DB
{


String getTableDefinitionFromCreateQuery(const ASTPtr & query)
{
	ASTPtr query_clone = query->clone();
	ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_clone.get());

	/// Удаляем из запроса всё, что не нужно для ATTACH.
	create.attach = true;
	create.database.clear();
	create.as_database.clear();
	create.as_table.clear();
	create.if_not_exists = false;
	create.is_populate = false;

	String engine = typeid_cast<ASTFunction &>(*create.storage).name;

	/// Для engine VIEW необходимо сохранить сам селект запрос, для остальных - наоборот
	if (engine != "View" && engine != "MaterializedView")
		create.select = nullptr;

	std::ostringstream statement_stream;
	formatAST(create, statement_stream, 0, false);
	statement_stream << '\n';
	return statement_stream.str();
}


std::pair<String, StoragePtr> createTableFromDefinition(
	const String & definition,
	const String & database_name,
	const String & database_data_path,
	Context & context,
	const String & description_for_error_message)
{
	ParserCreateQuery parser;
	ASTPtr ast = parseQuery(parser, definition.data(), definition.data() + definition.size(), description_for_error_message);

	ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
	ast_create_query.attach = true;
	ast_create_query.database = database_name;

	/// Не используем напрямую InterpreterCreateQuery::execute, так как:
	/// - база данных ещё не создана;
	/// - код проще, так как запрос уже приведён к подходящему виду.

	InterpreterCreateQuery::ColumnsInfo columns_info = InterpreterCreateQuery::getColumnsInfo(ast_create_query.columns, context);

	String storage_name;

	if (ast_create_query.is_view)
		storage_name = "View";
	else if (ast_create_query.is_materialized_view)
		storage_name = "MaterializedView";
	else
		storage_name = typeid_cast<ASTFunction &>(*ast_create_query.storage).name;

	return
	{
		ast_create_query.table,
		StorageFactory::instance().get(
			storage_name, database_data_path, ast_create_query.table, database_name, context,
			context.getGlobalContext(), ast, columns_info.columns,
			columns_info.materialized_columns, columns_info.alias_columns, columns_info.column_defaults,
			true)
	};
}

}
