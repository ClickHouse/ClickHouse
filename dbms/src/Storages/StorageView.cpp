#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTSelectQuery.h>

#include <DB/Storages/StorageView.h>
#include <DB/Storages/StorageFactory.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>

namespace DB
{


StoragePtr StorageView::create(const String & table_name_, const String & database_name_,
	Context & context_,	ASTPtr & query_, NamesAndTypesListPtr columns_)
{
	return (new StorageView(table_name_, database_name_, context_, query_, columns_))->thisPtr();
}

StorageView::StorageView(const String & table_name_, const String & database_name_,
	Context & context_,	ASTPtr & query_, NamesAndTypesListPtr columns_):
	table_name(table_name_), database_name(database_name_), context(context_), columns(columns_)
{
	ASTCreateQuery & create = dynamic_cast<ASTCreateQuery &>(*query_);
	inner_query = dynamic_cast<ASTSelectQuery &>(*(create.select));

	if (inner_query.database)
		select_database_name = dynamic_cast<ASTIdentifier &>(*inner_query.database).name;
	else
		select_database_name = context.getCurrentDatabase();

	if (inner_query.table)
		select_table_name = dynamic_cast<ASTIdentifier &>(*inner_query.table).name;
	else
		throw Exception("Logical error while creating StorageView."
			" Could not retrieve table name from select query.",
			DB::ErrorCodes::LOGICAL_ERROR);

	context.addDependency(DatabaseAndTableName(select_database_name, select_table_name), DatabaseAndTableName(database_name, table_name));
}

BlockInputStreams StorageView::read(
	const Names & column_names,
	ASTPtr query,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	ASTPtr view_query = getInnerQuery();
	InterpreterSelectQuery result (view_query, context, column_names);
	BlockInputStreams answer;
	answer.push_back(result.execute());
	return answer;
}


void StorageView::dropImpl() {
	context.removeDependency(DatabaseAndTableName(select_database_name, select_table_name), DatabaseAndTableName(database_name, table_name));
}


}
