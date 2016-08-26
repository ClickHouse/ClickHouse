#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTSelectQuery.h>

#include <DB/Storages/StorageView.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}


StoragePtr StorageView::create(
	const String & table_name_,
	const String & database_name_,
	Context & context_,
	ASTPtr & query_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_)
{
	return make_shared(
		table_name_, database_name_, context_, query_,
		columns_, materialized_columns_, alias_columns_, column_defaults_
	);
}


StorageView::StorageView(
	const String & table_name_,
	const String & database_name_,
	Context & context_,
	ASTPtr & query_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_)
	: IStorage{materialized_columns_, alias_columns_, column_defaults_}, table_name(table_name_),
	database_name(database_name_), context(context_), columns(columns_)
{
	ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_);
	ASTSelectQuery & select = typeid_cast<ASTSelectQuery &>(*create.select);

	/// Если во внутреннем запросе не указана база данных, получить ее из контекста и записать в запрос.
	select.setDatabaseIfNeeded(database_name);

	inner_query = select;

	extractDependentTable(inner_query);

	if (!select_table_name.empty())
		context.getGlobalContext().addDependency(
			DatabaseAndTableName(select_database_name, select_table_name),
			DatabaseAndTableName(database_name, table_name));
}


void StorageView::extractDependentTable(const ASTSelectQuery & query)
{
	auto query_table = query.table();

	if (!query_table)
		return;

	if (const ASTIdentifier * ast_id = typeid_cast<const ASTIdentifier *>(query_table.get()))
	{
		auto query_database = query.database();

		if (!query_database)
			throw Exception("Logical error while creating StorageView."
				" Could not retrieve database name from select query.",
				DB::ErrorCodes::LOGICAL_ERROR);

		select_database_name = typeid_cast<const ASTIdentifier &>(*query_database).name;
		select_table_name = ast_id->name;
	}
	else if (const ASTSelectQuery * ast_select = typeid_cast<const ASTSelectQuery *>(query_table.get()))
	{
		extractDependentTable(*ast_select);
	}
	else
		throw Exception("Logical error while creating StorageView."
			" Could not retrieve table name from select query.",
			DB::ErrorCodes::LOGICAL_ERROR);
}


BlockInputStreams StorageView::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	processed_stage = QueryProcessingStage::FetchColumns;

	ASTPtr inner_query_clone = getInnerQuery();
/*	ASTSelectQuery & inner_select = static_cast<ASTSelectQuery &>(*inner_query_clone);
	const ASTSelectQuery & outer_select = typeid_cast<const ASTSelectQuery &>(*query);

	/// Пробрасываем внутрь SAMPLE и FINAL, если они есть во внешнем запросе и их нет во внутреннем. TODO

	if (outer_select.sample_size && !inner_select.sample_size)
	{
		inner_select.sample_size = outer_select.sample_size;

		if (outer_select.sample_offset && !inner_select.sample_offset)
			inner_select.sample_offset = outer_select.sample_offset;
	}

	if (outer_select.final && !inner_select.final)
		inner_select.final = outer_select.final;*/

	return InterpreterSelectQuery(inner_query_clone, context, column_names).executeWithoutUnion();
}


void StorageView::drop()
{
	if (!select_table_name.empty())
		context.getGlobalContext().removeDependency(
			DatabaseAndTableName(select_database_name, select_table_name),
			DatabaseAndTableName(database_name, table_name));
}


}
