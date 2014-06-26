#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTDropQuery.h>

#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Interpreters/InterpreterDropQuery.h>

#include <DB/Storages/StorageMaterializedView.h>


namespace DB
{

StoragePtr StorageMaterializedView::create(const String & table_name_, const String & database_name_,
	Context & context_, ASTPtr & query_, NamesAndTypesListPtr columns_, bool attach_)
{
	return (new StorageMaterializedView(table_name_, database_name_, context_, query_, columns_, attach_))->thisPtr();
}

StorageMaterializedView::StorageMaterializedView(const String & table_name_, const String & database_name_,
	Context & context_, ASTPtr & query_, NamesAndTypesListPtr columns_,	bool attach_):
	StorageView(table_name_, database_name_, context_, query_, columns_)
{
	ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_);

	/// Если запрос ATTACH, то к этому моменту внутренняя таблица уже должна быть подключена.
	if (attach_)
	{
		if (!context.isTableExist(database_name, getInnerTableName()))
			throw Exception("Inner table is not attached yet."
				" Materialized view: " + database_name + "." + table_name + "."
				" Inner table: " + database_name + "." + getInnerTableName() + ".",
				DB::ErrorCodes::LOGICAL_ERROR);
		data = context.getTable(database_name, getInnerTableName());
	}
	else
	{
		/// Составим запрос для создания внутреннего хранилища.
		ASTCreateQuery * manual_create_query = new ASTCreateQuery();
		manual_create_query->database = database_name;
		manual_create_query->table = getInnerTableName();
		manual_create_query->columns = create.columns;
		ASTPtr ast_create_query = manual_create_query;

		/// Если не указан в запросе тип хранилища попробовать извлечь его из запроса Select.
		if (!create.inner_storage)
		{
			/// TODO так же попытаться извлечь params для создания хранилища
			ASTFunction * func = new ASTFunction();
			func->name = context.getTable(select_database_name, select_table_name)->getName();
			manual_create_query->storage = func;
		}
		else
			manual_create_query->storage = create.inner_storage;

		/// Выполним запрос.
		InterpreterCreateQuery create_interpreter(ast_create_query, context);
		data = create_interpreter.execute();
	}
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

void StorageMaterializedView::drop()
{
	context.getGlobalContext().removeDependency(DatabaseAndTableName(select_database_name, select_table_name), 	DatabaseAndTableName(database_name, table_name));

	if (context.tryGetTable(database_name, getInnerTableName()))
	{
		/// Состваляем и выполняем запрос drop для внутреннего хранилища.
		ASTDropQuery *drop_query = new ASTDropQuery;
		drop_query->database = database_name;
		drop_query->table = getInnerTableName();
		ASTPtr ast_drop_query = drop_query;
		InterpreterDropQuery drop_interpreter(ast_drop_query, context);
		drop_interpreter.execute();
	}
}

bool StorageMaterializedView::optimize() {
	return data->optimize();
}


}
