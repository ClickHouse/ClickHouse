#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTDropQuery.h>

#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <DB/Interpreters/InterpreterDropQuery.h>

#include <DB/Storages/StorageMaterializedView.h>
#include <DB/Storages/VirtualColumnFactory.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}


StoragePtr StorageMaterializedView::create(
	const String & table_name_,
	const String & database_name_,
	Context & context_,
	ASTPtr & query_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	bool attach_)
{
	return (new StorageMaterializedView{
		table_name_, database_name_, context_, query_,
		columns_, materialized_columns_, alias_columns_, column_defaults_,
		attach_
	})->thisPtr();
}

StorageMaterializedView::StorageMaterializedView(
	const String & table_name_,
	const String & database_name_,
	Context & context_,
	ASTPtr & query_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	bool attach_)
	: StorageView{table_name_, database_name_, context_, query_, columns_, materialized_columns_, alias_columns_, column_defaults_}
{
	ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_);

	auto inner_table_name = getInnerTableName();

	/// Если запрос ATTACH, то к этому моменту внутренняя таблица уже должна быть подключена.
	if (!attach_)
	{
		/// Составим запрос для создания внутреннего хранилища.
		ASTCreateQuery * manual_create_query = new ASTCreateQuery();
		manual_create_query->database = database_name;
		manual_create_query->table = inner_table_name;
		manual_create_query->columns = create.columns;
		manual_create_query->children.push_back(manual_create_query->columns);
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

		manual_create_query->children.push_back(manual_create_query->storage);

		/// Выполним запрос.
		InterpreterCreateQuery create_interpreter(ast_create_query, context);
		create_interpreter.execute();
	}
}

NameAndTypePair StorageMaterializedView::getColumn(const String & column_name) const
{
	auto type = VirtualColumnFactory::tryGetType(column_name);
	if (type)
		return NameAndTypePair(column_name, type);

	return getRealColumn(column_name);
}

bool StorageMaterializedView::hasColumn(const String & column_name) const
{
	return VirtualColumnFactory::hasColumn(column_name) || IStorage::hasColumn(column_name);
}

BlockInputStreams StorageMaterializedView::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	return getInnerTable()->read(column_names, query, context, settings, processed_stage, max_block_size, threads);
}

BlockOutputStreamPtr StorageMaterializedView::write(ASTPtr query, const Settings & settings)
{
	return getInnerTable()->write(query, settings);
}

void StorageMaterializedView::drop()
{
	context.getGlobalContext().removeDependency(
		DatabaseAndTableName(select_database_name, select_table_name),
		DatabaseAndTableName(database_name, table_name));

	auto inner_table_name = getInnerTableName();

	if (context.tryGetTable(database_name, inner_table_name))
	{
		/// Состваляем и выполняем запрос drop для внутреннего хранилища.
		ASTDropQuery *drop_query = new ASTDropQuery;
		drop_query->database = database_name;
		drop_query->table = inner_table_name;
		ASTPtr ast_drop_query = drop_query;
		InterpreterDropQuery drop_interpreter(ast_drop_query, context);
		drop_interpreter.execute();
	}
}

bool StorageMaterializedView::optimize(const Settings & settings)
{
	return getInnerTable()->optimize(settings);
}


}
