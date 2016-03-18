#pragma once

#include <threadpool.hpp>

#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/IInterpreter.h>
#include <DB/Storages/ColumnDefault.h>


namespace DB
{

class ASTCreateQuery;


/** Позволяет создать новую таблицу, или создать объект уже существующей таблицы, или создать БД, или создать объект уже существующей БД.
  */
class InterpreterCreateQuery : public IInterpreter
{
public:
	InterpreterCreateQuery(ASTPtr query_ptr_, Context & context_);

	BlockIO execute() override;

	/// Список столбцов с типами в AST.
	static ASTPtr formatColumns(const NamesAndTypesList & columns);
	static ASTPtr formatColumns(
		NamesAndTypesList columns,
		const NamesAndTypesList & materialized_columns,
		const NamesAndTypesList & alias_columns,
		const ColumnDefaults & column_defaults);

	void setDatabaseLoadingThreadpool(boost::threadpool::pool & thread_pool_)
	{
		thread_pool = &thread_pool_;
	}

	struct ColumnsInfo
	{
		NamesAndTypesListPtr columns = new NamesAndTypesList;
		NamesAndTypesList materialized_columns;
		NamesAndTypesList alias_columns;
		ColumnDefaults column_defaults;
	};

	/// Получить информацию о столбцах и типах их default-ов, для случая, когда столбцы в запросе create указаны явно.
	static ColumnsInfo getColumnsInfo(const ASTPtr & columns, const Context & context);

private:
	void createDatabase(ASTCreateQuery & create);
	BlockIO createTable(ASTCreateQuery & create);

	/// Вычислить список столбцов таблицы и вернуть его.
	ColumnsInfo setColumns(ASTCreateQuery & create, const Block & as_select_sample, const StoragePtr & as_storage) const;
	String setEngine(ASTCreateQuery & create, const StoragePtr & as_storage) const;

	ASTPtr query_ptr;
	Context context;

	/// Используется при загрузке базы данных.
	boost::threadpool::pool * thread_pool = nullptr;
};


}
