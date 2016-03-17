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

	BlockIO execute() override
	{
		return executeImpl(false);
	}

	/** Не проверять наличие файла с метаданными и не создавать его
	  *  (для случая выполнения запроса из существующего файла с метаданными).
	  */
	void executeLoadExisting()
	{
		executeImpl(true);
	}

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

private:
	BlockIO executeImpl(bool assume_metadata_exists);

	void createDatabase(ASTCreateQuery & create);
	BlockIO createTable(ASTCreateQuery & create, bool assume_metadata_exists);

	struct ColumnsInfo
	{
		NamesAndTypesListPtr columns = new NamesAndTypesList;
		NamesAndTypesList materialized_columns;
		NamesAndTypesList alias_columns;
		ColumnDefaults column_defaults;
	};

	/// Вычислить список столбцов таблицы и вернуть его.
	ColumnsInfo setColumns(ASTCreateQuery & create, const Block & as_select_sample, const StoragePtr & as_storage) const;
	String setEngine(ASTCreateQuery & create, const StoragePtr & as_storage) const;

	/// AST в список столбцов с типами. Столбцы типа Nested развернуты в список настоящих столбцов.
	using ColumnsAndDefaults = std::pair<NamesAndTypesList, ColumnDefaults>;
	ColumnsAndDefaults parseColumns(ASTPtr expression_list) const;

	/// removes columns from the columns list and return them in a separate list
	static NamesAndTypesList removeAndReturnColumns(ColumnsAndDefaults & columns_and_defaults, ColumnDefaultType type);

	ASTPtr query_ptr;
	Context context;

	/// Используется при загрузке базы данных.
	boost::threadpool::pool * thread_pool = nullptr;
};


}
