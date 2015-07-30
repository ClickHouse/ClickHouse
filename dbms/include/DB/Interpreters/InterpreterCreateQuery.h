#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/IInterpreter.h>
#include <DB/Storages/ColumnDefault.h>


namespace DB
{


/** Позволяет создать новую таблицу, или создать объект уже существующей таблицы, или создать БД, или создать объект уже существующей БД
  */
class InterpreterCreateQuery : public IInterpreter
{
public:
	InterpreterCreateQuery(ASTPtr query_ptr_, Context & context_);

	/** В случае таблицы: добавляет созданную таблицу в контекст, а также возвращает её.
	  * В случае БД: добавляет созданную БД в контекст и возвращает NULL.
	  * assume_metadata_exists - не проверять наличие файла с метаданными и не создавать его
	  *  (для случая выполнения запроса из существующего файла с метаданными).
	  */
	BlockIO execute() override
	{
		return executeImpl(false);
	}

	/** assume_metadata_exists - не проверять наличие файла с метаданными и не создавать его
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

private:
	BlockIO executeImpl(bool assume_metadata_exists);

	/// AST в список столбцов с типами. Столбцы типа Nested развернуты в список настоящих столбцов.
	using ColumnsAndDefaults = std::pair<NamesAndTypesList, ColumnDefaults>;
	ColumnsAndDefaults parseColumns(ASTPtr expression_list);

	/// removes columns from the columns list and return them in a separate list
	static NamesAndTypesList removeAndReturnColumns(ColumnsAndDefaults & columns_and_defaults, ColumnDefaultType type);

	ASTPtr query_ptr;
	Context context;
};


}
