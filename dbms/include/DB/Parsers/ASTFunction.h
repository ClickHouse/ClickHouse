#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Functions/IFunction.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{

/** Применение функции или оператора
  */
class ASTFunction : public IAST
{
public:
	/// имя функции
	String name;
	/// аргументы
	ASTPtr arguments;

	/// сама функция
	FunctionPtr function;
	/// или агрегатная функция
	AggregateFunctionPtr aggregate_function;
	/// тип возвращаемого значения
	DataTypePtr return_type;
	/// номер столбца возвращаемого значения
	size_t return_column_number;

	ASTFunction() {}
	ASTFunction(StringRange range_) : IAST(range_) {}

	String getColumnName() { return getTreeID(); }

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "Function_" + name; }
};

}
