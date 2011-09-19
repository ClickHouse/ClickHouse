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
	/// типы возвращаемых значений
	DataTypes return_types;
	/// номера столбцов возвращаемых значений
	ColumnNumbers return_column_numbers;

	ASTFunction() {}
	ASTFunction(StringRange range_) : IAST(range_) {}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "Function_" + name; }
};

}
