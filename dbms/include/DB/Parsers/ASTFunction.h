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

	String getColumnName()
	{
		std::stringstream s;
		s << name << "(";
		for (ASTs::iterator it = arguments->children.begin(); it != arguments->children.end(); ++it)
		{
			if (it != arguments->children.begin())
				s << ", ";
			s << (*it)->getColumnName();
		}
		s << ")";
		return s.str();
	}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "Function_" + name; }
};

}
