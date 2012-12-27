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
	/// параметры - для параметрических агрегатных функций. Пример: quantile(0.9)(x) - то, что в первых скобках - параметры.
	ASTPtr parameters;
	/// алиас, если есть
	String alias;

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

	String getColumnName() const
	{
		std::stringstream s;
		s << name;

		if (parameters)
		{
			s << "(";
			for (ASTs::const_iterator it = parameters->children.begin(); it != parameters->children.end(); ++it)
			{
				if (it != parameters->children.begin())
					s << ", ";
				s << (*it)->getColumnName();
			}
			s << ")";
		}

		s << "(";
		for (ASTs::const_iterator it = arguments->children.begin(); it != arguments->children.end(); ++it)
		{
			if (it != arguments->children.begin())
				s << ", ";
			s << (*it)->getColumnName();
		}
		s << ")";
		
		return s.str();
	}

	String getAlias() const { return alias.empty() ? getColumnName() : alias; }

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const { return "Function_" + name; }

	ASTPtr clone() const
	{
		ASTFunction * res = new ASTFunction(*this);
		res->children.clear();

		if (arguments) 	{ res->arguments = arguments->clone();		res->children.push_back(res->arguments); }
		if (parameters) { res->parameters = parameters->clone(); 	res->children.push_back(res->parameters); }

		return res;
	}
};

}
