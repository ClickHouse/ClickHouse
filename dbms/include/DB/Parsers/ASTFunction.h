#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Functions/IFunction.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/IO/WriteBufferFromString.h>


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
	/// оригинальное значение getColumnName() для функции, которую переписали этой функцией по rewrite правилу
	String original_column_name;

	/// сама функция
	FunctionPtr function;
	/// или агрегатная функция
	AggregateFunctionPtr aggregate_function;
	/// или лямбда-выражение (ColumnExpression)
	ColumnPtr column_expression;
	/// тип возвращаемого значения
	DataTypePtr return_type;

	ASTFunction() {}
	ASTFunction(StringRange range_) : IAST(range_) {}

	String getColumnName() const
	{
		String res;
		WriteBufferFromString wb(res);
		writeString(name, wb);

		if (parameters)
		{
			writeChar('(', wb);
			for (ASTs::const_iterator it = parameters->children.begin(); it != parameters->children.end(); ++it)
			{
				if (it != parameters->children.begin())
					writeString(", ", wb);
				writeString((*it)->getColumnName(), wb);
			}
			writeChar(')', wb);
		}

		writeChar('(', wb);
		for (ASTs::const_iterator it = arguments->children.begin(); it != arguments->children.end(); ++it)
		{
			if (it != arguments->children.begin())
				writeString(", ", wb);
			writeString((*it)->getColumnName(), wb);
		}
		writeChar(')', wb);

		return res;
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
