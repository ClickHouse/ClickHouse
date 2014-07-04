#pragma once

#include <DB/Parsers/ASTWithAlias.h>
#include <DB/Functions/IFunction.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/IO/WriteBufferFromString.h>


namespace DB
{

/** Применение функции или оператора
  */
class ASTFunction : public ASTWithAlias
{
public:
	enum FunctionKind
	{
		UNKNOWN,
		TABLE_FUNCTION,
		FUNCTION,
		AGGREGATE_FUNCTION,
		LAMBDA_EXPRESSION,
		ARRAY_JOIN,
	};

	/// имя функции
	String name;
	/// аргументы
	ASTPtr arguments;
	/// параметры - для параметрических агрегатных функций. Пример: quantile(0.9)(x) - то, что в первых скобках - параметры.
	ASTPtr parameters;

	FunctionKind kind;

	ASTFunction() : kind(UNKNOWN) {}
	ASTFunction(StringRange range_) : ASTWithAlias(range_), kind(UNKNOWN) {}

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
					writeCString(", ", wb);
				writeString((*it)->getColumnName(), wb);
			}
			writeChar(')', wb);
		}

		writeChar('(', wb);
		for (ASTs::const_iterator it = arguments->children.begin(); it != arguments->children.end(); ++it)
		{
			if (it != arguments->children.begin())
				writeCString(", ", wb);
			writeString((*it)->getColumnName(), wb);
		}
		writeChar(')', wb);

		return res;
	}

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
