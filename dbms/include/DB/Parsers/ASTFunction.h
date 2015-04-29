#pragma once

#include <DB/Parsers/ASTWithAlias.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Functions/IFunction.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/Common/SipHash.h>


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

	FunctionKind kind{UNKNOWN};

	ASTFunction() = default;
	ASTFunction(const StringRange range_) : ASTWithAlias(range_) {}

	String getColumnName() const override
	{
		SipHash hash;

		hash.update(name.data(), name.size());

		if (parameters)
		{
			hash.update("(", 1);
			for (const auto & param : parameters->children)
			{
				String param_name = param->getColumnName();		/// TODO Сделать метод updateHashWith.
				hash.update(param_name.data(), param_name.size() + 1);
			}
			hash.update(")", 1);
		}

		hash.update("(", 1);
		for (const auto & arg : arguments->children)
		{
			String arg_name = arg->getColumnName();
			hash.update(arg_name.data(), arg_name.size() + 1);
		}
		hash.update(")", 1);

		UInt64 low, high;
		hash.get128(low, high);

		return toString(high) + "_" + toString(low);			/// TODO hex.
	}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "Function_" + name; }

	ASTPtr clone() const override
	{
		ASTFunction * res = new ASTFunction(*this);
		ASTPtr ptr{res};

		res->children.clear();

		if (arguments) 	{ res->arguments = arguments->clone();		res->children.push_back(res->arguments); }
		if (parameters) { res->parameters = parameters->clone(); 	res->children.push_back(res->parameters); }

		return ptr;
	}
};


template <typename... Args>
ASTPtr makeASTFunction(const String & name, Args &&... args)
{
	const auto function = new ASTFunction{};
	ASTPtr result{function};

	function->name = name;
	function->arguments = new ASTExpressionList{};
	function->children.push_back(function->arguments);

	function->arguments->children = { std::forward<Args>(args)... };

	return result;
}


template <typename... Args>
ASTPtr makeASTFunction(const String & name, const StringRange & function_range,
	const StringRange & arguments_range, Args &&... args)
{
	const auto function = new ASTFunction{function_range};
	ASTPtr result{function};

	function->name = name;
	function->arguments = new ASTExpressionList{arguments_range};
	function->children.push_back(function->arguments);

	function->arguments->children = { std::forward<Args>(args)... };

	return result;
}

}
