#pragma once

#include <DB/Parsers/ASTWithAlias.h>
#include <DB/Parsers/ASTExpressionList.h>
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

	FunctionKind kind{UNKNOWN};

	enum class Genus
	{
		ORDINARY = 0,
		CASE_WITH_EXPR,
		CASE_WITHOUT_EXPR,
		CASE_ARRAY
	};

	Genus genus{Genus::ORDINARY};

public:
	ASTFunction() = default;
	ASTFunction(const StringRange range_) : ASTWithAlias(range_) {}

	String getColumnName() const override;

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override;

	ASTPtr clone() const override;

protected:
	void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

private:
	void formatCase(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;
};


template <typename... Args>
ASTPtr makeASTFunction(const String & name, Args &&... args)
{
	const auto function = std::make_shared<ASTFunction>();
	ASTPtr result{function};

	function->name = name;
	function->arguments = std::make_shared<ASTExpressionList>();
	function->children.push_back(function->arguments);

	function->arguments->children = { std::forward<Args>(args)... };

	return result;
}


template <typename... Args>
ASTPtr makeASTFunction(const String & name, const StringRange & function_range,
	const StringRange & arguments_range, Args &&... args)
{
	const auto function = std::make_shared<ASTFunction>(function_range);
	ASTPtr result{function};

	function->name = name;
	function->arguments = std::make_shared<ASTExpressionList>(arguments_range);
	function->children.push_back(function->arguments);

	function->arguments->children = { std::forward<Args>(args)... };

	return result;
}

}
