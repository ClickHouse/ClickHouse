#pragma once

#include <DB/Parsers/ASTWithAlias.h>
#include <DB/Parsers/ASTExpressionList.h>


namespace DB
{

/** AST for function application or operator.
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

	String name;
	ASTPtr arguments;
	/// parameters - for parametric aggregate function. Example: quantile(0.9)(x) - what in first parens are 'parameters'.
	ASTPtr parameters;

	FunctionKind kind{UNKNOWN};

public:
	ASTFunction() = default;
	ASTFunction(const StringRange range_) : ASTWithAlias(range_) {}

	String getColumnName() const override;

	/** Get text identifying the AST node. */
	String getID() const override;

	ASTPtr clone() const override;

protected:
	void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
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
