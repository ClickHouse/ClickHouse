#pragma once

#include <Parsers/MySQLCompatibility/IConversionTree.h>

namespace MySQLCompatibility
{
/*
class ExprLiteralInt64CT : public IConversionTree
{
public:
	ExprLiteralInt64CT(MySQLPtr source) : IConversionTree(source) {}
	virtual bool setup() override;
	virtual void convert(CHPtr & ch_tree) const override;
private:
	int64_t value;
}*/

class ExprIdentifierCT : public IConversionTree
{
public:
	ExprIdentifierCT(MySQLPtr source) : IConversionTree(source) {}
	virtual bool setup() override;
	virtual void convert(CHPtr & ch_tree) const override;
private:
	String value;
};

class BitExprCT : public IConversionTree
{
public:
	enum class OPERATION : uint8_t
	{
		PLUS,
		MINUS,
		MUL,
		DIV
	};
	BitExprCT(MySQLPtr source) : IConversionTree(source) {}
	virtual bool setup() override;
	virtual void convert(CHPtr & ch_tree) const override;
private:
	ConvPtr simple_expr = nullptr;
	
	// OPERATION operation;
	// ConvPtr first_operand = nullptr;
	// ConvPtr second_operatnd = nullptr;
};

class ExpressionCT : public IConversionTree
{
public:
	ExpressionCT(MySQLPtr source) : IConversionTree(source) {}
	virtual bool setup() override;
	virtual void convert(CHPtr & ch_tree) const override;
private:
	ConvPtr bit_expr = nullptr; // TODO: full grammar
};
}
