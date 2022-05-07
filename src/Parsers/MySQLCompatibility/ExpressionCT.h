#pragma once

#include <Parsers/MySQLCompatibility/IConversionTree.h>

namespace MySQLCompatibility
{

class ExprLiteralInt64CT : public IConversionTree
{
public:
    ExprLiteralInt64CT(MySQLPtr source) : IConversionTree(source, "numLiteral") { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    int64_t value;
};

class ExprLiteralFloat64CT : public IConversionTree
{
public:
    ExprLiteralFloat64CT(MySQLPtr source) : IConversionTree(source, "numLiteral") { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    double value;
};

class ExprLiteralNumericCT : public IConversionTree
{
public:
    ExprLiteralNumericCT(MySQLPtr source) : IConversionTree(source, "numLiteral") { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    ConvPtr numeric_ct = nullptr;
};

class ExprLiteralText : public IConversionTree
{
public:
    ExprLiteralText(MySQLPtr source) : IConversionTree(source, "textLiteral") { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    String value;
};

class ExprLiteralBoolCT : public IConversionTree
{
public:
    ExprLiteralBoolCT(MySQLPtr source) : IConversionTree(source, "boolLiteral") { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    bool value;
};

class ExprLiteralNullCT : public IConversionTree
{
public:
    ExprLiteralNullCT(MySQLPtr source) : IConversionTree(source, "nullLiteral") { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;
};

class ExprGenericLiteralCT : public IConversionTree
{
public:
    ExprGenericLiteralCT(MySQLPtr source) : IConversionTree(source, "literal") { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    ConvPtr literal_ct = nullptr;
};

class ExprIdentifierCT : public IConversionTree
{
public:
    ExprIdentifierCT(MySQLPtr source) : IConversionTree(source, "pureIdentifier") { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    String value;
};

class ExprVariableCT : public IConversionTree
{
public:
    ExprVariableCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
};

class ExprSimpleCT : public IConversionTree
{
public:
    ExprSimpleCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    ConvPtr subexpr_ct = nullptr;
    bool negate = false;
};

class ExprBitCT : public IConversionTree
{
public:
    ExprBitCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    ConvPtr simple_expr_ct = nullptr;

    MySQLTree::TOKEN_TYPE operation;
    ConvPtr first_operand_ct = nullptr;
    ConvPtr second_operand_ct = nullptr;
};

// TODO: add predicate

class ExprBoolStatementCT : public IConversionTree
{
public:
    ExprBoolStatementCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    ConvPtr predicate_ct = nullptr;
    MySQLTree::TOKEN_TYPE operation;
    ConvPtr first_operand_ct = nullptr;
    ConvPtr second_operand_ct = nullptr;
};

class ExpressionCT : public IConversionTree
{
public:
    ExpressionCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    ConvPtr subexpr_ct = nullptr;
    bool not_rule = false;

    MySQLTree::TOKEN_TYPE operation;
    ConvPtr first_operand_ct = nullptr;
    ConvPtr second_operand_ct = nullptr;
};
}
