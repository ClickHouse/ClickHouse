#pragma once

#include <Parsers/MySQLCompatibility/IConversionTree.h>

namespace MySQLCompatibility
{

class ExprGenericLiteralCT : public IConversionTree
{
public:
    ExprGenericLiteralCT(MySQLPtr source) : IConversionTree(source, "literal") { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    DB::Field value;
};

class ExprIdentifierCT : public IConversionTree
{
public:
    ExprIdentifierCT(MySQLPtr source) : IConversionTree(source, "identifier") { }
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
    String varname;
};

class ExprSumCT : public IConversionTree
{
public:
    ExprSumCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    MySQLTree::TOKEN_TYPE function_type;
    ConvPtr expr_ct = nullptr;
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
