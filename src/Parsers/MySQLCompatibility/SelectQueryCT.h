#pragma once

#include <Parsers/MySQLCompatibility/IConversionTree.h>

namespace MySQLCompatibility
{

class SelectItemsListCT : public IConversionTree
{
    struct SelectItem
    {
        ConvPtr expr;
        String alias = "";
    };

public:
    SelectItemsListCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    bool has_asterisk = false;
    std::vector<SelectItem> exprs;
};

class SelectOrderByCT : public IConversionTree
{
    struct OrderByArg
    {
        ConvPtr expr;
        MySQLTree::TOKEN_TYPE direction;
    };

public:
    SelectOrderByCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    std::vector<OrderByArg> args;
};

class SelectLimitOffsetCT : public IConversionTree
{
public:
    SelectLimitOffsetCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    int offset;
};

class SelectLimitLengthCT : public IConversionTree
{
public:
    SelectLimitLengthCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    int length;
};

class SelectTableCT : public IConversionTree
{
public:
    SelectTableCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;
private:
    String table;
    String database;
};

class SelectSubqueryCT : public IConversionTree
{
public:
    SelectSubqueryCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;
private:
    ConvPtr subquery_ct;
};

class SelectFromCT : public IConversionTree
{
public:
    SelectFromCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    std::vector<ConvPtr> items;
};

class SelectGroupByCT : public IConversionTree
{
public:
    SelectGroupByCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    std::vector<ConvPtr> args;
};

class SelectQueryExprCT : public IConversionTree
{
public:
    SelectQueryExprCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    ConvPtr select_items_ct = nullptr;
    ConvPtr order_by_ct = nullptr;
    ConvPtr limit_length_ct = nullptr;
    ConvPtr limit_offset_ct = nullptr;
    ConvPtr tables_ct = nullptr;
    ConvPtr where_ct = nullptr;
    ConvPtr group_by_ct = nullptr;
    ConvPtr having_ct = nullptr;
};

class SelectQueryCT : public IConversionTree
{
public:
    SelectQueryCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;
private:
    ConvPtr expr_ct;
};

}
