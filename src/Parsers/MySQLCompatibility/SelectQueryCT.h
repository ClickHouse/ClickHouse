#pragma once

#include <Parsers/MySQLCompatibility/IConversionTree.h>

namespace MySQLCompatibility
{

class SelectItemsListCT : public IConversionTree
{
public:
    SelectItemsListCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup() override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    bool has_asterisk = false;
    std::vector<ConvPtr> exprs;
};

class SelectOrderByCT : public IConversionTree
{
public:
    SelectOrderByCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup() override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    std::vector<std::pair<ConvPtr, MySQLTree::TOKEN_TYPE>> args;
};

class SelectLimitOffsetCT : public IConversionTree
{
public:
    SelectLimitOffsetCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup() override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    int offset;
};

class SelectLimitLengthCT : public IConversionTree
{
public:
    SelectLimitLengthCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup() override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    int length;
};

class SelectTablesCT : public IConversionTree
{
public:
    struct TableAndDB
    {
        String table = "";
        String database = "";
    };
    SelectTablesCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup() override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    std::vector<TableAndDB> tables;
};

class SelectGroupByCT : public IConversionTree
{
public:
    SelectGroupByCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup() override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    std::vector<ConvPtr> args;
};

class SelectQueryCT : public IConversionTree
{
public:
    SelectQueryCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup() override;
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

}
