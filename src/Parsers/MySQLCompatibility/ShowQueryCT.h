#pragma once

#include <Parsers/MySQLCompatibility/IConversionTree.h>

namespace MySQLCompatibility
{

class ShowColumnsCT : public IConversionTree
{
public:
    ShowColumnsCT(MySQLPtr source) : IConversionTree(source, "showStatement") { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    String table_name = "";
    String db_name = "";
};

class ShowTablesCT : public IConversionTree
{
public:
    ShowTablesCT(MySQLPtr source) : IConversionTree(source, "showStatement") { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    String from = "";
    String like = "";
};

class ShowQueryCT : public IConversionTree
{
public:
    ShowQueryCT(MySQLPtr source) : IConversionTree(source, "showStatement") { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    ConvPtr specific_show_ct = nullptr;
};
}
