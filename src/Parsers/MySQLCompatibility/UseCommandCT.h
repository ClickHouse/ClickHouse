#pragma once

#include <Parsers/MySQLCompatibility/IConversionTree.h>

namespace MySQLCompatibility
{

class UseCommandCT : public IConversionTree
{
public:
    UseCommandCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup() override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    String database;
};
}
