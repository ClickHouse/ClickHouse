#pragma once

#include <Parsers/MySQLCompatibility/IConversionTree.h>

namespace MySQLCompatibility
{
class ShowTablesQueryCT : public IConversionTree
{
public:
    ShowTablesQueryCT(MySQLPtr source) : IConversionTree(source, "showStatement") { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;
};
}
