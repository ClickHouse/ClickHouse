#pragma once

#include <Parsers/MySQLCompatibility/IConversionTree.h>

namespace MySQLCompatibility
{
class DescribeCommandCT : public IConversionTree
{
public:
    DescribeCommandCT(MySQLPtr source) : IConversionTree(source, "describeCommand") { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    String table_name = "";
    String db_name = "";
};

void makeDescribeCHNode(const String & table_name, const String & db_name, CHPtr & ch_tree);

}
