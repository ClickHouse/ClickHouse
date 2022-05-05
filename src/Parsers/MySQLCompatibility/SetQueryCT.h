#pragma once

#include <Parsers/MySQLCompatibility/IConversionTree.h>

namespace MySQLCompatibility
{
class SetQueryCT : public IConversionTree
{
public:
    SetQueryCT(MySQLPtr source) : IConversionTree(source) { }
    virtual bool setup(String & error) override;
    virtual void convert(CHPtr & ch_tree) const override;

private:
    std::vector<std::pair<String, String>> _key_value_list;
};
}
