#pragma once

#include <Parsers/MySQLCompatibility/types.h>

namespace MySQLCompatibility
{

class IConversionTree
{
public:
    IConversionTree(MySQLPtr source, const String & rule_name = "") : _source(source)
    {
        // extra check, that conversion node is built from correct source node
        // TODO: maybe exception?
        assert(source != nullptr);
        assert(rule_name.empty() || source->rule_name == rule_name);
        (void)rule_name;
    }
    virtual bool setup(String & error) = 0;
    virtual void convert(CHPtr & ch_tree) const = 0;
    virtual ~IConversionTree() { }

protected:
    MySQLPtr getSourceNode() const { return _source; }

private:
    MySQLPtr _source;
};
}
