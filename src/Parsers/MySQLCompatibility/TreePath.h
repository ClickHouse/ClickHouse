#pragma once

#include <Parsers/MySQLCompatibility/types.h>

namespace MySQLCompatibility
{
class TreePath
{
public:
    TreePath() { }
    TreePath(const std::vector<String> & names) : node_names(names) { }
    TreePath append(const TreePath & rhs) const
    {
        TreePath result = (*this);
        for (const auto & val : rhs.node_names)
            result.node_names.push_back(val);

        return result;
    }
    static TreePath columnPath()
    {
        auto path = TreePath({"expr", "boolPri", "predicate", "bitExpr", "simpleExpr", "columnRef", "fieldIdentifier", "pureIdentifier"});

        return path;
    }

public:
    MySQLPtr find(MySQLPtr node) const { return evaluate(node, false); }
    MySQLPtr descend(MySQLPtr node) const { return evaluate(node, true); }

private:
    MySQLPtr evaluate(MySQLPtr node, bool strict) const;

private:
    std::vector<String> node_names;
};
}
