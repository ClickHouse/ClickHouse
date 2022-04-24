#pragma once

#include <Parsers/MySQLCompatibility/AST_fwd.h>

namespace MySQLCompatibility
{
struct TreePath
{
	TreePath() {}
	TreePath(const std::vector<String> & names) : node_names(names) {}
	TreePath append(const TreePath & rhs) const
	{
		TreePath result = (*this);
		for (const auto & val : rhs.node_names)
			result.node_names.push_back(val);

		return result;
	}
	
	MySQLPtr evaluate(MySQLPtr node) const;

	std::vector<String> node_names;
};
}
