#include <base/types.h>

#include <Parsers/MySQLCompatibility/TreePath.h>

namespace MySQLCompatibility
{

using MySQLParserOverlay::ASTPtr;

static MySQLPtr extractNodeByName(MySQLPtr node, const String & rule_name)
{
	if (node == nullptr)
		return nullptr;
	
	if (node->rule_name == rule_name)
		return node;
	
	MySQLPtr result = nullptr;
	for (const auto & child : node->children)
		if ((result = extractNodeByName(child, rule_name)) != nullptr)
			return result;
	
	return nullptr;
}

MySQLPtr TreePath::evaluate(MySQLPtr node) const
{
	for (const auto & name : node_names)
		node = extractNodeByName(node, name);
	
	return node;
}

}
