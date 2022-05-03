#include <Parsers/MySQLCompatibility/TreePath.h>

namespace MySQLCompatibility
{

using MySQLParserOverlay::ASTPtr;

static MySQLPtr extractNodeByName(MySQLPtr node, const String & rule_name, bool strict)
{
	if (node == nullptr)
		return nullptr;
	
	if (node->rule_name == rule_name)
		return node;
	
	MySQLPtr result = nullptr;
	for (const auto & child : node->children)
	{
		if (!strict)
		{
			if ((result = extractNodeByName(child, rule_name, strict)) != nullptr)
				return result;
		} else
		{
			if (child->rule_name == rule_name)
				return child;
		}
	}
	return nullptr;
}

MySQLPtr TreePath::evaluate(MySQLPtr node, bool strict) const
{
	for (const auto & name : node_names)
		node = extractNodeByName(node, name, strict);
	
	return node;
}

}
