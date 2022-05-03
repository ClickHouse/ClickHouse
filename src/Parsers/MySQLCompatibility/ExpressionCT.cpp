#include <Parsers/MySQLCompatibility/ExpressionCT.h>
#include <Parsers/MySQLCompatibility/TreePath.h>

#include <Parsers/ASTIdentifier.h>

namespace MySQLCompatibility
{

bool ExprIdentifierCT::setup()
{
	MySQLPtr identifier_node = TreePath({
			"pureIdentifier"	
		}).evaluate(_source);
	
	if (identifier_node == nullptr)
		return false;
	
	assert(!identifier_node->terminals.empty());
	value = identifier_node->terminals[0];

	return true;
}

void ExprIdentifierCT::convert(CHPtr & ch_tree) const
{
	auto identifier = std::make_shared<DB::ASTIdentifier>(value);
	ch_tree = identifier;
}

bool BitExprCT::setup()
{
	MySQLPtr bitexpr_node = _source;
	if (bitexpr_node == nullptr)
		return false;
	
	MySQLPtr simple_expr_node = TreePath({
			"simpleExpr"
		}).evaluate(bitexpr_node, true);
	
	if (simple_expr_node != nullptr)
	{
		MySQLPtr column_node = TreePath({
				"columnRef"
			}).evaluate(simple_expr_node);
		if (column_node != nullptr)
		{
			simple_expr = std::make_shared<ExprIdentifierCT>(column_node);
			if (!simple_expr->setup())
			{
				simple_expr = nullptr;
				return false;
			}
			return true;
		}
	}
	return false; // FIXME
}

void BitExprCT::convert(CHPtr & ch_tree) const
{
	if (simple_expr != nullptr)
		simple_expr->convert(ch_tree);
}

bool ExpressionCT::setup()
{
	MySQLPtr expr = _source;
	if (expr == nullptr)
		return false;
	
	MySQLPtr bitexpr_node = TreePath({
			"boolPri",
			"predicate",
			"bitExpr",
		}).evaluate(expr, true);
	
	if (bitexpr_node == nullptr)
		return false;
	
	bit_expr = std::make_shared<BitExprCT>(bitexpr_node);
	if (!bit_expr->setup())
	{
		bit_expr = nullptr;
		return false;
	}
		
	return true;
}
void ExpressionCT::convert(CHPtr & ch_tree) const
{
	if (bit_expr != nullptr)
		bit_expr->convert(ch_tree);
}
}
