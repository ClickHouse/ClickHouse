#include "Internals.h"

#include "AST.h"
#include "ASTNode.h"

namespace MySQLParserOverlay
{
AST AST::FromQuery(const std::string & query)
{
	AST result;
	
	result.root_ = std::make_shared<ASTNode>();
	ASTNode::FromQuery(query, result.root_, result.error_);

	return result;
}
}
