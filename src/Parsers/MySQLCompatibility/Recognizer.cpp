#include <Parsers/MySQLCompatibility/Recognizer.h>

namespace MySQLCompatibility
{

using MySQLParserOverlay::ASTNode;
using MySQLParserOverlay::ASTNodePtr;

RecognizeResult SetQueryRecognizer::Recognize(ASTNodePtr node) const
{
	if (node->rule_type == "setStatement")
		return {node, SET_QUERY_TYPE};
	return {nullptr, UNKNOWN_QUERY_TYPE};
}

RecognizeResult GenericRecognizer::Recognize(ASTNodePtr node) const
{
	RecognizeResult result = {nullptr, UNKNOWN_QUERY_TYPE};
	for (const auto & rule : rules)
	{
		if ((result = rule->Recognize(node)).first != nullptr)
			return result;
	}

	for (auto child : node->children)
	{
		if ((result = GenericRecognizer::Recognize(child)).first != nullptr)
			return result;
	}

	return result;
}
}
