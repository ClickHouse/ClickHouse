#include <Parsers/MySQLCompatibility/Recognizer.h>

namespace MySQLCompatibility
{

ConvPtr SetQueryRecognizer::Recognize(MySQLPtr node) const
{
	if (node->rule_name == "setStatement")
		return std::make_shared<SetQueryCT>(node);
	return nullptr;
}

ConvPtr SimpleSelectQueryRecognizer::Recognize(MySQLPtr node) const
{
	if (node->rule_name == "selectStatement")
		return std::make_shared<SimpleSelectQueryCT>(node);
	
	return nullptr;
}

ConvPtr GenericRecognizer::Recognize(MySQLPtr node) const
{
	ConvPtr result = nullptr;
	for (const auto & rule : rules)
	{
		if ((result = rule->Recognize(node)) != nullptr)
			return result;
	}

	for (auto child : node->children)
	{
		if ((result = this->Recognize(child)) != nullptr)
			return result;
	}

	return result;
}
}
