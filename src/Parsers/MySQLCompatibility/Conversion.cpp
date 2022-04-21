#include <Parsers/MySQLCompatibility/Conversion.h>

#include <Parsers/MySQLCompatibility/ParserOverlay/Printer.h>
#include <Parsers/MySQLCompatibility/ParserOverlay/ASTNode.h>

#include <Parsers/MySQLCompatibility/Recognizer.h>

#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <Common/SettingsChanges.h>

namespace MySQLCompatibility
{

using MySQLParserOverlay::ASTNode;
using MySQLParserOverlay::ASTNodePtr;

static ASTNodePtr extractRuleByName(const ASTNodePtr & node, std::string required_name)
{
	if (node->rule_type == required_name)
		return node;
	
	ASTNodePtr result = nullptr;
	for (const auto & child : node->children)
	{
		if ((result = extractRuleByName(child, required_name)) != nullptr)
			return result;
	}

	return result;
}

// MOO TODO: right now simple SET key=value; improve this
static DB::ASTPtr convertSetQuery(ASTNodePtr mysql_node)
{
	// MOO checks!!!
	ASTNodePtr keyNode = extractRuleByName(mysql_node, "internalVariableName");
	ASTNodePtr keyIdentifier = extractRuleByName(keyNode, "pureIdentifier");
	std::string key = keyIdentifier->terminals[0];
	
	ASTNodePtr valueNode = extractRuleByName(mysql_node, "setExprOrDefault");
	ASTNodePtr valueIdentifier = extractRuleByName(valueNode, "textStringLiteral");
	std::string value = valueIdentifier->terminals[0];

	auto query = std::make_shared<DB::ASTSetQuery>();
	
	DB::SettingsChanges changes;
	changes.push_back(DB::SettingChange{});
	
	DB::ASTPtr chKey = std::make_shared<DB::ASTIdentifier>(key);
	DB::tryGetIdentifierNameInto(chKey, changes.back().name);
	changes.back().value = value;

	query->is_standalone = false;
	query->changes = std::move(changes);

	return query;
}

String Converter::dumpAST(const String & query)
{
	ASTNodePtr root = std::make_shared<ASTNode>();
	std::string error;
	MySQLParserOverlay::ASTNode::FromQuery(query, root, error);
		
	if (root != nullptr)
		return root->PrintTree();
	else
		return "MySQL query is invalid, TODO: listen antlr errors";
}

DB::ASTPtr Converter::toClickHouseAST(const String & query)
{
	ASTNodePtr root = std::make_shared<ASTNode>();
	std::string error;
	MySQLParserOverlay::ASTNode::FromQuery(query, root, error);

	GenericRecognizer recognizer;
	auto result = recognizer.Recognize(root);
	switch (result.second)
	{
		case SET_QUERY_TYPE:
			return convertSetQuery(result.first);
		case UNKNOWN_QUERY_TYPE:
			return nullptr;
	}
	return nullptr;
}

}
