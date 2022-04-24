#include <Parsers/MySQLCompatibility/AST_fwd.h>

#include <Parsers/MySQLCompatibility/Conversion.h>

#include <Parsers/MySQLCompatibility/ParserOverlay/Printer.h>

#include <Parsers/MySQLCompatibility/Recognizer.h>
#include <Parsers/MySQLCompatibility/TreePath.h>
#include <Parsers/MySQLCompatibility/ConversionTree.h>

#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <Common/SettingsChanges.h>

namespace MySQLCompatibility
{

String Converter::dumpAST(const String & query)
{
	MySQLPtr root = std::make_shared<MySQLTree>();
	std::string error;
	MySQLTree::FromQuery(query, root, error);
		
	if (root != nullptr)
		return root->PrintTree();
	else
		return "MySQL query is invalid, TODO: listen antlr errors";
}

void Converter::toClickHouseAST(const String & query, CHPtr & ch_tree)
{
	ch_tree = nullptr;
	MySQLPtr root = std::make_shared<MySQLTree>();
	
	// TODO: report meaningful errors
	std::string error;
	MySQLTree::FromQuery(query, root, error);

	if (root == nullptr)
		return;

	GenericRecognizer recognizer;
	auto result = recognizer.Recognize(root);
	if (result != nullptr && result->setup())
		result->convert(ch_tree);
}

}
