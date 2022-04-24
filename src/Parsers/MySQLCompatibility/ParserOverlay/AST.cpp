#include "Internals.h"

#include "AST.h"

#include <sstream>

namespace MySQLParserOverlay
{

static bool buildFromANTLR(const MySQLParser & parser, const antlr4::RuleContext * antlr_tree, ASTPtr node, std::string & error)
{
	auto rule_index = antlr_tree->getRuleIndex();
	auto rule_name_name = parser.getRuleNames()[rule_index];
	node->rule_name = rule_name_name;
	for (const auto & x : antlr_tree->children)
	{
		if (antlrcpp::is<antlr4::tree::ErrorNode *>(x))
		{
			error = dynamic_cast<antlr4::tree::ErrorNode *>(x)->toString();
			return false;
		}

		antlr4::tree::TerminalNode * antlr_terminal;
		if ((antlr_terminal = dynamic_cast<antlr4::tree::TerminalNode *>(x)) != nullptr)
		{
			if (antlr_terminal->getSymbol() != nullptr)
				node->terminals.push_back(antlr_terminal->getSymbol()->getText());
		}
	}
	
	for (const auto & x : antlr_tree->children)
	{
		antlr4::RuleContext * antlr_child;
		if ((antlr_child = dynamic_cast<antlr4::RuleContext *>(x)) != nullptr)
		{
			ASTPtr ast_child = std::make_shared<AST>();
			node->children.push_back(ast_child);
			if (!buildFromANTLR(parser, antlr_child, ast_child, error))
				return false;
		}
	}

	return true;
}

void AST::FromQuery(const std::string & query, ASTPtr & result, std::string & error)
{
	antlr4::ANTLRInputStream input(query);

	MySQLLexer lexer(&input);
	antlr4::CommonTokenStream tokens(&lexer);
	tokens.fill();

	MySQLParser parser(&tokens);
	auto antlr_tree = dynamic_cast<antlr4::RuleContext *>(parser.query());
	
	if (!buildFromANTLR(parser, antlr_tree, result, error))
		result = nullptr;
}

std::string AST::PrintTree() const
{
	std::stringstream ss;
	PrintTree(ss);
	return ss.str();
}

void AST::PrintTree(std::stringstream & ss) const
{
	ss << "(";
	ss << rule_name << " ";
	if (!terminals.empty())
	{
		ss << "terminals = [";
		for (const auto & x : terminals)
		{
			ss << x << " ";
		}
		ss << "]; ";
	}
	if (!children.empty())
	{
		ss << "children = ";
		for (const auto & x : children)
		{
			x->PrintTree(ss);
		}
	}
	ss << ")";
}
}
