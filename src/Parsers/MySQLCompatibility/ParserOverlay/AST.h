#pragma once

#include <vector>

namespace MySQLParserOverlay
{
class AST; // fwd
using ASTPtr = std::shared_ptr<AST>;

class AST
{
public:
	AST() {}
	AST(const std::string & rule_name_) : rule_name(rule_name_) {}
	static void FromQuery(const std::string & query, ASTPtr & result, std::string & error);
public:
	std::string PrintTree() const;
private:
	void PrintTree(std::stringstream & ss) const;
public:	
	std::string rule_name;
	std::vector<ASTPtr> children;
	std::vector<std::string> terminals;
};

}
