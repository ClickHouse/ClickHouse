#pragma once

#include <vector>

namespace MySQLParserOverlay
{
class ASTNode; // fwd
using ASTNodePtr = std::shared_ptr<ASTNode>;

class ASTNode
{
public:
	ASTNode() {}
	ASTNode(const std::string & rule_type_) : rule_type(rule_type_) {}
	static void FromQuery(const std::string & query, ASTNodePtr & result, std::string & error);
public:
	std::string PrintTree() const;
private:
	void PrintTree(std::stringstream & ss) const;
public:	
	std::string rule_type;
	std::vector<ASTNodePtr > children;
	std::vector<std::string> terminals;
};

}
