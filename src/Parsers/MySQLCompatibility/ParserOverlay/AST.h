#include "ASTNode.h"

namespace MySQLParserOverlay
{
class AST
{
public:
	static AST FromQuery(const std::string & query);
public:
	std::string PrintTree() const { return root_->PrintTree(); }
	bool Exists() const { return root_ != nullptr; }
	const std::string & GetError() const { return error_; }
private:
	AST() {}
private:
	ASTNodePtr root_;
	std::string error_;
};
}
