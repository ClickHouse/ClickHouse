#include <sstream>

#include <boost/variant/static_visitor.hpp>

#include <Poco/NumberFormatter.h>

#include <mysqlxx/Manip.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Parsers/formatAST.h>


namespace DB
{


void formatAST(const IAST & ast, std::ostream & s)
{
	const ASTSelectQuery * select = dynamic_cast<const ASTSelectQuery *>(&ast);
	if (select)
	{
		formatAST(*select, s);
		return;
	}

	const ASTCreateQuery * create = dynamic_cast<const ASTCreateQuery *>(&ast);
	if (create)
	{
		formatAST(*create, s);
		return;
	}
	
	const ASTExpressionList * exp_list = dynamic_cast<const ASTExpressionList *>(&ast);
	if (exp_list)
	{
		formatAST(*exp_list, s);
		return;
	}

	const ASTFunction * func = dynamic_cast<const ASTFunction *>(&ast);
	if (func)
	{
		formatAST(*func, s);
		return;
	}

	const ASTIdentifier * id = dynamic_cast<const ASTIdentifier *>(&ast);
	if (id)
	{
		formatAST(*id, s);
		return;
	}

	const ASTLiteral * lit = dynamic_cast<const ASTLiteral *>(&ast);
	if (lit)
	{
		formatAST(*lit, s);
		return;
	}

	const ASTNameTypePair * ntp = dynamic_cast<const ASTNameTypePair *>(&ast);
	if (ntp)
	{
		formatAST(*ntp, s);
		return;
	}

	throw DB::Exception("Unknown element in AST", ErrorCodes::UNKNOWN_ELEMENT_IN_AST);
}

void formatAST(const ASTSelectQuery 		& ast, std::ostream & s)
{
	s << "SELECT ";
	formatAST(*ast.select, s);
}

void formatAST(const ASTCreateQuery 		& ast, std::ostream & s)
{
	s << (ast.attach ? "ATTACH TABLE " : "CREATE TABLE ") << ast.name << " (";
	formatAST(*ast.columns, s);
	s << ") ENGINE = ";
	formatAST(*ast.storage, s);
}

void formatAST(const ASTExpressionList 		& ast, std::ostream & s)
{
	for (ASTs::const_iterator it = ast.children.begin(); it != ast.children.end(); ++it)
	{
		if (it != ast.children.begin())
			s << ", ";
		formatAST(**it, s);
	}
}

void formatAST(const ASTFunction 			& ast, std::ostream & s)
{
	s << ast.name;
	if (ast.arguments)
	{
		s << '(';
		formatAST(*ast.arguments, s);
		s << ')';
	}
}

void formatAST(const ASTIdentifier 			& ast, std::ostream & s)
{
	s << ast.name;
}

void formatAST(const ASTLiteral 			& ast, std::ostream & s)
{
	s << boost::apply_visitor(FieldVisitorToString(), ast.value);
}

void formatAST(const ASTNameTypePair		& ast, std::ostream & s)
{
	s << ast.name << " ";
	formatAST(*ast.type, s);
}

}

