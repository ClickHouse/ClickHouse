#include <sstream>

#include <boost/variant/static_visitor.hpp>

#include <Poco/NumberFormatter.h>

#include <strconvert/escape_manip.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Parsers/formatAST.h>


namespace DB
{

/** Выводит текстовое представление типа, как литерала в SQL запросе */
class FieldVisitorToString : public boost::static_visitor<String>
{
public:
	String operator() (const Null 		& x) const { return "NULL"; }
	String operator() (const UInt64 	& x) const { return Poco::NumberFormatter::format(x); }
	String operator() (const Int64 		& x) const { return Poco::NumberFormatter::format(x); }
	String operator() (const Float64 	& x) const { return Poco::NumberFormatter::format(x); }

	String operator() (const String 	& x) const
	{
		std::stringstream s;
		s << strconvert::quote_fast << x;
		return s.str();
	}

	String operator() (const Array 		& x) const
	{
		std::stringstream s;
		FieldVisitorToString visitor;
		
		s << "[";
		for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
		{
			if (it != x.begin())
				s << ", ";
			s << boost::apply_visitor(FieldVisitorToString(), *it);
		}
		s << "]";

		return s.str();
	}
};


void formatAST(const IAST & ast, std::ostream & s)
{
	const ASTSelectQuery * select = dynamic_cast<const ASTSelectQuery *>(&ast);
	if (select)
	{
		formatAST(*select, s);
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

	throw DB::Exception("Unknown element in AST", ErrorCodes::UNKNOWN_ELEMENT_IN_AST);
}

void formatAST(const ASTSelectQuery 		& ast, std::ostream & s)
{
	s << "SELECT ";
	formatAST(*ast.select, s);
}

void formatAST(const ASTExpressionList 	& ast, std::ostream & s)
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
	s << ast.name << '(';
	formatAST(*ast.arguments, s);
	s << ')';
}

void formatAST(const ASTIdentifier 		& ast, std::ostream & s)
{
	s << ast.name;
}

void formatAST(const ASTLiteral 			& ast, std::ostream & s)
{
	s << boost::apply_visitor(FieldVisitorToString(), ast.value);
}

}

