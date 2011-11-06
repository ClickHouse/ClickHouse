#include <sstream>

#include <boost/variant/static_visitor.hpp>

#include <Poco/NumberFormatter.h>

#include <mysqlxx/Manip.h>

#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/WriteHelpers.h>

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

	const ASTAsterisk * asterisk = dynamic_cast<const ASTAsterisk *>(&ast);
	if (asterisk)
	{
		formatAST(*asterisk, s);
		return;
	}

	const ASTOrderByElement * order_by_elem = dynamic_cast<const ASTOrderByElement *>(&ast);
	if (order_by_elem)
	{
		formatAST(*order_by_elem, s);
		return;
	}

	throw DB::Exception("Unknown element in AST", ErrorCodes::UNKNOWN_ELEMENT_IN_AST);
}

void formatAST(const ASTSelectQuery 		& ast, std::ostream & s)
{
	s << "SELECT ";
	formatAST(*ast.select_expression_list, s);

	if (ast.table)
	{
		s << " FROM ";
		if (ast.database)
		{
			formatAST(*ast.database, s);
			s << ".";
		}
		formatAST(*ast.table, s);
	}

	if (ast.where_expression)
	{
		s << " WHERE ";
		formatAST(*ast.where_expression, s);
	}

	if (ast.group_expression_list)
	{
		s << " GROUP BY ";
		formatAST(*ast.group_expression_list, s);
	}

	if (ast.having_expression)
	{
		s << " HAVING ";
		formatAST(*ast.having_expression, s);
	}

	if (ast.order_expression_list)
	{
		s << " ORDER BY ";
		formatAST(*ast.order_expression_list, s);
	}

	if (ast.limit_length)
	{
		s << " LIMIT ";
		if (ast.limit_offset)
		{
			formatAST(*ast.limit_offset, s);
			s << ", ";
		}
		formatAST(*ast.limit_length, s);
	}

	if (ast.format)
	{
		s << " FORMAT ";
		formatAST(*ast.format, s);
	}
}

void formatAST(const ASTCreateQuery 		& ast, std::ostream & s)
{
	s << (ast.attach ? "ATTACH TABLE " : "CREATE TABLE ") << (!ast.database.empty() ? ast.database + "." : "") << ast.table << " (";
	formatAST(*ast.columns, s);
	s << ") ENGINE = ";
	formatAST(*ast.storage, s);
}

void formatAST(const ASTInsertQuery 		& ast, std::ostream & s)
{
	s << "INSERT INTO " << (!ast.database.empty() ? ast.database + "." : "") << ast.table;

	if (ast.columns)
	{
		s << " (";
		formatAST(*ast.columns, s);
		s << ")";
	}

	if (ast.select)
	{
		s << " ";
		formatAST(*ast.select, s);
	}
	else
	{
		if (!ast.format.empty())
		{
			s << " FORMAT " << ast.format;
		}
		else
		{
			s << " VALUES";
		}
	}
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

static void writeAlias(const String & name, std::ostream & s)
{
	s << " AS ";
	WriteBufferFromOStream wb(s);
	writeProbablyBackQuotedString(name, wb);
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

	if (!ast.alias.empty())
		writeAlias(ast.alias, s);
}

void formatAST(const ASTIdentifier 			& ast, std::ostream & s)
{
	{
		WriteBufferFromOStream wb(s);
		writeProbablyBackQuotedString(ast.name, wb);
	}

	if (!ast.alias.empty())
		writeAlias(ast.alias, s);
}

void formatAST(const ASTLiteral 			& ast, std::ostream & s)
{
	s << boost::apply_visitor(FieldVisitorToString(), ast.value);

	if (!ast.alias.empty())
		writeAlias(ast.alias, s);
}

void formatAST(const ASTNameTypePair		& ast, std::ostream & s)
{
	s << ast.name << " ";
	formatAST(*ast.type, s);
}

void formatAST(const ASTAsterisk			& ast, std::ostream & s)
{
	s << "*";
}

void formatAST(const ASTOrderByElement		& ast, std::ostream & s)
{
	formatAST(*ast.children.front(), s);
	s << (ast.direction == -1 ? " DESC" : " ASC");
}

}

