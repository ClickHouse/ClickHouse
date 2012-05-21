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


static const char * hilite_keyword = "\033[1;37m";
static const char * hilite_identifier = "\033[0;36m";
static const char * hilite_function = "\033[0;33m";
static const char * hilite_alias = "\033[0;32m";
static const char * hilite_none = "\033[0m";


void formatAST(const IAST & ast, std::ostream & s, size_t indent, bool hilite)
{
	const ASTSelectQuery * select = dynamic_cast<const ASTSelectQuery *>(&ast);
	if (select)
	{
		formatAST(*select, s, indent, hilite);
		return;
	}

	const ASTInsertQuery * insert = dynamic_cast<const ASTInsertQuery *>(&ast);
	if (insert)
	{
		formatAST(*insert, s, indent, hilite);
		return;
	}

	const ASTCreateQuery * create = dynamic_cast<const ASTCreateQuery *>(&ast);
	if (create)
	{
		formatAST(*create, s, indent, hilite);
		return;
	}

	const ASTDropQuery * drop = dynamic_cast<const ASTDropQuery *>(&ast);
	if (drop)
	{
		formatAST(*drop, s, indent, hilite);
		return;
	}
	
	const ASTExpressionList * exp_list = dynamic_cast<const ASTExpressionList *>(&ast);
	if (exp_list)
	{
		formatAST(*exp_list, s, indent, hilite);
		return;
	}

	const ASTFunction * func = dynamic_cast<const ASTFunction *>(&ast);
	if (func)
	{
		formatAST(*func, s, indent, hilite);
		return;
	}

	const ASTIdentifier * id = dynamic_cast<const ASTIdentifier *>(&ast);
	if (id)
	{
		formatAST(*id, s, indent, hilite);
		return;
	}

	const ASTLiteral * lit = dynamic_cast<const ASTLiteral *>(&ast);
	if (lit)
	{
		formatAST(*lit, s, indent, hilite);
		return;
	}

	const ASTNameTypePair * ntp = dynamic_cast<const ASTNameTypePair *>(&ast);
	if (ntp)
	{
		formatAST(*ntp, s, indent, hilite);
		return;
	}

	const ASTAsterisk * asterisk = dynamic_cast<const ASTAsterisk *>(&ast);
	if (asterisk)
	{
		formatAST(*asterisk, s, indent, hilite);
		return;
	}

	const ASTOrderByElement * order_by_elem = dynamic_cast<const ASTOrderByElement *>(&ast);
	if (order_by_elem)
	{
		formatAST(*order_by_elem, s, indent, hilite);
		return;
	}

	throw DB::Exception("Unknown element in AST: " + std::string(ast.range.first, ast.range.second - ast.range.first), ErrorCodes::UNKNOWN_ELEMENT_IN_AST);
}

void formatAST(const ASTSelectQuery 		& ast, std::ostream & s, size_t indent, bool hilite)
{
	std::string indent_str(4 * indent, ' ');
	
	s << (hilite ? hilite_keyword : "") << indent_str << "SELECT " << (hilite ? hilite_none : "");
	formatAST(*ast.select_expression_list, s, indent, hilite);

	if (ast.table)
	{
		s << (hilite ? hilite_keyword : "") << "\n" << indent_str << "FROM " << (hilite ? hilite_none : "");
		if (ast.database)
		{
			formatAST(*ast.database, s, indent, hilite);
			s << ".";
		}

		if (dynamic_cast<const ASTSelectQuery *>(&*ast.table))
		{
			s << "\n" << indent_str << "(\n";
			formatAST(*ast.table, s, indent + 1, hilite);
			s << "\n" << indent_str << ")";
		}
		else
			formatAST(*ast.table, s, indent, hilite);
	}

	if (ast.where_expression)
	{
		s << (hilite ? hilite_keyword : "") << "\n" << indent_str << "WHERE " << (hilite ? hilite_none : "");
		formatAST(*ast.where_expression, s, indent, hilite);
	}

	if (ast.group_expression_list)
	{
		s << (hilite ? hilite_keyword : "") << "\n" << indent_str << "GROUP BY " << (hilite ? hilite_none : "");
		formatAST(*ast.group_expression_list, s, indent, hilite);
	}

	if (ast.having_expression)
	{
		s << (hilite ? hilite_keyword : "") << "\n" << indent_str << "HAVING " << (hilite ? hilite_none : "");
		formatAST(*ast.having_expression, s, indent, hilite);
	}

	if (ast.order_expression_list)
	{
		s << (hilite ? hilite_keyword : "") << "\n" << indent_str << "ORDER BY " << (hilite ? hilite_none : "");
		formatAST(*ast.order_expression_list, s, indent, hilite);
	}

	if (ast.limit_length)
	{
		s << (hilite ? hilite_keyword : "") << "\n" << indent_str << "LIMIT " << (hilite ? hilite_none : "");
		if (ast.limit_offset)
		{
			formatAST(*ast.limit_offset, s, indent, hilite);
			s << ", ";
		}
		formatAST(*ast.limit_length, s, indent, hilite);
	}

	if (ast.format)
	{
		s << (hilite ? hilite_keyword : "") << "\n" << indent_str << "FORMAT " << (hilite ? hilite_none : "");
		formatAST(*ast.format, s, indent, hilite);
	}
}

void formatAST(const ASTCreateQuery 		& ast, std::ostream & s, size_t indent, bool hilite)
{
	if (!ast.database.empty() && ast.table.empty())
	{
		s << (hilite ? hilite_keyword : "") << (ast.attach ? "ATTACH DATABASE " : "CREATE DATABASE ") << (ast.if_not_exists ? "IF NOT EXISTS " : "") << (hilite ? hilite_none : "")
			<< ast.database;
		return;
	}
	
	s << (hilite ? hilite_keyword : "") << (ast.attach ? "ATTACH TABLE " : "CREATE TABLE ") << (ast.if_not_exists ? "IF NOT EXISTS " : "") << (hilite ? hilite_none : "")
		<< (!ast.database.empty() ? ast.database + "." : "") << ast.table;

	if (!ast.as_table.empty())
	{
		s << (hilite ? hilite_keyword : "") << " AS " << (hilite ? hilite_none : "")
			<< (!ast.as_database.empty() ? ast.as_database + "." : "") << ast.as_table;
	}

	if (ast.columns)
	{
		s << "\n(\n";
		formatAST(*ast.columns, s, indent + 1, hilite);
		s << ")";
	}

	if (ast.storage)
	{
		s << (hilite ? hilite_keyword : "") << " ENGINE" << (hilite ? hilite_none : "") << " = ";
		formatAST(*ast.storage, s, indent, hilite);
	}

	if (ast.select)
	{
		s << (hilite ? hilite_keyword : "") << " AS\n" << (hilite ? hilite_none : "");
		formatAST(*ast.select, s, indent, hilite);
	}
}

void formatAST(const ASTDropQuery 			& ast, std::ostream & s, size_t indent, bool hilite)
{
	if (!ast.database.empty())
	{
		s << (hilite ? hilite_keyword : "") << (ast.detach ? "DETACH DATABASE " : "DROP DATABASE ") << (ast.if_exists ? "IF EXISTS " : "") << (hilite ? hilite_none : "") << ast.database;
		return;
	}

	s << (hilite ? hilite_keyword : "") << (ast.detach ? "DETACH TABLE " : "DROP TABLE ") << (ast.if_exists ? "IF EXISTS " : "") << (hilite ? hilite_none : "") << ast.table;
}

void formatAST(const ASTInsertQuery 		& ast, std::ostream & s, size_t indent, bool hilite)
{
	s << (hilite ? hilite_keyword : "") << "INSERT INTO " << (hilite ? hilite_none : "") << (!ast.database.empty() ? ast.database + "." : "") << ast.table;

	if (ast.columns)
	{
		s << " (";
		formatAST(*ast.columns, s, indent, hilite);
		s << ")";
	}

	if (ast.select)
	{
		s << " ";
		formatAST(*ast.select, s, indent, hilite);
	}
	else
	{
		if (!ast.format.empty())
		{
			s << (hilite ? hilite_keyword : "") << " FORMAT " << (hilite ? hilite_none : "") << ast.format;
		}
		else
		{
			s << (hilite ? hilite_keyword : "") << " VALUES" << (hilite ? hilite_none : "");
		}
	}
}

void formatAST(const ASTExpressionList 		& ast, std::ostream & s, size_t indent, bool hilite)
{
	for (ASTs::const_iterator it = ast.children.begin(); it != ast.children.end(); ++it)
	{
		if (it != ast.children.begin())
			s << ", ";
		formatAST(**it, s, indent, hilite);
	}
}

static void writeAlias(const String & name, std::ostream & s, bool hilite)
{
	s << (hilite ? hilite_keyword : "") << " AS " << (hilite ? hilite_alias : "");
	{
		WriteBufferFromOStream wb(s);
		writeProbablyBackQuotedString(name, wb);
	}
	s << (hilite ? hilite_none : "");
}

void formatAST(const ASTFunction 			& ast, std::ostream & s, size_t indent, bool hilite)
{
	s << (hilite ? hilite_function : "") << ast.name;
	if (ast.arguments)
	{
		s << '(' << (hilite ? hilite_none : "");
		formatAST(*ast.arguments, s, indent, hilite);
		s << (hilite ? hilite_function : "") << ')';
	}
	s << (hilite ? hilite_none : "");

	if (!ast.alias.empty())
		writeAlias(ast.alias, s, hilite);
}

void formatAST(const ASTIdentifier 			& ast, std::ostream & s, size_t indent, bool hilite)
{
	s << (hilite ? hilite_identifier : "");
	{
		WriteBufferFromOStream wb(s);
		writeProbablyBackQuotedString(ast.name, wb);
	}
	s << (hilite ? hilite_none : "");

	if (!ast.alias.empty())
		writeAlias(ast.alias, s, hilite);
}

void formatAST(const ASTLiteral 			& ast, std::ostream & s, size_t indent, bool hilite)
{
	s << boost::apply_visitor(FieldVisitorToString(), ast.value);

	if (!ast.alias.empty())
		writeAlias(ast.alias, s, hilite);
}

void formatAST(const ASTNameTypePair		& ast, std::ostream & s, size_t indent, bool hilite)
{
	std::string indent_str(4 * indent, ' ');
	s << indent_str << ast.name << " ";
	formatAST(*ast.type, s, indent, hilite);
	s << "\n";
}

void formatAST(const ASTAsterisk			& ast, std::ostream & s, size_t indent, bool hilite)
{
	s << "*";
}

void formatAST(const ASTOrderByElement		& ast, std::ostream & s, size_t indent, bool hilite)
{
	formatAST(*ast.children.front(), s, indent, hilite);
	s << (hilite ? hilite_keyword : "") << (ast.direction == -1 ? " DESC" : " ASC") << (hilite ? hilite_none : "");
}

}

