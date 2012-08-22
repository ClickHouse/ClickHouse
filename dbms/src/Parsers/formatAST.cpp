#include <sstream>

#include <boost/variant/static_visitor.hpp>

#include <Poco/NumberFormatter.h>

#include <mysqlxx/Manip.h>

#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/WriteBufferFromString.h>
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


/// Квотировать идентификатор обратными кавычками, если это требуется.
static String backQuoteIfNeed(const String & x)
{
	String res(x.size(), '\0');
	{
		WriteBufferFromString wb(res);
		writeProbablyBackQuotedString(x, wb);
	}
	return res;
}


void formatAST(const IAST & ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{

#define DISPATCH(NAME) \
	else if (const AST ## NAME * concrete = dynamic_cast<const AST ## NAME *>(&ast)) \
		formatAST(*concrete, s, indent, hilite, one_line);

	if (false) {}
	DISPATCH(SelectQuery)
	DISPATCH(InsertQuery)
	DISPATCH(CreateQuery)
	DISPATCH(DropQuery)
	DISPATCH(RenameQuery)
	DISPATCH(ShowTablesQuery)
	DISPATCH(UseQuery)
	DISPATCH(SetQuery)
	DISPATCH(OptimizeQuery)
	DISPATCH(ExpressionList)
	DISPATCH(Function)
	DISPATCH(Identifier)
	DISPATCH(Literal)
	DISPATCH(NameTypePair)
	DISPATCH(Asterisk)
	DISPATCH(OrderByElement)
	DISPATCH(Subquery)
	else
		throw DB::Exception("Unknown element in AST: " + std::string(ast.range.first, ast.range.second - ast.range.first),
			ErrorCodes::UNKNOWN_ELEMENT_IN_AST);
	
#undef DISPATCH
}


void formatAST(const ASTSelectQuery 		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	std::string nl_or_nothing = one_line ? "" : "\n";
		
	std::string indent_str = one_line ? "" : std::string(4 * indent, ' ');
	std::string nl_or_ws = one_line ? " " : "\n";
			
	s << (hilite ? hilite_keyword : "") << indent_str << "SELECT " << (hilite ? hilite_none : "");
	formatAST(*ast.select_expression_list, s, indent, hilite, one_line);

	if (ast.table)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "FROM " << (hilite ? hilite_none : "");
		if (ast.database)
		{
			formatAST(*ast.database, s, indent, hilite, one_line);
			s << ".";
		}

		if (dynamic_cast<const ASTSelectQuery *>(&*ast.table))
		{
			if (one_line)
				s << " (";
			else
				s << "\n" << indent_str << "(\n";
			
			formatAST(*ast.table, s, indent + 1, hilite, one_line);

			if (one_line)
				s << ")";
			else
				s << "\n" << indent_str << ")";
		}
		else
			formatAST(*ast.table, s, indent, hilite, one_line);
	}

	if (ast.where_expression)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "WHERE " << (hilite ? hilite_none : "");
		formatAST(*ast.where_expression, s, indent, hilite, one_line);
	}

	if (ast.group_expression_list)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "GROUP BY " << (hilite ? hilite_none : "");
		formatAST(*ast.group_expression_list, s, indent, hilite, one_line);
	}

	if (ast.having_expression)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "HAVING " << (hilite ? hilite_none : "");
		formatAST(*ast.having_expression, s, indent, hilite, one_line);
	}

	if (ast.order_expression_list)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "ORDER BY " << (hilite ? hilite_none : "");
		formatAST(*ast.order_expression_list, s, indent, hilite, one_line);
	}

	if (ast.limit_length)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "LIMIT " << (hilite ? hilite_none : "");
		if (ast.limit_offset)
		{
			formatAST(*ast.limit_offset, s, indent, hilite, one_line);
			s << ", ";
		}
		formatAST(*ast.limit_length, s, indent, hilite, one_line);
	}

	if (ast.format)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "FORMAT " << (hilite ? hilite_none : "");
		formatAST(*ast.format, s, indent, hilite, one_line);
	}
}

void formatAST(const ASTSubquery 			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	std::string nl_or_nothing = one_line ? "" : "\n";

	s << nl_or_nothing << "(" << nl_or_nothing;
	formatAST(*ast.children[0], s, indent + 1, hilite, one_line);
	s << nl_or_nothing << ")";
}

void formatAST(const ASTCreateQuery 		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	std::string nl_or_ws = one_line ? " " : "\n";
	
	if (!ast.database.empty() && ast.table.empty())
	{
		s << (hilite ? hilite_keyword : "") << (ast.attach ? "ATTACH DATABASE " : "CREATE DATABASE ") << (ast.if_not_exists ? "IF NOT EXISTS " : "") << (hilite ? hilite_none : "")
			<< backQuoteIfNeed(ast.database);
		return;
	}
	
	s << (hilite ? hilite_keyword : "") << (ast.attach ? "ATTACH TABLE " : "CREATE TABLE ") << (ast.if_not_exists ? "IF NOT EXISTS " : "") << (hilite ? hilite_none : "")
		<< (!ast.database.empty() ? backQuoteIfNeed(ast.database) + "." : "") << backQuoteIfNeed(ast.table);

	if (!ast.as_table.empty())
	{
		s << (hilite ? hilite_keyword : "") << " AS " << (hilite ? hilite_none : "")
			<< (!ast.as_database.empty() ? backQuoteIfNeed(ast.as_database) + "." : "") << backQuoteIfNeed(ast.as_table);
	}

	if (ast.columns)
	{
		s << (one_line ? " (" : "\n(");
		formatAST(*ast.columns, s, indent + 1, hilite, one_line);
		s << (one_line ? ")" : "\n)");
	}

	if (ast.storage)
	{
		s << (hilite ? hilite_keyword : "") << " ENGINE" << (hilite ? hilite_none : "") << " = ";
		formatAST(*ast.storage, s, indent, hilite, one_line);
	}

	if (ast.select)
	{
		s << (hilite ? hilite_keyword : "") << " AS" << nl_or_ws << (hilite ? hilite_none : "");
		formatAST(*ast.select, s, indent, hilite, one_line);
	}
}

void formatAST(const ASTDropQuery 			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	if (ast.table.empty() && !ast.database.empty())
	{
		s << (hilite ? hilite_keyword : "") << (ast.detach ? "DETACH DATABASE " : "DROP DATABASE ") << (ast.if_exists ? "IF EXISTS " : "") << (hilite ? hilite_none : "") << backQuoteIfNeed(ast.database);
		return;
	}

	s << (hilite ? hilite_keyword : "") << (ast.detach ? "DETACH TABLE " : "DROP TABLE ") << (ast.if_exists ? "IF EXISTS " : "") << (hilite ? hilite_none : "")
		<< (!ast.database.empty() ? backQuoteIfNeed(ast.database) + "." : "") << backQuoteIfNeed(ast.table);
}

void formatAST(const ASTOptimizeQuery		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	s << (hilite ? hilite_keyword : "") << "OPTIMIZE TABLE " << (hilite ? hilite_none : "")
		<< (!ast.database.empty() ? backQuoteIfNeed(ast.database) + "." : "") << backQuoteIfNeed(ast.table);
}

void formatAST(const ASTRenameQuery			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	s << (hilite ? hilite_keyword : "") << "RENAME TABLE " << (hilite ? hilite_none : "");

	for (ASTRenameQuery::Elements::const_iterator it = ast.elements.begin(); it != ast.elements.end(); ++it)
	{
		if (it != ast.elements.begin())
			s << ", ";

		s << (!it->from.database.empty() ? backQuoteIfNeed(it->from.database) + "." : "") << backQuoteIfNeed(it->from.table)
			<< (hilite ? hilite_keyword : "") << " TO " << (hilite ? hilite_none : "")
			<< (!it->to.database.empty() ? backQuoteIfNeed(it->to.database) + "." : "") << backQuoteIfNeed(it->to.table);
	}
}

void formatAST(const ASTSetQuery			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	s << (hilite ? hilite_keyword : "") << "SET " << (ast.global ? "GLOBAL " : "") << (hilite ? hilite_none : "");

	for (ASTSetQuery::Changes::const_iterator it = ast.changes.begin(); it != ast.changes.end(); ++it)
	{
		if (it != ast.changes.begin())
			s << ", ";

		s << it->name << " = " << boost::apply_visitor(FieldVisitorToString(), it->value);
	}
}

void formatAST(const ASTShowTablesQuery		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	if (ast.databases)
	{
		s << (hilite ? hilite_keyword : "") << "SHOW DATABASES" << (hilite ? hilite_none : "");
		return;
	}
	
	s << (hilite ? hilite_keyword : "") << "SHOW TABLES" << (hilite ? hilite_none : "");

	if (!ast.from.empty())
		s << (hilite ? hilite_keyword : "") << " FROM " << (hilite ? hilite_none : "")
			<< backQuoteIfNeed(ast.from);

	if (!ast.like.empty())
		s << (hilite ? hilite_keyword : "") << " LIKE " << (hilite ? hilite_none : "")
			<< mysqlxx::quote << ast.like;
}

void formatAST(const ASTUseQuery				& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	s << (hilite ? hilite_keyword : "") << "USE " << (hilite ? hilite_none : "") << backQuoteIfNeed(ast.database);
	return;
}

void formatAST(const ASTInsertQuery 		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	s << (hilite ? hilite_keyword : "") << "INSERT INTO " << (hilite ? hilite_none : "")
		<< (!ast.database.empty() ? backQuoteIfNeed(ast.database) + "." : "") << backQuoteIfNeed(ast.table);

	if (ast.columns)
	{
		s << " (";
		formatAST(*ast.columns, s, indent, hilite, one_line);
		s << ")";
	}

	if (ast.select)
	{
		s << " ";
		formatAST(*ast.select, s, indent, hilite, one_line);
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

void formatAST(const ASTExpressionList 		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	for (ASTs::const_iterator it = ast.children.begin(); it != ast.children.end(); ++it)
	{
		if (it != ast.children.begin())
			s << ", ";
		formatAST(**it, s, indent, hilite, one_line);
	}
}

static void writeAlias(const String & name, std::ostream & s, bool hilite, bool one_line)
{
	s << (hilite ? hilite_keyword : "") << " AS " << (hilite ? hilite_alias : "");
	{
		WriteBufferFromOStream wb(s, 32);
		writeProbablyBackQuotedString(name, wb);
	}
	s << (hilite ? hilite_none : "");
}

void formatAST(const ASTFunction 			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	s << (hilite ? hilite_function : "") << ast.name;
	if (ast.arguments)
	{
		s << '(' << (hilite ? hilite_none : "");
		formatAST(*ast.arguments, s, indent, hilite, one_line);
		s << (hilite ? hilite_function : "") << ')';
	}
	s << (hilite ? hilite_none : "");

	if (!ast.alias.empty())
		writeAlias(ast.alias, s, hilite, one_line);
}

void formatAST(const ASTIdentifier 			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	s << (hilite ? hilite_identifier : "");
	{
		WriteBufferFromOStream wb(s, 32);
		writeProbablyBackQuotedString(ast.name, wb);
	}
	s << (hilite ? hilite_none : "");

	if (!ast.alias.empty())
		writeAlias(ast.alias, s, hilite, one_line);
}

void formatAST(const ASTLiteral 			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	s << boost::apply_visitor(FieldVisitorToString(), ast.value);

	if (!ast.alias.empty())
		writeAlias(ast.alias, s, hilite, one_line);
}

void formatAST(const ASTNameTypePair		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	std::string indent_str = one_line ? "" : std::string(4 * indent, ' ');
	std::string nl_or_ws = one_line ? " " : "\n";
	
	s << nl_or_ws << indent_str << backQuoteIfNeed(ast.name) << " ";
	formatAST(*ast.type, s, indent, hilite, one_line);
}

void formatAST(const ASTAsterisk			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	s << "*";
}

void formatAST(const ASTOrderByElement		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
	formatAST(*ast.children.front(), s, indent, hilite, one_line);
	s << (hilite ? hilite_keyword : "") << (ast.direction == -1 ? " DESC" : " ASC") << (hilite ? hilite_none : "");
}

}

