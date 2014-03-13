#include <sstream>

#include <boost/variant/static_visitor.hpp>

#include <mysqlxx/Manip.h>

#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Core/NamesAndTypes.h>

#include <DB/Parsers/formatAST.h>


namespace DB
{


static const char * hilite_keyword = "\033[1;37m";
static const char * hilite_identifier = "\033[0;36m";
static const char * hilite_function = "\033[0;33m";
static const char * hilite_operator = "\033[1;33m";
static const char * hilite_alias = "\033[0;32m";
static const char * hilite_none = "\033[0m";


/// Квотировать идентификатор обратными кавычками, если это требуется.
String backQuoteIfNeed(const String & x)
{
	String res(x.size(), '\0');
	{
		WriteBufferFromString wb(res);
		writeProbablyBackQuotedString(x, wb);
	}
	return res;
}


void formatAST(const IAST & ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{

#define DISPATCH(NAME) \
	else if (const AST ## NAME * concrete = dynamic_cast<const AST ## NAME *>(&ast)) \
		formatAST(*concrete, s, indent, hilite, one_line, need_parens);

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
	DISPATCH(ExistsQuery)
	DISPATCH(ShowCreateQuery)
	DISPATCH(DescribeQuery)
	DISPATCH(ExpressionList)
	DISPATCH(Function)
	DISPATCH(Identifier)
	DISPATCH(Literal)
	DISPATCH(NameTypePair)
	DISPATCH(Asterisk)
	DISPATCH(OrderByElement)
	DISPATCH(Subquery)
	DISPATCH(AlterQuery)
	DISPATCH(ShowProcesslistQuery)
	else
		throw Exception("Unknown element in AST: " + ast.getID() + " '" + std::string(ast.range.first, ast.range.second - ast.range.first) + "'",
			ErrorCodes::UNKNOWN_ELEMENT_IN_AST);
	
#undef DISPATCH
}


void formatAST(const ASTExpressionList 		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	for (ASTs::const_iterator it = ast.children.begin(); it != ast.children.end(); ++it)
	{
		if (it != ast.children.begin())
			s << ", ";

		formatAST(**it, s, indent, hilite, one_line, need_parens);
	}
}

/** Вывести список выражений в секциях запроса SELECT - по одному выражению на строку.
  */
static void formatExpressionListMultiline(const ASTExpressionList & ast, std::ostream & s, size_t indent, bool hilite)
{
	std::string indent_str = "\n" + std::string(4 * (indent + 1), ' ');

	for (ASTs::const_iterator it = ast.children.begin(); it != ast.children.end(); ++it)
	{
		if (it != ast.children.begin())
			s << ", ";

		if (ast.children.size() > 1)
			s << indent_str;

		formatAST(**it, s, indent + 1, hilite, false);
	}
}


void formatAST(const ASTSelectQuery 		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	std::string nl_or_nothing = one_line ? "" : "\n";
		
	std::string indent_str = one_line ? "" : std::string(4 * indent, ' ');
	std::string nl_or_ws = one_line ? " " : "\n";
			
	s << (hilite ? hilite_keyword : "") << indent_str << "SELECT " << (ast.distinct ? "DISTINCT " : "") << (hilite ? hilite_none : "");
	one_line
		? formatAST(*ast.select_expression_list, s, indent, hilite, one_line)
		: formatExpressionListMultiline(dynamic_cast<const ASTExpressionList &>(*ast.select_expression_list), s, indent, hilite);

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
	
	if (ast.array_join_expression_list)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "ARRAY JOIN " << (hilite ? hilite_none : "");
		one_line
			? formatAST(*ast.array_join_expression_list, s, indent, hilite, one_line)
			: formatExpressionListMultiline(dynamic_cast<const ASTExpressionList &>(*ast.array_join_expression_list), s, indent, hilite);
	}
	
	if (ast.final)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "FINAL" << (hilite ? hilite_none : "");
	}
	
	if (ast.sample_size)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "SAMPLE " << (hilite ? hilite_none : "");
		formatAST(*ast.sample_size, s, indent, hilite, one_line);
	}

	if (ast.prewhere_expression)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "PREWHERE " << (hilite ? hilite_none : "");
		one_line
			? formatAST(*ast.prewhere_expression, s, indent, hilite, one_line)
			: formatExpressionListMultiline(dynamic_cast<const ASTExpressionList &>(*ast.prewhere_expression), s, indent, hilite);
	}

	if (ast.where_expression)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "WHERE " << (hilite ? hilite_none : "");
		formatAST(*ast.where_expression, s, indent, hilite, one_line);
	}

	if (ast.group_expression_list)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "GROUP BY " << (hilite ? hilite_none : "");
		one_line
			? formatAST(*ast.group_expression_list, s, indent, hilite, one_line)
			: formatExpressionListMultiline(dynamic_cast<const ASTExpressionList &>(*ast.group_expression_list), s, indent, hilite);

		if (ast.group_by_with_totals)
			s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << (one_line ? "" : "    ") << "WITH TOTALS" << (hilite ? hilite_none : "");
	}

	if (ast.having_expression)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "HAVING " << (hilite ? hilite_none : "");
		formatAST(*ast.having_expression, s, indent, hilite, one_line);
	}

	if (ast.order_expression_list)
	{
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "ORDER BY " << (hilite ? hilite_none : "");
		one_line
			? formatAST(*ast.order_expression_list, s, indent, hilite, one_line)
			: formatExpressionListMultiline(dynamic_cast<const ASTExpressionList &>(*ast.order_expression_list), s, indent, hilite);
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

void formatAST(const ASTSubquery 			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	std::string indent_str = one_line ? "" : std::string(4 * indent, ' ');
	std::string nl_or_nothing = one_line ? "" : "\n";

	s << nl_or_nothing << indent_str << "(" << nl_or_nothing;
	formatAST(*ast.children[0], s, indent + 1, hilite, one_line);
	s << nl_or_nothing << indent_str << ")";
}

void formatAST(const ASTCreateQuery 		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	std::string nl_or_ws = one_line ? " " : "\n";
	
	if (!ast.database.empty() && ast.table.empty())
	{
		s << (hilite ? hilite_keyword : "") << (ast.attach ? "ATTACH DATABASE " : "CREATE DATABASE ") << (ast.if_not_exists ? "IF NOT EXISTS " : "") << (hilite ? hilite_none : "")
			<< backQuoteIfNeed(ast.database);
		return;
	}
	
	{
		std::string what = "TABLE";
		if (ast.is_view)
			what = "VIEW";
		if (ast.is_materialized_view)
			what = "MATERIALIZED VIEW";

		s << (hilite ? hilite_keyword : "") << (ast.attach ? "ATTACH " : "CREATE ") << what << " " << (ast.if_not_exists ? "IF NOT EXISTS " : "") << (hilite ? hilite_none : "")
		<< (!ast.database.empty() ? backQuoteIfNeed(ast.database) + "." : "") << backQuoteIfNeed(ast.table);
	}

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

	if (ast.storage && !ast.is_materialized_view && !ast.is_view)
	{
		s << (hilite ? hilite_keyword : "") << " ENGINE" << (hilite ? hilite_none : "") << " = ";
		formatAST(*ast.storage, s, indent, hilite, one_line);
	}

	if (ast.inner_storage)
	{
		s << (hilite ? hilite_keyword : "") << " ENGINE" << (hilite ? hilite_none : "") << " = ";
		formatAST(*ast.inner_storage, s, indent, hilite, one_line);
	}

	if (ast.is_populate)
	{
		s << (hilite ? hilite_keyword : "") << " POPULATE" << (hilite ? hilite_none : "");
	}

	if (ast.select)
	{
		s << (hilite ? hilite_keyword : "") << " AS" << nl_or_ws << (hilite ? hilite_none : "");
		formatAST(*ast.select, s, indent, hilite, one_line);
	}
}

void formatAST(const ASTDropQuery 			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	if (ast.table.empty() && !ast.database.empty())
	{
		s << (hilite ? hilite_keyword : "") << (ast.detach ? "DETACH DATABASE " : "DROP DATABASE ") << (ast.if_exists ? "IF EXISTS " : "") << (hilite ? hilite_none : "") << backQuoteIfNeed(ast.database);
		return;
	}

	s << (hilite ? hilite_keyword : "") << (ast.detach ? "DETACH TABLE " : "DROP TABLE ") << (ast.if_exists ? "IF EXISTS " : "") << (hilite ? hilite_none : "")
		<< (!ast.database.empty() ? backQuoteIfNeed(ast.database) + "." : "") << backQuoteIfNeed(ast.table);
}

void formatAST(const ASTOptimizeQuery		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	s << (hilite ? hilite_keyword : "") << "OPTIMIZE TABLE " << (hilite ? hilite_none : "")
		<< (!ast.database.empty() ? backQuoteIfNeed(ast.database) + "." : "") << backQuoteIfNeed(ast.table);
}

void formatAST(const ASTQueryWithTableAndOutput & ast, std::string name, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	s << (hilite ? hilite_keyword : "") << name << " " << (hilite ? hilite_none : "")
	<< (!ast.database.empty() ? backQuoteIfNeed(ast.database) + "." : "") << backQuoteIfNeed(ast.table);
	
	if (ast.format)
	{
		std::string indent_str = one_line ? "" : std::string(4 * indent, ' ');
		std::string nl_or_ws = one_line ? " " : "\n";
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "FORMAT " << (hilite ? hilite_none : "");
		formatAST(*ast.format, s, indent, hilite, one_line);
	}
}

void formatAST(const ASTExistsQuery			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	formatAST(static_cast<const ASTQueryWithTableAndOutput &>(ast), "EXISTS TABLE", s, indent, hilite, one_line);
}

void formatAST(const ASTDescribeQuery			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	formatAST(static_cast<const ASTQueryWithTableAndOutput &>(ast), "DESCRIBE TABLE", s, indent, hilite, one_line);
}

void formatAST(const ASTShowCreateQuery		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	formatAST(static_cast<const ASTQueryWithTableAndOutput &>(ast), "SHOW CREATE TABLE", s, indent, hilite, one_line);
}

void formatAST(const ASTRenameQuery			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
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

void formatAST(const ASTSetQuery			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	s << (hilite ? hilite_keyword : "") << "SET " << (ast.global ? "GLOBAL " : "") << (hilite ? hilite_none : "");

	for (ASTSetQuery::Changes::const_iterator it = ast.changes.begin(); it != ast.changes.end(); ++it)
	{
		if (it != ast.changes.begin())
			s << ", ";

		s << it->name << " = " << apply_visitor(FieldVisitorToString(), it->value);
	}
}

void formatAST(const ASTShowTablesQuery		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	if (ast.databases)
	{
		s << (hilite ? hilite_keyword : "") << "SHOW DATABASES" << (hilite ? hilite_none : "");
	}
	else
	{
		s << (hilite ? hilite_keyword : "") << "SHOW TABLES" << (hilite ? hilite_none : "");

		if (!ast.from.empty())
			s << (hilite ? hilite_keyword : "") << " FROM " << (hilite ? hilite_none : "")
				<< backQuoteIfNeed(ast.from);

		if (!ast.like.empty())
			s << (hilite ? hilite_keyword : "") << " LIKE " << (hilite ? hilite_none : "")
				<< mysqlxx::quote << ast.like;
	}
			
	if (ast.format)
	{
		std::string indent_str = one_line ? "" : std::string(4 * indent, ' ');
		std::string nl_or_ws = one_line ? " " : "\n";
		s << (hilite ? hilite_keyword : "") << nl_or_ws << indent_str << "FORMAT " << (hilite ? hilite_none : "");
		formatAST(*ast.format, s, indent, hilite, one_line);
	}
}

void formatAST(const ASTUseQuery				& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	s << (hilite ? hilite_keyword : "") << "USE " << (hilite ? hilite_none : "") << backQuoteIfNeed(ast.database);
	return;
}

void formatAST(const ASTShowProcesslistQuery	& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	s << (hilite ? hilite_keyword : "") << "SHOW PROCESSLIST" << (hilite ? hilite_none : "");
	return;
}

void formatAST(const ASTInsertQuery 		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
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

static void writeAlias(const String & name, std::ostream & s, bool hilite, bool one_line)
{
	s << (hilite ? hilite_keyword : "") << " AS " << (hilite ? hilite_alias : "");
	
	WriteBufferFromOStream wb(s, 32);
	writeProbablyBackQuotedString(name, wb);
	wb.next();
	
	s << (hilite ? hilite_none : "");
}

void formatAST(const ASTFunction 			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	/// Стоит ли записать эту функцию в виде оператора?
	bool written = false;
	if (ast.arguments && !ast.parameters)
	{
		if (ast.arguments->children.size() == 1)
		{
			const char * operators[] =
			{
				"negate", "-",
				"not", "NOT ",
				nullptr
			};

			for (const char ** func = operators; *func; func += 2)
			{
				if (0 == strcmp(ast.name.c_str(), func[0]))
				{
					s << (hilite ? hilite_operator : "") << func[1] << (hilite ? hilite_none : "");
					formatAST(*ast.arguments, s, indent, hilite, one_line, true);
					written = true;
				}
			}
		}

		/** need_parens - нужны ли скобки вокруг выражения с оператором.
		  * Они нужны, только если это выражение входит в другое выражение с оператором.
		  */

		if (!written && ast.arguments->children.size() == 2)
		{
			const char * operators[] =
			{
				"multiply",			" * ",
				"divide",			" / ",
				"modulo",			" % ",
				"plus", 			" + ",
				"minus", 			" - ",
				"notEquals",		" != ",
				"lessOrEquals",		" <= ",
				"greaterOrEquals",	" >= ",
				"less",				" < ",
				"greater",			" > ",
				"equals",			" = ",
				"like",				" LIKE ",
				"notLike",			" NOT LIKE ",
				"in",				" IN ",
				"notIn",			" NOT IN ",
				nullptr
			};

			for (const char ** func = operators; *func; func += 2)
			{
				if (0 == strcmp(ast.name.c_str(), func[0]))
				{
					if (need_parens)
						s << '(';
					formatAST(*ast.arguments->children[0], s, indent, hilite, one_line, true);
					s << (hilite ? hilite_operator : "") << func[1] << (hilite ? hilite_none : "");
					formatAST(*ast.arguments->children[1], s, indent, hilite, one_line, true);
					if (need_parens)
						s << ')';
					written = true;
				}
			}

			if (!written && 0 == strcmp(ast.name.c_str(), "arrayElement"))
			{
				formatAST(*ast.arguments->children[0], s, indent, hilite, one_line, true);
				s << (hilite ? hilite_operator : "") << '[' << (hilite ? hilite_none : "");
				formatAST(*ast.arguments->children[1], s, indent, hilite, one_line, true);
				s << (hilite ? hilite_operator : "") << ']' << (hilite ? hilite_none : "");
				written = true;
			}

			if (!written && 0 == strcmp(ast.name.c_str(), "tupleElement"))
			{
				formatAST(*ast.arguments->children[0], s, indent, hilite, one_line, true);
				s << (hilite ? hilite_operator : "") << "." << (hilite ? hilite_none : "");
				formatAST(*ast.arguments->children[1], s, indent, hilite, one_line, true);
				written = true;
			}
		}

		if (!written && ast.arguments->children.size() >= 2)
		{
			const char * operators[] =
			{
				"and",				" AND ",
				"or",				" OR ",
				nullptr
			};

			for (const char ** func = operators; *func; func += 2)
			{
				if (0 == strcmp(ast.name.c_str(), func[0]))
				{
					if (need_parens)
						s << '(';
					for (size_t i = 0; i < ast.arguments->children.size(); ++i)
					{
						if (i != 0)
							s << (hilite ? hilite_operator : "") << func[1] << (hilite ? hilite_none : "");
						formatAST(*ast.arguments->children[i], s, indent, hilite, one_line, true);
					}
					if (need_parens)
						s << ')';
					written = true;
				}
			}

			if (!written && 0 == strcmp(ast.name.c_str(), "array"))
			{
				s << '[';
				for (size_t i = 0; i < ast.arguments->children.size(); ++i)
				{
					if (i != 0)
						s << ", ";
					formatAST(*ast.arguments->children[i], s, indent, hilite, one_line, false);
				}
				s << ']';
				written = true;
			}

			if (!written && 0 == strcmp(ast.name.c_str(), "tuple"))
			{
				s << '(';
				for (size_t i = 0; i < ast.arguments->children.size(); ++i)
				{
					if (i != 0)
						s << ", ";
					formatAST(*ast.arguments->children[i], s, indent, hilite, one_line, false);
				}
				s << ')';
				written = true;
			}
		}
	}

	if (!written)
	{
		s << (hilite ? hilite_function : "") << ast.name;

		if (ast.parameters)
		{
			s << '(' << (hilite ? hilite_none : "");
			formatAST(*ast.parameters, s, indent, hilite, one_line);
			s << (hilite ? hilite_function : "") << ')';
		}

		if (ast.arguments)
		{
			s << '(' << (hilite ? hilite_none : "");
			formatAST(*ast.arguments, s, indent, hilite, one_line);
			s << (hilite ? hilite_function : "") << ')';
		}

		s << (hilite ? hilite_none : "");
	}

	if (!ast.alias.empty())
		writeAlias(ast.alias, s, hilite, one_line);
}

void formatAST(const ASTIdentifier 			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	s << (hilite ? hilite_identifier : "");

	WriteBufferFromOStream wb(s, 32);
	writeProbablyBackQuotedString(ast.name, wb);
	wb.next();

	s << (hilite ? hilite_none : "");

	if (!ast.alias.empty())
		writeAlias(ast.alias, s, hilite, one_line);
}

void formatAST(const ASTLiteral 			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	s << apply_visitor(FieldVisitorToString(), ast.value);

	if (!ast.alias.empty())
		writeAlias(ast.alias, s, hilite, one_line);
}

void formatAST(const ASTNameTypePair		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	std::string indent_str = one_line ? "" : std::string(4 * indent, ' ');
	std::string nl_or_ws = one_line ? " " : "\n";
	
	s << nl_or_ws << indent_str << backQuoteIfNeed(ast.name) << " ";
	formatAST(*ast.type, s, indent, hilite, one_line);
}

void formatAST(const ASTAsterisk			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	s << "*";
}

void formatAST(const ASTOrderByElement		& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	formatAST(*ast.children.front(), s, indent, hilite, one_line);
	s << (hilite ? hilite_keyword : "") << (ast.direction == -1 ? " DESC" : " ASC") << (hilite ? hilite_none : "");
	if (!ast.collator.isNull())
	{
		s << (hilite ? hilite_keyword : "") << " COLLATE " << (hilite ? hilite_none : "")
			<< "'" << ast.collator->getLocale() << "'";
	}
}

void formatAST(const ASTAlterQuery 			& ast, std::ostream & s, size_t indent, bool hilite, bool one_line, bool need_parens)
{
	std::string nl_or_nothing = one_line ? "" : "\n";

	std::string indent_str = one_line ? "" : std::string(4 * indent, ' ');
	std::string nl_or_ws = one_line ? " " : "\n";

	s << (hilite ? hilite_keyword : "") << indent_str << "ALTER TABLE " << (hilite ? hilite_none : "");

	if (!ast.table.empty())
	{
		if (!ast.database.empty())
		{
			s << (hilite ? hilite_keyword : "") << indent_str << ast.database << (hilite ? hilite_none : "");
			s << ".";
		}
		s << (hilite ? hilite_keyword : "") << indent_str << ast.table << (hilite ? hilite_none : "");
	}
	s << nl_or_ws;

	for( std::size_t i = 0; i < ast.parameters.size(); ++i)
	{
		const ASTAlterQuery::Parameters &p = ast.parameters[i];

		if (p.type == ASTAlterQuery::ADD)
		{
			s << (hilite ? hilite_keyword : "") << indent_str << "ADD COLUMN " << (hilite ? hilite_none : "");
			formatAST(*p.name_type, s, indent, hilite, true);

			/// AFTER
			if (p.column)
			{
				s << (hilite ? hilite_keyword : "") << indent_str << " AFTER " << (hilite ? hilite_none : "");
				formatAST(*p.column, s, indent, hilite, one_line);
			}
		}
		else if (p.type == ASTAlterQuery::DROP)
		{
			s << (hilite ? hilite_keyword : "") << indent_str << "DROP COLUMN " << (hilite ? hilite_none : "");
			formatAST(*p.column, s, indent, hilite, true);
		}

		std::string comma = (i < (ast.parameters.size() -1) ) ? "," : "";
		s << (hilite ? hilite_keyword : "") << indent_str << comma << (hilite ? hilite_none : "");

		s << nl_or_ws;
	}
}


String formatColumnsForCreateQuery(NamesAndTypesList & columns)
{
	std::string res;
	res += "(";
	for (NamesAndTypesList::iterator it = columns.begin(); it != columns.end(); ++it)
	{
		if (it != columns.begin())
			res += ", ";
		res += backQuoteIfNeed(it->first);
		res += " ";
		res += it->second->getName();
	}
	res += ")";
	return res;
}

}

