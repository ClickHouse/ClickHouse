#pragma once

#include <ostream>

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTDropQuery.h>
#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTRenameQuery.h>
#include <DB/Parsers/ASTShowTablesQuery.h>
#include <DB/Parsers/ASTUseQuery.h>
#include <DB/Parsers/ASTSetQuery.h>
#include <DB/Parsers/ASTOptimizeQuery.h>
#include <DB/Parsers/TablePropertiesQueriesASTs.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/Parsers/ASTAsterisk.h>
#include <DB/Parsers/ASTOrderByElement.h>
#include <DB/Parsers/ASTSubquery.h>
#include <DB/Parsers/ASTAlterQuery.h>
#include <DB/Parsers/ASTShowProcesslistQuery.h>
#include <DB/Parsers/ASTJoin.h>
#include <DB/Parsers/ASTCheckQuery.h>
//#include <DB/Parsers/ASTMultiQuery.h>


namespace DB
{

/** Берёт синтаксическое дерево и превращает его обратно в текст.
  * В случае запроса INSERT, данные будут отсутствовать.
  */
void formatAST(const IAST 				& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);

void formatAST(const ASTSelectQuery 	& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTCreateQuery 	& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTDropQuery 		& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTInsertQuery 	& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTRenameQuery 	& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTShowTablesQuery & ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTUseQuery		& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTSetQuery		& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTOptimizeQuery	& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTExistsQuery		& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTDescribeQuery	& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTShowCreateQuery & ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTExpressionList	& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTFunction 		& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTIdentifier 		& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTLiteral 		& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTNameTypePair	& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTAsterisk		& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTOrderByElement	& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTSubquery		& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTAlterQuery 		& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTJoin 			& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
void formatAST(const ASTCheckQuery 		& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);
//void formatAST(const ASTMultiQuery 		& ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);

void formatAST(const ASTQueryWithTableAndOutput & ast, std::string name, std::ostream & s,
			   size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);

void formatAST(const ASTShowProcesslistQuery 	& ast, std::ostream & s,
			   size_t indent = 0, bool hilite = true, bool one_line = false, bool need_parens = false);


String formatColumnsForCreateQuery(NamesAndTypesList & columns);
String backQuoteIfNeed(const String & x);

}
