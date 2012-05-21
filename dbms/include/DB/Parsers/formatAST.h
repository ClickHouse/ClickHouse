#pragma once

#include <ostream>

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTDropQuery.h>
#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/Parsers/ASTAsterisk.h>
#include <DB/Parsers/ASTOrderByElement.h>


namespace DB
{

/** Берёт синтаксическое дерево и превращает его обратно в текст.
  * В случае запроса INSERT, данные могут быть опущены.
  */
void formatAST(const IAST 				& ast, std::ostream & s, size_t indent = 0, bool hilite = true);

void formatAST(const ASTSelectQuery 	& ast, std::ostream & s, size_t indent = 0, bool hilite = true);
void formatAST(const ASTCreateQuery 	& ast, std::ostream & s, size_t indent = 0, bool hilite = true);
void formatAST(const ASTDropQuery 		& ast, std::ostream & s, size_t indent = 0, bool hilite = true);
void formatAST(const ASTInsertQuery 	& ast, std::ostream & s, size_t indent = 0, bool hilite = true);
void formatAST(const ASTExpressionList 	& ast, std::ostream & s, size_t indent = 0, bool hilite = true);
void formatAST(const ASTFunction 		& ast, std::ostream & s, size_t indent = 0, bool hilite = true);
void formatAST(const ASTIdentifier 		& ast, std::ostream & s, size_t indent = 0, bool hilite = true);
void formatAST(const ASTLiteral 		& ast, std::ostream & s, size_t indent = 0, bool hilite = true);
void formatAST(const ASTNameTypePair	& ast, std::ostream & s, size_t indent = 0, bool hilite = true);
void formatAST(const ASTAsterisk		& ast, std::ostream & s, size_t indent = 0, bool hilite = true);
void formatAST(const ASTOrderByElement	& ast, std::ostream & s, size_t indent = 0, bool hilite = true);

}
