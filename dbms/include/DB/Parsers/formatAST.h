#ifndef DBMS_PARSERS_FORMATAST_H
#define DBMS_PARSERS_FORMATAST_H

#include <ostream>

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTNameTypePair.h>


namespace DB
{

/** Берёт синтаксическое дерево и превращает его обратно в текст.
  */
void formatAST(const IAST 				& ast, std::ostream & s);

void formatAST(const ASTSelectQuery 	& ast, std::ostream & s);
void formatAST(const ASTCreateQuery 	& ast, std::ostream & s);
void formatAST(const ASTExpressionList 	& ast, std::ostream & s);
void formatAST(const ASTFunction 		& ast, std::ostream & s);
void formatAST(const ASTIdentifier 		& ast, std::ostream & s);
void formatAST(const ASTLiteral 		& ast, std::ostream & s);
void formatAST(const ASTNameTypePair	& ast, std::ostream & s);

}


#endif
