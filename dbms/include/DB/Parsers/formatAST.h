#pragma once

#include <ostream>

#include <DB/Core/NamesAndTypes.h>
#include <DB/Parsers/IAST.h>


namespace DB
{

/** Берёт синтаксическое дерево и превращает его обратно в текст.
  * В случае запроса INSERT, данные будут отсутствовать.
  */
void formatAST(const IAST & ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false);

std::string formatASTToString(const IAST & ast);

String formatColumnsForCreateQuery(NamesAndTypesList & columns);

inline std::ostream & operator<<(std::ostream & os, const IAST & ast) { return formatAST(ast, os, 0, false, true), os; }
inline std::ostream & operator<<(std::ostream & os, const ASTPtr & ast) { return formatAST(*ast, os, 0, false, true), os; }

}
