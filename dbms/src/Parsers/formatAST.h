#pragma once

#include <ostream>

#include <Core/NamesAndTypes.h>
#include <Parsers/IAST.h>


namespace DB
{

/** Takes a syntax tree and turns it back into text.
  * In case of INSERT query, the data will be missing.
  */
void formatAST(const IAST & ast, std::ostream & s, size_t indent = 0, bool hilite = true, bool one_line = false);

std::string formatASTToString(const IAST & ast);

String formatColumnsForCreateQuery(NamesAndTypesList & columns);

inline std::ostream & operator<<(std::ostream & os, const IAST & ast) { return formatAST(ast, os, 0, false, true), os; }
inline std::ostream & operator<<(std::ostream & os, const ASTPtr & ast) { return formatAST(*ast, os, 0, false, true), os; }

}
