#pragma once

#include <ostream>
#include <Parsers/IAST.h>


namespace DB
{

/** Takes a syntax tree and turns it back into text.
  * In case of INSERT query, the data will be missing.
  */
void formatAST(const IAST & ast, std::ostream & s, bool hilite = true, bool one_line = false);

inline std::ostream & operator<<(std::ostream & os, const IAST & ast)
{
    formatAST(ast, os, false, true);
    return os;
}

inline std::ostream & operator<<(std::ostream & os, const ASTPtr & ast)
{
    formatAST(*ast, os, false, true);
    return os;
}

}
