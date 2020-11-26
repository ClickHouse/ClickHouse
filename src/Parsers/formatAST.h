#pragma once

#include <ostream>
#include <Parsers/IAST.h>


namespace DB
{

class WriteBuffer;

/** Takes a syntax tree and turns it back into text.
  * In case of INSERT query, the data will be missing.
  */
void formatAST(const IAST & ast, WriteBuffer & buf, bool hilite = true, bool one_line = false);

String serializeAST(const IAST & ast, bool one_line = true);

inline WriteBuffer & operator<<(WriteBuffer & buf, const IAST & ast)
{
    formatAST(ast, buf, false, true);
    return buf;
}

inline WriteBuffer & operator<<(WriteBuffer & buf, const ASTPtr & ast)
{
    formatAST(*ast, buf, false, true);
    return buf;
}

}
