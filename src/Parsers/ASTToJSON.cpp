#include <Parsers/ASTToJSON.h>
#include <Parsers/IAST.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

String serializeASTToJSON(const IAST & ast)
{
    WriteBufferFromOwnString buf;
    ast.writeJSON(buf);
    return buf.str();
}

void serializeASTToJSON(const IAST & ast, WriteBuffer & out)
{
    ast.writeJSON(out);
}

}
