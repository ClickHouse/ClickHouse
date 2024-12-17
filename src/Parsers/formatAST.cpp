#include <Parsers/formatAST.h>


namespace DB
{

void formatAST(const IAST & ast, WriteBuffer & buf, bool hilite, bool one_line, bool show_secrets)
{
    IAST::FormatSettings settings(one_line, hilite);
    settings.show_secrets = show_secrets;
    ast.format(buf, settings);
}

String serializeAST(const IAST & ast)
{
    WriteBufferFromOwnString buf;
    formatAST(ast, buf, false, true);
    return buf.str();
}

}
