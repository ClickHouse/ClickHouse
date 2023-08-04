#include <Parsers/formatAST.h>


namespace DB
{

void formatAST(const IAST & ast, WriteBuffer & buf, bool hilite, bool one_line, bool show_secrets)
{
    IAST::FormatSettings settings(buf, one_line);
    settings.hilite = hilite;
    settings.show_secrets = show_secrets;
    ast.format(settings);
}

String serializeAST(const IAST & ast, bool one_line)
{
    WriteBufferFromOwnString buf;
    formatAST(ast, buf, false, one_line);
    return buf.str();
}

}
