#include <Parsers/formatAST.h>


namespace DB
{

void formatAST(const IAST & ast, WriteBuffer & buf, bool hilite, bool one_line)
{
    IAST::FormatSettings settings{
            .hilite = hilite,
            .one_line = one_line,
            .always_quote_identifiers = false,
            .identifier_quoting_style = IdentifierQuotingStyle::Backticks
    };
    ast.format(buf, settings);
}

String serializeAST(const IAST & ast, bool one_line)
{
    WriteBufferFromOwnString buf;
    formatAST(ast, buf, false, one_line);
    return buf.str();
}

}
