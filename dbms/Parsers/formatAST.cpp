#include <Parsers/formatAST.h>

#include <sstream>

namespace DB
{

void formatAST(const IAST & ast, std::ostream & s, bool hilite, bool one_line)
{
    IAST::FormatSettings settings(s, one_line);
    settings.hilite = hilite;

    ast.format(settings);
}

String serializeAST(const IAST & ast, bool one_line)
{
    std::stringstream ss;
    formatAST(ast, ss, false, one_line);
    return ss.str();
}

}
