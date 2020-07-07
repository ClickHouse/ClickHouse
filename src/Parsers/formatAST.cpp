#include <Parsers/formatAST.h>

#include <sstream>

namespace DB
{

void formatAST(const IAST & ast, std::ostream & s, bool hilite, bool one_line, bool is_translate)
{
    IAST::FormatSettings settings(s, one_line);
    settings.hilite = hilite;
    settings.is_translate = is_translate;

    ast.format(settings);
}

String serializeAST(const IAST & ast, bool one_line)
{
    std::stringstream ss;
    formatAST(ast, ss, false, one_line);
    return ss.str();
}

}
