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

}
