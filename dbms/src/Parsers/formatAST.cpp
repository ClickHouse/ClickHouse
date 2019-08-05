#include <Parsers/formatAST.h>

#include <sstream>

namespace DB
{

void formatAST(const IAST & ast, std::ostream & s, bool hilite, bool one_line, bool mask_password)
{
    IAST::FormatSettings settings(s, one_line);
    settings.hilite = hilite;
    settings.mask_password = mask_password;

    ast.format(settings);
}

String serializeAST(const IAST & ast, bool one_line, bool mask_password)
{
    std::stringstream ss;
    formatAST(ast, ss, false, one_line, mask_password);
    return ss.str();
}

}
