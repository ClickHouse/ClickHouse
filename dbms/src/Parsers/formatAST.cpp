#include <Parsers/formatAST.h>

#include <sstream>

namespace DB
{

void formatAST(const IAST & ast, std::ostream & s, size_t indent, bool hilite, bool one_line)
{
    IAST::FormatSettings settings(s, hilite, one_line);
    ast.format(settings);
}


String formatColumnsForCreateQuery(NamesAndTypesList & columns)
{
    std::string res;
    res += "(";
    for (NamesAndTypesList::iterator it = columns.begin(); it != columns.end(); ++it)
    {
        if (it != columns.begin())
            res += ", ";
        res += backQuoteIfNeed(it->name);
        res += " ";
        res += it->type->getName();
    }
    res += ")";
    return res;
}

}
