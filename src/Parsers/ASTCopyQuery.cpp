#include <IO/Operators.h>
#include <Parsers/ASTCopyQuery.h>
#include "Common/Exception.h"

namespace DB
{

void ASTCopyQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    ostr << (settings.hilite ? hilite_alias : "");
    ostr << table_name;
}

ASTPtr ASTCopyQuery::clone() const
{
    auto res = std::make_shared<ASTCopyQuery>(*this);
    res->children.clear();
    return res;
}

}
