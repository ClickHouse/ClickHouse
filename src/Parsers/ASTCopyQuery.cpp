#include <IO/Operators.h>
#include <Parsers/ASTCopyQuery.h>
#include "Common/Exception.h"

namespace DB
{

void ASTCopyQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    ostr << (settings.hilite ? hilite_alias : "");
    if (data)
    {
        const bool prep_whitespace = frame.expression_list_prepend_whitespace;
        frame.expression_list_prepend_whitespace = false;

        ostr << " (";
        data->format(ostr, settings, state, frame);
        ostr << ")";

        frame.expression_list_prepend_whitespace = prep_whitespace;
    }
    ostr << (settings.hilite ? hilite_keyword : "") << " AS" << (settings.hilite ? hilite_none : "");
    ostr << settings.nl_or_ws << indent_str;
    if (file)
    {
        const bool prep_whitespace = frame.expression_list_prepend_whitespace;
        frame.expression_list_prepend_whitespace = false;

        ostr << " (";
        file->format(ostr, settings, state, frame);
        ostr << ")";

        frame.expression_list_prepend_whitespace = prep_whitespace;
    }
}

}
