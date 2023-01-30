#include <IO/Operators.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Common/quoteString.h>

namespace DB
{

ASTPtr ASTProjectionDeclaration::clone() const
{
    auto res = std::make_shared<ASTProjectionDeclaration>();
    res->name = name;
    if (query)
        res->set(res->query, query->clone());
    return res;
}


void ASTProjectionDeclaration::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << backQuoteIfNeed(name);
    std::string indent_str = settings.isOneLine() ? "" : std::string(4u * frame.indent, ' ');
    settings.nlOrWs();
    settings.ostr << indent_str << "(";
    settings.nlOrNothing();
    FormatStateStacked frame_nested = frame;
    frame_nested.need_parens = false;
    ++frame_nested.indent;
    query->formatImpl(settings, state, frame_nested);
    settings.nlOrNothing();
    settings.ostr << indent_str << ")";
}

}
