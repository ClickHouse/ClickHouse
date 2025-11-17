#include <IO/Operators.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

ASTPtr ASTProjectionDeclaration::clone() const
{
    auto res = std::make_shared<ASTProjectionDeclaration>();
    res->name = name;
    if (query)
        res->set(res->query, query->clone());
    if (options)
        res->set(res->options, options->clone());
    return res;
}


void ASTProjectionDeclaration::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.writeIdentifier(ostr, name, /*ambiguous=*/false);
    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');
    std::string nl_or_nothing = settings.one_line ? "" : "\n";
    ostr << settings.nl_or_ws << indent_str << "(" << nl_or_nothing;
    FormatStateStacked frame_nested = frame;
    frame_nested.need_parens = false;
    ++frame_nested.indent;
    query->format(ostr, settings, state, frame_nested);
    ostr << nl_or_nothing << indent_str << ")";

    if (options)
    {
        ostr << settings.nl_or_ws << indent_str << "OPTIONS (";
        options->format(ostr, settings, state, frame);
        ostr << ")";
    }
}
}
