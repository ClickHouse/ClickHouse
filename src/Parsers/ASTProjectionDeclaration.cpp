#include <Parsers/ASTProjectionDeclaration.h>

#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

ASTPtr ASTProjectionDeclaration::clone() const
{
    auto res = make_intrusive<ASTProjectionDeclaration>();
    res->name = name;
    if (query)
        res->set(res->query, query->clone());
    if (index)
        res->set(res->index, index->clone());
    if (type)
        res->set(res->type, type->clone());
    if (with_settings)
        res->set(res->with_settings, with_settings->clone());
    return res;
}


void ASTProjectionDeclaration::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.writeIdentifier(ostr, name, /*ambiguous=*/false);
    if (query)
    {
        std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');
        std::string nl_or_nothing = settings.one_line ? "" : "\n";
        ostr << settings.nl_or_ws << indent_str << "(" << nl_or_nothing;
        FormatStateStacked frame_nested = frame;
        frame_nested.need_parens = false;
        ++frame_nested.indent;
        query->format(ostr, settings, state, frame_nested);
        ostr << nl_or_nothing << indent_str << ")";
    }

    if (index)
    {
        ostr << " INDEX ";
        index->format(ostr, settings, state, frame);
    }

    if (type)
    {
        ostr << " TYPE ";
        type->format(ostr, settings, state, frame);
    }

    if (with_settings)
    {
        ostr << " WITH SETTINGS (";
        with_settings->format(ostr, settings, state, frame);
        ostr << ")";
    }
}
}
