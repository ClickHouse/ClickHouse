#include <Parsers/ASTSubquery.h>
#include <IO/WriteHelpers.h>

namespace DB
{

void ASTSubquery::appendColumnNameImpl(WriteBuffer & ostr) const
{
    /// This is a hack. We use alias, if available, because otherwise tree could change during analysis.
    if (!alias.empty())
        writeString(alias, ostr);

    Hash hash = getTreeHash();
    writeCString("__subquery_", ostr);
    writeText(hash.first, ostr);
    ostr.write('_');
    writeText(hash.second, ostr);
}

void ASTSubquery::formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');
    std::string nl_or_nothing = settings.one_line ? "" : "\n";

    settings.ostr << nl_or_nothing << indent_str << "(" << nl_or_nothing;
    FormatStateStacked frame_nested = frame;
    frame_nested.need_parens = false;
    ++frame_nested.indent;
    children[0]->formatImpl(settings, state, frame_nested);
    settings.ostr << nl_or_nothing << indent_str << ")";
}

}

