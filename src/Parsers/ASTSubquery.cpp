#include <Parsers/ASTSubquery.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Common/SipHash.h>

namespace DB
{

void ASTSubquery::appendColumnNameImpl(WriteBuffer & ostr) const
{
    /// This is a hack. We use alias, if available, because otherwise tree could change during analysis.
    if (!alias.empty())
    {
        writeString(alias, ostr);
    }
    else if (!cte_name.empty())
    {
        writeString(cte_name, ostr);
    }
    else
    {
        Hash hash = getTreeHash();
        writeCString("__subquery_", ostr);
        writeText(hash.first, ostr);
        ostr.write('_');
        writeText(hash.second, ostr);
    }
}

void ASTSubquery::formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    /// NOTE: due to trickery of filling cte_name (in interpreters) it is hard
    /// to print it without newline (for !oneline case), since if nl_or_ws
    /// prepended here, then formatting will be incorrect with alias:
    ///
    ///   (select 1 in ((select 1) as sub))
    if (!cte_name.empty())
    {
        settings.ostr << (settings.hilite ? hilite_identifier : "");
        settings.writeIdentifier(cte_name);
        settings.ostr << (settings.hilite ? hilite_none : "");
        return;
    }

    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');
    std::string nl_or_nothing = settings.one_line ? "" : "\n";

    settings.ostr << "(" << nl_or_nothing;
    FormatStateStacked frame_nested = frame;
    frame_nested.need_parens = false;
    ++frame_nested.indent;
    children.front()->formatImpl(settings, state, frame_nested);
    settings.ostr << nl_or_nothing << indent_str << ")";
}

void ASTSubquery::updateTreeHashImpl(SipHash & hash_state) const
{
    if (!cte_name.empty())
        hash_state.update(cte_name);
    IAST::updateTreeHashImpl(hash_state);
}

String ASTSubquery::getAliasOrColumnName() const
{
    if (!alias.empty())
        return alias;
    if (!cte_name.empty())
        return cte_name;
    return getColumnName();
}

String ASTSubquery::tryGetAlias() const
{
    if (!alias.empty())
        return alias;
    return cte_name;
}

}

