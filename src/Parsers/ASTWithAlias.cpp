#include <Common/SipHash.h>
#include <Parsers/ASTWithAlias.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


namespace DB
{

static void writeAlias(const String & name, const ASTWithAlias::FormatSettings & settings)
{
    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " AS " << (settings.hilite ? IAST::hilite_alias : "");
    settings.writeIdentifier(name);
    settings.ostr << (settings.hilite ? IAST::hilite_none : "");
}


void ASTWithAlias::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    /// If we have previously output this node elsewhere in the query, now it is enough to output only the alias.
    /// This is needed because the query can become extraordinary large after substitution of aliases.
    if (!alias.empty() && !state.printed_asts_with_alias.emplace(frame.current_select, alias, getTreeHash()).second)
    {
        settings.writeIdentifier(alias);
    }
    else
    {
        /// If there is an alias, then parentheses are required around the entire expression, including the alias.
        /// Because a record of the form `0 AS x + 0` is syntactically invalid.
        if (frame.need_parens && !alias.empty())
            settings.ostr << '(';

        formatImplWithoutAlias(settings, state, frame);

        if (!alias.empty())
        {
            writeAlias(alias, settings);
            if (frame.need_parens)
                settings.ostr << ')';
        }
    }
}

void ASTWithAlias::updateTreeHashImpl(SipHash & hash_state) const
{
    if (!alias.empty())
    {
        hash_state.update(alias.data(), alias.size());
    }
    IAST::updateTreeHashImpl(hash_state);
}

void ASTWithAlias::appendColumnName(WriteBuffer & ostr) const
{
    if (prefer_alias_to_column_name && !alias.empty())
        writeString(alias, ostr);
    else
        appendColumnNameImpl(ostr);
}

void ASTWithAlias::appendColumnNameWithoutAlias(WriteBuffer & ostr) const
{
    appendColumnNameImpl(ostr);
}

}
