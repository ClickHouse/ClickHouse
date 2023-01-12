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
        if (!alias.empty())
        {
            if (settings.serialize_force_alias && force_alias)
                settings.ostr << "__columnWithAliasName(";
            else if (frame.need_parens)
                settings.ostr << '(';
        }

        formatImplWithoutAlias(settings, state, frame);

        if (!alias.empty())
        {
            writeAlias(alias, settings);
            if (frame.need_parens || (settings.serialize_force_alias && force_alias))
                settings.ostr << ')';
        }
    }
}

void ASTWithAlias::appendColumnName(WriteBuffer & ostr, bool prefer_alias) const
{
    if (!alias.empty() && ((prefer_alias && prefer_alias_to_column_name) || force_alias))
        writeString(alias, ostr);
    else
        appendColumnNameImpl(ostr, prefer_alias);
}

}
