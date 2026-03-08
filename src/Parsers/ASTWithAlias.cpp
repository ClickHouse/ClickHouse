#include <Common/SipHash.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTQueryParameter.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <iostream>

namespace DB
{

ASTWithAlias::ASTWithAlias() = default;
ASTWithAlias::~ASTWithAlias() = default;
ASTWithAlias::ASTWithAlias(const ASTWithAlias &) = default;
ASTWithAlias & ASTWithAlias::operator=(const ASTWithAlias &) = default;

static void writeAlias(const String & name, WriteBuffer & ostr, const ASTWithAlias::FormatSettings & settings)
{
    ostr << " AS ";
    settings.writeIdentifier(ostr, name, /*ambiguous=*/false);
}


void ASTWithAlias::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    /// This is needed for distributed queries with the old analyzer. Remove it after removing the old analyzer.
    /// If we have previously output this node elsewhere in the query, now it is enough to output only the alias.
    if (settings.collapse_identical_nodes_to_aliases && !alias.empty() && !state.printed_asts_with_alias.emplace(frame.current_select, alias, getTreeHash(/*ignore_aliases=*/ true)).second)
    {
        settings.writeIdentifier(ostr, alias, /*ambiguous=*/false);
    }
    else
    {
        formatImplWithoutAlias(ostr, settings, state, frame);
        if (!alias.empty())
            writeAlias(alias, ostr, settings);
    }
}

void ASTWithAlias::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    if (!alias.empty() && !ignore_aliases)
        hash_state.update(alias);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTWithAlias::appendColumnName(WriteBuffer & ostr) const
{
    if (preferAliasToColumnName() && !alias.empty())
        writeString(alias, ostr);
    else
        appendColumnNameImpl(ostr);
}

void ASTWithAlias::appendColumnNameWithoutAlias(WriteBuffer & ostr) const
{
    appendColumnNameImpl(ostr);
}

}
