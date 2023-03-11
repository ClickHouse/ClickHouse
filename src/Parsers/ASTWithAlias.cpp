#include <Parsers/ASTWithAlias.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


namespace DB
{

void ASTWithAlias::formatImpl(const FormattingBuffer & out) const
{
    /// If we have previously output this node elsewhere in the query, now it is enough to output only the alias.
    /// This is needed because the query can become extraordinary large after substitution of aliases.
    if (!alias.empty() && out.insertAlias(alias, getTreeHash()))
    {
        out.writeIdentifier(alias);
    }
    else
    {
        /// If there is an alias, then parentheses are required around the entire expression, including the alias.
        /// Because a record of the form `0 AS x + 0` is syntactically invalid.
        if (out.needsParens() && !alias.empty())
            out.ostr << '(';

        formatImplWithoutAlias(out);

        if (!alias.empty())
        {
            out.writeKeyword(" AS ");
            out.writeAlias(alias);
            if (out.needsParens())
                out.ostr << ')';
        }
    }
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
