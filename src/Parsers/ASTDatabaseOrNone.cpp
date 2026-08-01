#include <Parsers/ASTDatabaseOrNone.h>
#include <Parsers/CommonParsers.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>

namespace DB
{
void ASTDatabaseOrNone::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    if (none)
    {
        ostr << "NONE";
        return;
    }

    /// A database literally named `NONE` (case-insensitive) must be quoted: emitted as a bare
    /// identifier it would be reparsed by `ParserDatabaseOrNone` as the `NONE` keyword (meaning
    /// "no default database"). That is wrong on replay (e.g. `SHOW CREATE USER`) and breaks the
    /// format -> parse round-trip that the debug-build AST-consistency check requires now that
    /// `updateTreeHashImpl` folds `database_name`.
    if (equalsCaseInsensitive(database_name, toStringView(Keyword::NONE)))
        ostr << backQuote(database_name);
    else
        ostr << backQuoteIfNeed(database_name);
}

void ASTDatabaseOrNone::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Both fields are produced by the formatter (`NONE` / the quoted database name), so they
    /// survive the format -> parse round-trip that the debug-build AST consistency check requires.
    hash_state.update(none);
    hash_state.update(database_name);
}

}
