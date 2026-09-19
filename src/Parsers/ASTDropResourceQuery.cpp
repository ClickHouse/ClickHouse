#include <Parsers/ASTDropResourceQuery.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropResourceQuery::clone() const
{
    return make_intrusive<ASTDropResourceQuery>(*this);
}

void ASTDropResourceQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold the semantic fields kept outside `children`. See the header comment for why the
    /// rewrite-rule matcher needs this. Each field is produced by the formatter, so it survives the
    /// format -> parse round-trip that the debug-build AST consistency check requires.
    hash_state.update(resource_name);
    hash_state.update(if_exists);
    hash_state.update(cluster);
}

void ASTDropResourceQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "DROP RESOURCE ";

    if (if_exists)
        ostr << "IF EXISTS ";

    ostr << backQuoteIfNeed(resource_name);
    formatOnCluster(ostr, settings);
}

}
