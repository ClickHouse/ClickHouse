#include <Parsers/ASTDropWorkloadQuery.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropWorkloadQuery::clone() const
{
    return make_intrusive<ASTDropWorkloadQuery>(*this);
}

void ASTDropWorkloadQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold the semantic fields kept outside `children`. See the header comment for why the
    /// rewrite-rule matcher needs this. Each field is produced by the formatter, so it survives the
    /// format -> parse round-trip that the debug-build AST consistency check requires.
    hash_state.update(workload_name);
    hash_state.update(if_exists);
    hash_state.update(cluster);
}

void ASTDropWorkloadQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "DROP WORKLOAD ";

    if (if_exists)
        ostr << "IF EXISTS ";

    ostr << backQuoteIfNeed(workload_name);
    formatOnCluster(ostr, settings);
}

}
