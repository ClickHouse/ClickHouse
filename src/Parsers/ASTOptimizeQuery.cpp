#include <Parsers/ASTOptimizeQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

void ASTOptimizeQuery::formatQueryImpl(FormattingBuffer out) const
{
    out.writeKeyword("OPTIMIZE TABLE ");
    out.ostr << (database ? backQuoteIfNeed(getDatabase()) + "." : "") << backQuoteIfNeed(getTable());

    formatOnCluster(out);

    if (partition)
    {
        out.writeKeyword(" PARTITION ");
        partition->formatImpl(out);
    }

    if (final)
        out.writeKeyword(" FINAL");

    if (deduplicate)
        out.writeKeyword(" DEDUPLICATE");

    if (cleanup)
        out.writeKeyword(" CLEANUP");

    if (deduplicate_by_columns)
    {
        out.writeKeyword(" BY ");
        deduplicate_by_columns->formatImpl(out);
    }
}

}
