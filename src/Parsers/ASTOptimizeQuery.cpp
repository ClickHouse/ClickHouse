#include <Parsers/ASTOptimizeQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

void ASTOptimizeQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.writeKeyword("OPTIMIZE TABLE ");
    settings.ostr << (database ? backQuoteIfNeed(getDatabase()) + "." : "") << backQuoteIfNeed(getTable());

    formatOnCluster(settings);

    if (partition)
    {
        settings.writeKeyword(" PARTITION ");
        partition->formatImpl(settings, state, frame);
    }

    if (final)
        settings.writeKeyword(" FINAL");

    if (deduplicate)
        settings.writeKeyword(" DEDUPLICATE");

    if (deduplicate_by_columns)
    {
        settings.writeKeyword(" BY ");
        deduplicate_by_columns->formatImpl(settings, state, frame);
    }
}

}
