#include <Parsers/ASTOptimizeQuery.h>
#include <IO/Operators.h>


namespace DB
{

void ASTOptimizeQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "OPTIMIZE TABLE ";

    if (auto db = getDatabaseAst())
    {
        db->format(ostr, settings, state, frame);
        ostr << '.';
    }

    auto tbl = getTableAst();
    chassert(tbl);
    tbl->format(ostr, settings, state, frame);

    formatOnCluster(ostr, settings);

    if (partition)
    {
        ostr << " PARTITION ";
        partition->format(ostr, settings, state, frame);
    }

    if (final)
        ostr << " FINAL";

    if (deduplicate)
        ostr << " DEDUPLICATE";

    if (cleanup)
        ostr << " CLEANUP";

    if (deduplicate_by_columns)
    {
        ostr << " BY ";
        deduplicate_by_columns->format(ostr, settings, state, frame);
    }
}

}
