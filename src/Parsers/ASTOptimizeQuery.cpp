#include <Parsers/ASTOptimizeQuery.h>
#include <IO/Operators.h>


namespace DB
{

void ASTOptimizeQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "OPTIMIZE TABLE ";

    if (database)
    {
        database->format(ostr, settings, state, frame);
        ostr << '.';
    }

    chassert(table);
    table->format(ostr, settings, state, frame);

    formatOnCluster(ostr, settings);

    if (partition)
    {
        ostr << " PARTITION ";
        partition->format(ostr, settings, state, frame);
    }

    if (dry_run)
    {
        ostr << " DRY RUN";
        if (parts_list)
        {
            ostr << " PARTS ";
            parts_list->format(ostr, settings, state, frame);
        }
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
