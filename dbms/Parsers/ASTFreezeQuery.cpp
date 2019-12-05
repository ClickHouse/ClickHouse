#include <Parsers/ASTFreezeQuery.h>
#include <Common/quoteString.h>

#include <iomanip>


namespace DB
{
void ASTFreezeQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "FREEZE TABLE " << (settings.hilite ? hilite_none : "")
                  << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);

    if (partition)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " PARTITION " << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
    }

    if (!with_name.empty())
    {
        settings.ostr << " " << (settings.hilite ? hilite_keyword : "") << "WITH NAME" << (settings.hilite ? hilite_none : "")
                      << " " << std::quoted(with_name, '\'');
    }
}

}
