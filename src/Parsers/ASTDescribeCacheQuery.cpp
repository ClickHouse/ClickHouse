#include <Parsers/ASTDescribeCacheQuery.h>
#include <Common/quoteString.h>


namespace DB
{

String ASTDescribeCacheQuery::getID(char) const { return "DescribeCacheQuery"; }

ASTPtr ASTDescribeCacheQuery::clone() const
{
    auto res = std::make_shared<ASTDescribeCacheQuery>(*this);
    cloneOutputOptions(*res);
    return res;
}

void ASTDescribeCacheQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DESCRIBE FILESYSTEM CACHE" << (settings.hilite ? hilite_none : "")
        << " " << quoteString(cache_name);
}

}
