#include <Parsers/ASTDescribeCacheQuery.h>
#include <Common/quoteString.h>


namespace DB
{

String ASTDescribeCacheQuery::getID(char) const { return "DescribeCacheQuery"; }

ASTPtr ASTDescribeCacheQuery::clone() const
{
    auto res = make_intrusive<ASTDescribeCacheQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTDescribeCacheQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "DESCRIBE FILESYSTEM CACHE" << " " << quoteString(cache_name);
}

}
