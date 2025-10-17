#include <Parsers/ASTShowFunctionsQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Common/quoteString.h>


namespace DB
{

ASTPtr ASTShowFunctionsQuery::clone() const
{
    auto res = std::make_shared<ASTShowFunctionsQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowFunctionsQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "SHOW FUNCTIONS";
    if (!like.empty())
        ostr << (case_insensitive_like ? " ILIKE " : " LIKE ") << quoteString(like);
}

}
