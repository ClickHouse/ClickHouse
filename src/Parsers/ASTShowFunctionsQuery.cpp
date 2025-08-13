#include <Parsers/ASTShowFunctionsQuery.h>

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

void ASTShowFunctionsQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW FUNCTIONS" << (settings.hilite ? hilite_none : "");

    if (!like.empty())
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (case_insensitive_like ? " ILIKE " : " LIKE ")
                      << (settings.hilite ? hilite_none : "") << DB::quote << like;
}

}
