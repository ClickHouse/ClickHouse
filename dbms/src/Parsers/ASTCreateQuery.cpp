#include <Parsers/ASTCreateQuery.h>
#include <Parsers/IAST.h>

#include <Core/Types.h>

namespace DB
{

void ASTSource::formatImpl(const FormatSettings &settings,
                           FormatState &state,
                           FormatStateStacked frame) const
{

}

String ASTCreateDictionaryQuery::getID() const
{
    return "CreateDictionary_" + dictionary;
}

ASTPtr ASTCreateDictionaryQuery::clone() const
{
    auto res = std::make_shared<ASTCreateDictionaryQuery>(*this);
    res->children.clear();

    // TODO: implement it later
    return res;
}

void ASTCreateDictionaryQuery::formatQueryImpl(const FormatSettings &settings,
                                               FormatState &state,
                                               FormatStateStacked frame) const
{

}

}