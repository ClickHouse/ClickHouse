
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{

ASTPtr ASTCreateNamedCollectionQuery::clone() const
{
    auto res = std::make_shared<ASTCreateNamedCollectionQuery>(*this);
    res->collection_def = collection_def->clone();
    return res;
}

void ASTCreateNamedCollectionQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE NAMED COLLECTION ";
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(collection_name) << (settings.hilite ? hilite_none : "");

    formatOnCluster(settings);

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");
    collection_def->formatImpl(settings, state, frame);
}

}
