#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTAlterNamedCollectionQuery::clone() const
{
    auto res = std::make_shared<ASTAlterNamedCollectionQuery>(*this);
    res->changes = changes->clone();
    return res;
}

void ASTAlterNamedCollectionQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "Alter NAMED COLLECTION ";
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(collection_name) << (settings.hilite ? hilite_none : "");
    formatOnCluster(settings);
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " SET " << (settings.hilite ? hilite_none : "");
    changes->formatImpl(settings, state, frame);
}

}
