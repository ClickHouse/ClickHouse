#include <Parsers/ASTDropModelQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropModelQuery::clone() const
{
    auto res = std::make_shared<ASTDropModelQuery>(*this);
    res->children.clear();

    res->model_name = model_name->clone();
    res->children.push_back(res->model_name);

    return res;
}

void ASTDropModelQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << "DROP MODEL " << (settings.hilite ? hilite_none : "");
    model_name->format(ostr, settings, state, frame);
}

}
