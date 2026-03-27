#include <Parsers/ASTCreateHandlerQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTCreateHandlerQuery::clone() const
{
    return make_intrusive<ASTCreateHandlerQuery>(*this);
}

void ASTCreateHandlerQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "CREATE HANDLER ";
    if (if_not_exists)
        ostr << "IF NOT EXISTS ";
    ostr << backQuoteIfNeed(handler_name);

    formatOnCluster(ostr, settings);

    ostr << " URL";
    if (url_type == HandlerURLType::Prefix)
        ostr << " PREFIX";
    else if (url_type == HandlerURLType::Regexp)
        ostr << " REGEXP";
    ostr << " " << quoteString(url);

    if (!methods.empty())
    {
        ostr << " METHODS (";
        for (size_t i = 0; i < methods.size(); ++i)
        {
            if (i > 0)
                ostr << ", ";
            ostr << methods[i];
        }
        ostr << ")";
    }

    ostr << " AS " << quoteString(query);
}

}
