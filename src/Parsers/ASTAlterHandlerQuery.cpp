#include <Parsers/ASTAlterHandlerQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTAlterHandlerQuery::clone() const
{
    return make_intrusive<ASTAlterHandlerQuery>(*this);
}

void ASTAlterHandlerQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "ALTER HANDLER ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(handler_name);

    formatOnCluster(ostr, settings);

    if (url)
    {
        ostr << " URL";
        if (url_type && *url_type == HandlerURLType::Prefix)
            ostr << " PREFIX";
        else if (url_type && *url_type == HandlerURLType::Regexp)
            ostr << " REGEXP";
        ostr << " " << quoteString(*url);
    }

    if (methods)
    {
        ostr << " METHODS (";
        for (size_t i = 0; i < methods->size(); ++i)
        {
            if (i > 0)
                ostr << ", ";
            ostr << (*methods)[i];
        }
        ostr << ")";
    }

    if (query)
        ostr << " AS " << quoteString(*query);
}

}
