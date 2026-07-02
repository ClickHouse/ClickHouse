#include <Parsers/ASTCreateHandlerQuery.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTCreateHandlerQuery::clone() const
{
    auto res = make_intrusive<ASTCreateHandlerQuery>(*this);
    res->children.clear();
    if (query)
    {
        res->query = query->clone();
        res->children.push_back(res->query);
    }
    return res;
}

void ASTCreateHandlerQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    ostr << (is_alter ? "ALTER HANDLER " : "CREATE HANDLER ");
    if (if_not_exists)
        ostr << "IF NOT EXISTS ";
    ostr << backQuoteIfNeed(handler_name);

    formatOnCluster(ostr, settings);

    if (protocol)
        ostr << " PROTOCOL " << backQuoteIfNeed(*protocol);

    if (has_url)
    {
        ostr << " URL ";
        switch (url_match_type)
        {
            case URLMatchType::Exact: break;
            case URLMatchType::Prefix: ostr << "PREFIX "; break;
            case URLMatchType::Regexp: ostr << "REGEXP "; break;
        }
        ostr << quoteString(url);
    }

    if (methods)
    {
        ostr << " METHODS (";
        bool first = true;
        for (const auto & method : *methods)
        {
            if (!first)
                ostr << ", ";
            first = false;
            ostr << method;
        }
        ostr << ")";
    }

    if (handler_type)
        ostr << " TYPE " << *handler_type;

    if (query)
    {
        ostr << " AS ";
        query->format(ostr, settings, state, frame);
    }
}

}
