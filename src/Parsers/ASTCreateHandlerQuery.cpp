#include <Parsers/ASTCreateHandlerQuery.h>
#include <Parsers/ASTSQLSecurity.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Poco/String.h>


namespace DB
{

ASTPtr ASTCreateHandlerQuery::clone() const
{
    auto res = make_intrusive<ASTCreateHandlerQuery>(*this);
    res->children.clear();
    res->query.reset();
    res->sql_security.reset();
    if (query)
        res->set(res->query, query->clone());
    if (sql_security)
        res->set(res->sql_security, sql_security->clone());
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
    {
        /// A protocol literally named "any" must be back-quoted, otherwise it would be parsed back
        /// as the PROTOCOL ANY reset clause.
        if (Poco::icompare(*protocol, "any") == 0)
            ostr << " PROTOCOL " << backQuote(*protocol);
        else
            ostr << " PROTOCOL " << backQuoteIfNeed(*protocol);
    }
    else if (reset_protocol)
    {
        ostr << " PROTOCOL ANY";
    }

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

    if (sql_security && sql_security->as<ASTSQLSecurity &>().type)
    {
        ostr << " ";
        sql_security->format(ostr, settings, state, frame);
    }

    if (query)
    {
        ostr << " AS ";
        query->format(ostr, settings, state, frame);
    }
}

}
