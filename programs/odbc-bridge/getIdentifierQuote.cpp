#include "getIdentifierQuote.h"

#if USE_ODBC

#include <Common/logger_useful.h>
#include <sql.h>
#include <sqlext.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


std::string getIdentifierQuote(nanodbc::ConnectionHolderPtr connection_holder)
{
    std::string quote;
    try
    {
        quote = execute<std::string>(connection_holder,
                    [&](nanodbc::connection & connection) { return connection.get_info<std::string>(SQL_IDENTIFIER_QUOTE_CHAR); });
    }
    catch (...)
    {
        LOG_WARNING(getLogger("ODBCGetIdentifierQuote"), "Cannot fetch identifier quote. Default double quote is used. Reason: {}", getCurrentExceptionMessage(false));
        return "\"";
    }

    return quote;
}


IdentifierQuotingStyle getQuotingStyle(nanodbc::ConnectionHolderPtr connection)
{
    auto identifier_quote = getIdentifierQuote(connection);
    if (identifier_quote.empty())
        return IdentifierQuotingStyle::Backticks;
    if (identifier_quote[0] == '`')
        return IdentifierQuotingStyle::Backticks;
    if (identifier_quote[0] == '"')
        return IdentifierQuotingStyle::DoubleQuotes;

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Can not map quote identifier '{}' to IdentifierQuotingStyle value", identifier_quote);
}

}

#endif
