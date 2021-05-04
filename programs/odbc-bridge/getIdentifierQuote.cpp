#include "getIdentifierQuote.h"

#if USE_ODBC

#include <common/logger_useful.h>
#include <nanodbc/nanodbc.h>
#include <sql.h>
#include <sqlext.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


std::string getIdentifierQuote(nanodbc::connection & connection)
{
    std::string quote;
    try
    {
        quote = connection.get_info<std::string>(SQL_IDENTIFIER_QUOTE_CHAR);
    }
    catch (...)
    {
        LOG_WARNING(&Poco::Logger::get("ODBCGetIdentifierQuote"), "Cannot fetch identifier quote. Default double quote is used. Reason: {}", getCurrentExceptionMessage(false));
        return "\"";
    }

    return quote;
}


IdentifierQuotingStyle getQuotingStyle(nanodbc::connection & connection)
{
    auto identifier_quote = getIdentifierQuote(connection);
    if (identifier_quote.length() == 0)
        return IdentifierQuotingStyle::None;
    else if (identifier_quote[0] == '`')
        return IdentifierQuotingStyle::Backticks;
    else if (identifier_quote[0] == '"')
        return IdentifierQuotingStyle::DoubleQuotes;
    else
        throw Exception("Can not map quote identifier '" + identifier_quote + "' to IdentifierQuotingStyle value", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

#endif
