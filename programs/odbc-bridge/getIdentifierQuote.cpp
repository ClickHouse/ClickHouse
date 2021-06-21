#include "getIdentifierQuote.h"

#if USE_ODBC

#    include <Poco/Data/ODBC/ODBCException.h>
#    include <Poco/Data/ODBC/SessionImpl.h>
#    include <Poco/Data/ODBC/Utility.h>

#    define POCO_SQL_ODBC_CLASS Poco::Data::ODBC


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

std::string getIdentifierQuote(SQLHDBC hdbc)
{
    std::string identifier;

    SQLSMALLINT t;
    SQLRETURN r = POCO_SQL_ODBC_CLASS::SQLGetInfo(hdbc, SQL_IDENTIFIER_QUOTE_CHAR, nullptr, 0, &t);

    if (POCO_SQL_ODBC_CLASS::Utility::isError(r))
        throw POCO_SQL_ODBC_CLASS::ConnectionException(hdbc);

    if (t > 0)
    {
        // I have no idea, why to add '2' here, got from: contrib/poco/Data/ODBC/src/ODBCStatementImpl.cpp:60 (SQL_DRIVER_NAME)
        identifier.resize(static_cast<std::size_t>(t) + 2);

        if (POCO_SQL_ODBC_CLASS::Utility::isError(POCO_SQL_ODBC_CLASS::SQLGetInfo(
                hdbc, SQL_IDENTIFIER_QUOTE_CHAR, &identifier[0], SQLSMALLINT((identifier.length() - 1) * sizeof(identifier[0])), &t)))
            throw POCO_SQL_ODBC_CLASS::ConnectionException(hdbc);

        identifier.resize(static_cast<std::size_t>(t));
    }
    return identifier;
}

IdentifierQuotingStyle getQuotingStyle(SQLHDBC hdbc)
{
    auto identifier_quote = getIdentifierQuote(hdbc);
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
