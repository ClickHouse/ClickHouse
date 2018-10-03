#include "getIdentifierQuote.h"
#if USE_POCO_SQLODBC || USE_POCO_DATAODBC

#if USE_POCO_SQLODBC
#include <Poco/SQL/ODBC/ODBCException.h>
#include <Poco/SQL/ODBC/SessionImpl.h>
#include <Poco/SQL/ODBC/Utility.h>
#define POCO_SQL_ODBC_CLASS Poco::SQL::ODBC
#endif
#if USE_POCO_DATAODBC
#include <Poco/Data/ODBC/ODBCException.h>
#include <Poco/Data/ODBC/SessionImpl.h>
#include <Poco/Data/ODBC/Utility.h>
#define POCO_SQL_ODBC_CLASS Poco::Data::ODBC
#endif


namespace DB
{
std::string getIdentifierQuote(SQLHDBC hdbc)
{
    std::string identifier;

    SQLSMALLINT t;
    SQLRETURN r = POCO_SQL_ODBC_CLASS::SQLGetInfo(hdbc, SQL_IDENTIFIER_QUOTE_CHAR, NULL, 0, &t);

    if (POCO_SQL_ODBC_CLASS::Utility::isError(r))
        throw POCO_SQL_ODBC_CLASS::ConnectionException(hdbc);

    if (t > 0)
    {
        // I have not idea, why to add '2' here, got from: contrib/poco/Data/ODBC/src/ODBCStatementImpl.cpp:60 (SQL_DRIVER_NAME)
        identifier.resize(static_cast<std::size_t>(t) + 2);

        if (POCO_SQL_ODBC_CLASS::Utility::isError(POCO_SQL_ODBC_CLASS::SQLGetInfo(
                hdbc, SQL_IDENTIFIER_QUOTE_CHAR, &identifier[0], SQLSMALLINT((identifier.length() - 1) * sizeof(identifier[0])), &t)))
            throw POCO_SQL_ODBC_CLASS::ConnectionException(hdbc);

        identifier.resize(static_cast<std::size_t>(t));
    }
    return identifier;
}
}
#endif