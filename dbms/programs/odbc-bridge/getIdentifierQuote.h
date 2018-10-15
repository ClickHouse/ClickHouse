#pragma once

#include <Common/config.h>
#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequestHandler.h>

#if USE_POCO_SQLODBC || USE_POCO_DATAODBC

#if USE_POCO_SQLODBC
#include <Poco/SQL/ODBC/Utility.h>
#endif
#if USE_POCO_DATAODBC
#include <Poco/Data/ODBC/Utility.h>
#endif

namespace DB
{

std::string getIdentifierQuote(SQLHDBC hdbc);
}
#endif