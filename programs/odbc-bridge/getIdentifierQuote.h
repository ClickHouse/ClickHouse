#pragma once

#if USE_ODBC

#    include <Interpreters/Context.h>
#    include <Poco/Logger.h>
#    include <Poco/Net/HTTPRequestHandler.h>

#    include <Poco/Data/ODBC/Utility.h>

#include <Parsers/IdentifierQuotingStyle.h>

namespace DB
{

std::string getIdentifierQuote(SQLHDBC hdbc);

IdentifierQuotingStyle getQuotingStyle(SQLHDBC hdbc);

}

#endif
