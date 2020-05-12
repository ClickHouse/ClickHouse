#pragma once

#if USE_ODBC

#    include <Interpreters/Context.h>
#    include <Poco/Logger.h>
#    include <Poco/Net/HTTPRequestHandler.h>

#    include <Poco/Data/ODBC/Utility.h>

namespace DB
{

std::string getIdentifierQuote(SQLHDBC hdbc);

}

#endif
