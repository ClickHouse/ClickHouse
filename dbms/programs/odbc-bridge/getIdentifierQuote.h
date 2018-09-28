#pragma once

#include <Common/config.h>
#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <sqltypes.h>

#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
namespace DB
{

    std::string getIdentifierQuote(SQLHDBC hdbc);

}
#endif