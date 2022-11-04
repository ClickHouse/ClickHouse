#pragma once

#include <Common/config.h>

#if USE_ODBC

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include "ODBCConnectionFactory.h"


namespace DB
{

std::string getIdentifierQuote(nanodbc::ConnectionHolderPtr connection_holder);
IdentifierQuotingStyle getQuotingStyle(nanodbc::ConnectionHolderPtr connection);

}

#endif
