#pragma once

#if USE_ODBC

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <nanodbc/nanodbc.h>


namespace DB
{

std::string getIdentifierQuote(nanodbc::connection & connection);

IdentifierQuotingStyle getQuotingStyle(nanodbc::connection & connection);

}

#endif
