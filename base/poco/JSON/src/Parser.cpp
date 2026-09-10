//
// Parser.cpp
//
// Library: JSON
// Package: JSON
// Module:  Parser
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/JSON/Parser.h"
#include "Poco/JSON/JSONException.h"
#include "Poco/Ascii.h"
#include "Poco/Token.h"
#include "Poco/UTF8Encoding.h"
#include "Poco/String.h"
#undef min
#undef max
#include <limits>
#include <clocale>
#include <istream>


namespace Poco {
namespace JSON {


Parser::Parser(const Handler::Ptr& pHandler, std::size_t bufSize):
	ParserImpl(pHandler, bufSize)
{
}


Parser::~Parser()
{
}


void Parser::setHandler(const Handler::Ptr& pHandler)
{
	setHandlerImpl(pHandler);
}


} } // namespace Poco::JSON
