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
#include "Poco/StreamCopier.h"
#undef min
#undef max
#include <limits>
#include <clocale>
#include <istream>
#include "pdjson.h"


typedef struct json_stream json_stream;


namespace Poco {
namespace JSON {


ParserImpl::ParserImpl(const Handler::Ptr& pHandler, std::size_t bufSize):
	_pJSON(new json_stream),
	_pHandler(pHandler),
	_depth(JSON_UNLIMITED_DEPTH),
	_decimalPoint('.'),
	_allowNullByte(true),
	_allowComments(false)
{
}


ParserImpl::~ParserImpl()
{
	delete _pJSON;
}


void ParserImpl::handle(const std::string& json)
{
	if (!_allowNullByte && json.find("\\u0000") != json.npos)
		throw JSONException("Null bytes in strings not allowed.");

	try
	{
		json_open_buffer(_pJSON, json.data(), json.size());
		checkError();
		//////////////////////////////////
		// Underlying parser is capable of parsing multiple consecutive JSONs;
		// we do not currently support this feature; to force error on
		// excessive characters past valid JSON end, this MUST be called
		// AFTER opening the buffer - otherwise it is overwritten by
		// json_open*() call, which calls internal init()
		json_set_streaming(_pJSON, false);
		/////////////////////////////////
		handle(); checkError();
		if (JSON_DONE != json_next(_pJSON))
			throw JSONException("Excess characters found after JSON end.");
		json_close(_pJSON);
	}
	catch (std::exception&)
	{
		json_close(_pJSON);
		throw;
	}
}


Dynamic::Var ParserImpl::parseImpl(const std::string& json)
{
	if (_allowComments)
	{
		std::string str = json;
		stripComments(str);
		handle(str);
	}
	else handle(json);

	return asVarImpl();
}


Dynamic::Var ParserImpl::parseImpl(std::istream& in)
{
	std::ostringstream os;
	StreamCopier::copyStream(in, os);
	return parseImpl(os.str());
}


void ParserImpl::stripComments(std::string& json)
{
	if (_allowComments)
	{
		bool inString = false;
		bool inComment = false;
		char prevChar = 0;
		std::string::iterator it = json.begin();
		for (; it != json.end();)
		{
			if (*it == '"' && !inString) inString = true;
			else inString = false;
			if (!inString)
			{
				if (*it == '/' && it + 1 != json.end() && *(it + 1) == '*')
					inComment = true;
			}
			if (inComment)
			{
				char c = *it;
				it = json.erase(it);
				if (prevChar == '*' && c == '/')
				{
					inComment = false;
					prevChar = 0;
				}
				else prevChar = c;
			}
			else ++it;
		}
	}
}


void ParserImpl::handleArray()
{
	json_type tok = json_peek(_pJSON);
	while (tok != JSON_ARRAY_END && checkError())
	{
		handle();
		tok = json_peek(_pJSON);
	}

	if (tok == JSON_ARRAY_END) handle();
	else throw JSONException("JSON array end not found");
}


void ParserImpl::handleObject()
{
	json_type tok = json_peek(_pJSON);
	while (tok != JSON_OBJECT_END && checkError())
	{
		json_next(_pJSON);
		if (_pHandler) _pHandler->key(std::string(json_get_string(_pJSON, NULL)));
		handle();
		tok = json_peek(_pJSON);
	}

	if (tok == JSON_OBJECT_END) handle();
	else throw JSONException("JSON object end not found");
}


void ParserImpl::handle()
{
	enum json_type type = json_next(_pJSON);
	switch (type)
	{
		case JSON_DONE:
			return;
		case JSON_NULL:
			_pHandler->null();
			break;
		case JSON_TRUE:
			if (_pHandler) _pHandler->value(true);
			break;
		case JSON_FALSE:
			if (_pHandler) _pHandler->value(false);
			break;
		case JSON_NUMBER:
		{
			if (_pHandler)
			{
				std::string str(json_get_string(_pJSON, NULL));
				if (str.find(_decimalPoint) != str.npos || str.find('e') != str.npos || str.find('E') != str.npos)
				{
					_pHandler->value(NumberParser::parseFloat(str));
				}
				else
				{
					Poco::Int64 val;
					if (NumberParser::tryParse64(str, val))
						_pHandler->value(val);
					else
						_pHandler->value(NumberParser::parseUnsigned64(str));
				}
			}
			break;
		}
		case JSON_STRING:
			if (_pHandler) _pHandler->value(std::string(json_get_string(_pJSON, NULL)));
			break;
		case JSON_OBJECT:
			if (_pHandler) _pHandler->startObject();
			handleObject();
			break;
		case JSON_OBJECT_END:
			if (_pHandler) _pHandler->endObject();
			return;
		case JSON_ARRAY:
			if (_pHandler) _pHandler->startArray();
			handleArray();
			break;
		case JSON_ARRAY_END:
			if (_pHandler) _pHandler->endArray();
			return;
		case JSON_ERROR:
		{
			const char* pErr = json_get_error(_pJSON);
			std::string err(pErr ? pErr : "JSON parser error.");
			throw JSONException(err);
		}
	}
}


bool ParserImpl::checkError()
{
	const char* err = json_get_error(_pJSON);
	if (err) throw Poco::JSON::JSONException(err);
	return true;
}


} } // namespace Poco::JSON
