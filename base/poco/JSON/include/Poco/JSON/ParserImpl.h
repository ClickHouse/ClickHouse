//
// Parser.h
//
// Library: JSON
// Package: JSON
// Module:  ParserImpl
//
// Definition of the ParserImpl class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef JSON_JSONParserImpl_INCLUDED
#define JSON_JSONParserImpl_INCLUDED


#include "Poco/JSON/JSON.h"
#include "Poco/JSON/Object.h"
#include "Poco/JSON/Array.h"
#include "Poco/JSON/ParseHandler.h"
#include "Poco/JSON/JSONException.h"
#include "Poco/UTF8Encoding.h"
#include "Poco/Dynamic/Var.h"
#include <string>


struct json_stream;


namespace Poco {
namespace JSON {


class JSON_API ParserImpl
{
protected:
	static const std::size_t JSON_PARSE_BUFFER_SIZE = 4096;
	static const std::size_t JSON_PARSER_STACK_SIZE = 128;
	static const int         JSON_UNLIMITED_DEPTH = -1;

	ParserImpl(const Handler::Ptr& pHandler = new ParseHandler, std::size_t bufSize = JSON_PARSE_BUFFER_SIZE);
		/// Creates JSON ParserImpl, using the given Handler and buffer size.

	virtual ~ParserImpl();
		/// Destroys JSON ParserImpl.

	void resetImpl();
		/// Resets the parser.

	void setAllowCommentsImpl(bool comments);
		/// Allow or disallow comments. By default, comments are not allowed.

	bool getAllowCommentsImpl() const;
		/// Returns true if comments are allowed, false otherwise.
		///
		/// By default, comments are not allowed.

	void setAllowNullByteImpl(bool nullByte);
		/// Allow or disallow null byte in strings.
		///
		/// By default, null byte is allowed.

	bool getAllowNullByteImpl() const;
		/// Returns true if null byte is allowed, false otherwise.
		///
		/// By default, null bytes are allowed.

	void setDepthImpl(std::size_t depth);
		/// Sets the allowed JSON depth.

	std::size_t getDepthImpl() const;
		/// Returns the allowed JSON depth.

	Dynamic::Var parseImpl(const std::string& json);
		/// Parses JSON from a string.

	Dynamic::Var parseImpl(std::istream& in);
		/// Parses JSON from an input stream.

	void setHandlerImpl(const Handler::Ptr& pHandler);
		/// Set the Handler.

	const Handler::Ptr& getHandlerImpl();
		/// Returns the Handler.

	Dynamic::Var asVarImpl() const;
		/// Returns the result of parsing;

	Dynamic::Var resultImpl() const;
		/// Returns the result of parsing as Dynamic::Var;

private:
	ParserImpl(const ParserImpl&);
	ParserImpl& operator =(const ParserImpl&);

	void handleArray();
	void handleObject();
	void handle();
	void handle(const std::string& json);
	void stripComments(std::string& json);
	bool checkError();

	struct json_stream* _pJSON;
	Handler::Ptr _pHandler;
	int          _depth;
	char         _decimalPoint;
	bool         _allowNullByte;
	bool         _allowComments;
};


//
// inlines
//

inline void ParserImpl::resetImpl()
{
	// currently, json stream is opened and closed on every parse request
	// (perhaps there is some optimization room?)
	//json_reset(&_json);
	if (_pHandler) _pHandler->reset();
}


inline void ParserImpl::setAllowCommentsImpl(bool comments)
{
	_allowComments = comments;
}


inline bool ParserImpl::getAllowCommentsImpl() const
{
	return _allowComments;
}


inline void ParserImpl::setAllowNullByteImpl(bool nullByte)
{
	_allowNullByte = nullByte;
}


inline bool ParserImpl::getAllowNullByteImpl() const
{
	return _allowNullByte;
}


inline void ParserImpl::setDepthImpl(std::size_t depth)
{
	_depth = static_cast<int>(depth);
}


inline std::size_t ParserImpl::getDepthImpl() const
{
	return static_cast<int>(_depth);
}


inline void ParserImpl::setHandlerImpl(const Handler::Ptr& pHandler)
{
	_pHandler = pHandler;
}


inline const Handler::Ptr& ParserImpl::getHandlerImpl()
{
	return _pHandler;
}


inline Dynamic::Var ParserImpl::resultImpl() const
{
	return asVarImpl();
}


inline Dynamic::Var ParserImpl::asVarImpl() const
{
	if (_pHandler) return _pHandler->asVar();

	return Dynamic::Var();
}


} } // namespace Poco::JSON


#endif // JSON_JSONParserImpl_INCLUDED
