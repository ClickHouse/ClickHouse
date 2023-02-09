//
// Parser.h
//
// Library: JSON
// Package: JSON
// Module:  Parser
//
// Definition of the Parser class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef JSON_JSONParser_INCLUDED
#define JSON_JSONParser_INCLUDED


#include "Poco/JSON/JSON.h"
#include "Poco/JSON/ParserImpl.h"
#include "Poco/JSON/Object.h"
#include "Poco/JSON/Array.h"
#include "Poco/JSON/ParseHandler.h"
#include "Poco/JSON/JSONException.h"
#include "Poco/UTF8Encoding.h"
#include "Poco/Dynamic/Var.h"
#include <string>


namespace Poco {
namespace JSON {


class JSON_API Parser: private ParserImpl
	/// A parser for reading RFC 4627 compliant JSON from strings or streams.
	/// 
	/// Simple usage example:
	/// 
	///    std::string json = "{ \"name\" : \"Franky\", \"children\" : [ \"Jonas\", \"Ellen\" ] }";
	///    Parser parser;
	///    Var result = parser.parse(json);
	///    // ... use result (see next example)
	///    parser.reset();
	///    std::ostringstream ostr;
	///    PrintHandler::Ptr pHandler = new PrintHandler(ostr);
	///    parser.setHandler(pHandler);
	///    parser.parse(json); // ostr.str() == json
	/// ----
	///
	/// The result of parsing a valid JSON document will be either an Object
	/// or an Array. Therefore the result of parse() is a Poco::Dynamic::Var
	/// containing a Poco::SharedPtr to an Object or Array instance.
	///
	/// Example:
	/// 
	///    std::string json = "{ \"name\" : \"Franky\", \"children\" : [ \"Jonas\", \"Ellen\" ] }";
	///    Parser parser;
	///    Var result = parser.parse(json);
	///    Object::Ptr object = result.extract<Object::Ptr>();
	///    std::string name = object.getValue<std::string>("name");
	///    Array::Ptr children = object.getArray("children");
	/// ----
{
public:

	Parser(const Handler::Ptr& pHandler = new ParseHandler, std::size_t bufSize = JSON_PARSE_BUFFER_SIZE);
		/// Creates JSON Parser, using the given Handler and buffer size.

	virtual ~Parser();
		/// Destroys JSON Parser.

	void reset();
		/// Resets the parser.

	void setAllowComments(bool comments);
		/// Allow or disallow comments. By default, comments are not allowed.

	bool getAllowComments() const;
		/// Returns true if comments are allowed, false otherwise.
		///
		/// By default, comments are not allowed.
		
	void setAllowNullByte(bool nullByte);
		/// Allow or disallow null byte in strings. 
		///
		/// By default, null byte is allowed.

	bool getAllowNullByte() const;
		/// Returns true if null byte is allowed, false otherwise.
		///
		/// By default, null bytes are allowed.

	void setDepth(std::size_t depth);
		/// Sets the allowed JSON depth.

	std::size_t getDepth() const;
		/// Returns the allowed JSON depth.

	Dynamic::Var parse(const std::string& json);
		/// Parses JSON from a string.

	Dynamic::Var parse(std::istream& in);
		/// Parses JSON from an input stream.

	void setHandler(const Handler::Ptr& pHandler);
		/// Set the Handler.

	const Handler::Ptr& getHandler();
		/// Returns the Handler.

	Dynamic::Var asVar() const;
		/// Returns the result of parsing;

	Dynamic::Var result() const;
		/// Returns the result of parsing as Dynamic::Var;

private:
	Parser(const Parser&);
	Parser& operator = (const Parser&);
};


//
// inlines
//
inline void Parser::setAllowComments(bool comments)
{
	setAllowCommentsImpl(comments);
}


inline bool Parser::getAllowComments() const
{
	return getAllowCommentsImpl();
}


inline void Parser::setAllowNullByte(bool nullByte)
{
	setAllowNullByteImpl(nullByte);
}


inline bool Parser::getAllowNullByte() const
{
	return getAllowNullByteImpl();
}


inline void Parser::setDepth(std::size_t depth)
{
	setDepthImpl(depth);
}


inline std::size_t Parser::getDepth() const
{
	return getDepthImpl();
}


inline const Handler::Ptr& Parser::getHandler()
{
	return getHandlerImpl();
}


inline Dynamic::Var Parser::result() const
{
	return resultImpl();
}


inline Dynamic::Var Parser::asVar() const
{
	return asVarImpl();
}


inline void Parser::reset()
{
	resetImpl();
}


inline Dynamic::Var Parser::parse(const std::string& json)
{
	return parseImpl(json);
}


inline Dynamic::Var Parser::parse(std::istream& in)
{
	return parseImpl(in);
}


} } // namespace Poco::JSON


#endif // JSON_JSONParser_INCLUDED
