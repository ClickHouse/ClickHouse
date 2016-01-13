//
// Token.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Token.h#1 $
//
// Library: Foundation
// Package: Streams
// Module:  StreamTokenizer
//
// Definition of the Token class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Token_INCLUDED
#define Foundation_Token_INCLUDED


#include "Poco/Foundation.h"
#include <istream>


namespace Poco {


class Foundation_API Token
	/// The base class for all token classes that can be
	/// registered with the StreamTokenizer.
{
public:
	enum Class
	{
		IDENTIFIER_TOKEN,
		KEYWORD_TOKEN,
		SEPARATOR_TOKEN,
		OPERATOR_TOKEN,
		STRING_LITERAL_TOKEN,
		CHAR_LITERAL_TOKEN,
		INTEGER_LITERAL_TOKEN,
		LONG_INTEGER_LITERAL_TOKEN,
		FLOAT_LITERAL_TOKEN,
		DOUBLE_LITERAL_TOKEN,
		COMMENT_TOKEN,
		SPECIAL_COMMENT_TOKEN,
		PREPROCESSOR_TOKEN,
		WHITESPACE_TOKEN,
		EOF_TOKEN,
		INVALID_TOKEN,
		USER_TOKEN
	};
	
	Token();
		/// Creates the Token.

	virtual ~Token();
		/// Destroys the Token.
		
	virtual bool start(char c, std::istream& istr);
		/// Checks if the given character (and, optionally,
		/// the next character in the input stream) start
		/// a valid token. Returns true if so, false
		/// otherwise.
		///
		/// The current read position in istr must not be
		/// changed. In other words, only the peek() method
		/// of istream may be used.
		///
		/// If the character starts the token, it should
		/// be set as the token's value.

	virtual void finish(std::istream& istr);
		/// Builds the token by reading and appending 
		/// the remaining characters from istr.
		
	virtual Class tokenClass() const;
		/// Returns the kind of the token.
	
	const std::string& tokenString() const;
		/// Returns the token's raw string.
	
	virtual std::string asString() const;
		/// Returns a string representation of the token.
	
#if defined(POCO_HAVE_INT64)
	virtual Int64 asInteger64() const;
        /// Returns a 64-bit integer representation of the token.

	virtual UInt64 asUnsignedInteger64() const;
        /// Returns an unsigned 64-bit integer representation of the token.
#endif

	virtual int asInteger() const;
		/// Returns an integer representation of the token.

	virtual unsigned asUnsignedInteger() const;
		/// Returns an unsigned integer representation of the token.

	virtual double asFloat() const;
		/// Returns a floating-point representation of the token.

	virtual char asChar() const;
		/// Returns a char representation of the token.

	bool is(Class tokenClass) const;
		/// Returns true iff the token has the given class.

protected:
	std::string _value;
	
private:
	Token(const Token&);
	Token& operator = (const Token&);
};


class Foundation_API InvalidToken: public Token
	/// This token class is used for signalling that
	/// an invalid character sequence has been encountered
	/// in the input stream.
{
public:
	InvalidToken();
	~InvalidToken();
	Class tokenClass() const;
};


class Foundation_API EOFToken: public Token
	/// This token class is used to signal the
	/// end of the input stream.
{
public:
	EOFToken();
	~EOFToken();
	Class tokenClass() const;
};


class Foundation_API WhitespaceToken: public Token
	/// This pseudo token class is used to eat
	/// up whitespace in between real tokens.
{
public:
	WhitespaceToken();
	~WhitespaceToken();
	Class tokenClass() const;
	bool start(char c, std::istream& istr);
	void finish(std::istream& istr);
};


//
// inlines
//
inline const std::string& Token::tokenString() const
{
	return _value;
}


inline bool Token::is(Token::Class cls) const
{
	return tokenClass() == cls;
}


} // namespace Poco


#endif // Foundation_Token_INCLUDED
