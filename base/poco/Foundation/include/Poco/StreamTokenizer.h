//
// StreamTokenizer.h
//
// Library: Foundation
// Package: Streams
// Module:  StreamTokenizer
//
// Definition of the StreamTokenizer class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_StreamTokenizer_INCLUDED
#define Foundation_StreamTokenizer_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Token.h"
#include <istream>
#include <vector>


namespace Poco {


class Foundation_API StreamTokenizer
	/// A stream tokenizer splits an input stream
	/// into a sequence of tokens of different kinds.
	/// Various token kinds can be registered with
	/// the tokenizer.
{
public:
	StreamTokenizer();
		/// Creates a StreamTokenizer with no attached stream.

	StreamTokenizer(std::istream& istr);
		/// Creates a StreamTokenizer with no attached stream.

	virtual ~StreamTokenizer();
		/// Destroys the StreamTokenizer and deletes all
		/// registered tokens.

	void attachToStream(std::istream& istr);
		/// Attaches the tokenizer to an input stream.

	void addToken(Token* pToken);
		/// Adds a token class to the tokenizer. The
		/// tokenizer takes ownership of the token and
		/// deletes it when no longer needed. Comment
		/// and whitespace tokens will be marked as
		/// ignorable, which means that next() will not
		/// return them.
	
	void addToken(Token* pToken, bool ignore);
		/// Adds a token class to the tokenizer. The
		/// tokenizer takes ownership of the token and
		/// deletes it when no longer needed.
		/// If ignore is true, the token will be marked
		/// as ignorable, which means that next() will
		/// not return it.
		
	const Token* next();
		/// Extracts the next token from the input stream.
		/// Returns a pointer to an EOFToken if there are
		/// no more characters to read. 
		/// Returns a pointer to an InvalidToken if an
		/// invalid character is encountered.
		/// If a token is marked as ignorable, it will not
		/// be returned, and the next token will be
		/// examined.
		/// Never returns a NULL pointer.
		/// You must not delete the token returned by next().

private:
	struct TokenInfo
	{
		Token* pToken;
		bool   ignore;
	};
	
	typedef std::vector<TokenInfo> TokenVec;
	
	TokenVec      _tokens;
	std::istream* _pIstr;
	InvalidToken  _invalidToken;
	EOFToken      _eofToken;
};


} // namespace Poco


#endif // Foundation_StreamTokenizer_INCLUDED
