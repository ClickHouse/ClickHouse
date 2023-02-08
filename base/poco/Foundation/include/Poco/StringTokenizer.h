//
// StringTokenizer.h
//
// Library: Foundation
// Package: Core
// Module:  StringTokenizer
//
// Definition of the StringTokenizer class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_StringTokenizer_INCLUDED
#define Foundation_StringTokenizer_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Exception.h"
#include <vector>
#include <cstddef>


namespace Poco {


class Foundation_API StringTokenizer
	/// A simple tokenizer that splits a string into
	/// tokens, which are separated by separator characters.
	/// An iterator is used to iterate over all tokens.
{
public:
	enum Options
	{
		TOK_IGNORE_EMPTY = 1, /// ignore empty tokens
		TOK_TRIM	 = 2  /// remove leading and trailing whitespace from tokens
	};
	
	typedef std::vector<std::string> TokenVec;
	typedef TokenVec::const_iterator Iterator;
	
	StringTokenizer(const std::string& str, const std::string& separators, int options = 0);
		/// Splits the given string into tokens. The tokens are expected to be
		/// separated by one of the separator characters given in separators.
		/// Additionally, options can be specified:
		///   * TOK_IGNORE_EMPTY: empty tokens are ignored
		///   * TOK_TRIM: trailing and leading whitespace is removed from tokens.

	~StringTokenizer();
		/// Destroys the tokenizer.
	
	Iterator begin() const;
	Iterator end() const;
	
	const std::string& operator [] (std::size_t index) const;
		/// Returns const reference the index'th token.
		/// Throws a RangeException if the index is out of range.

	std::string& operator [] (std::size_t index);
		/// Returns reference to the index'th token.
		/// Throws a RangeException if the index is out of range.

	bool has(const std::string& token) const;
		/// Returns true if token exists, false otherwise.

	std::string::size_type find(const std::string& token, std::string::size_type pos = 0) const;
		/// Returns the index of the first occurence of the token
		/// starting at position pos.
		/// Throws a NotFoundException if the token is not found.

	std::size_t replace(const std::string& oldToken, const std::string& newToken, std::string::size_type pos = 0);
		/// Starting at position pos, replaces all subsequent tokens having value 
		/// equal to oldToken with newToken.
		/// Returns the number of modified tokens.
		
	std::size_t count() const;
		/// Returns the total number of tokens.

	std::size_t count(const std::string& token) const;
		/// Returns the number of tokens equal to the specified token.

private:
	StringTokenizer(const StringTokenizer&);
	StringTokenizer& operator = (const StringTokenizer&);
	
	void trim(std::string& token);

	TokenVec _tokens;
};


//
// inlines
//


inline StringTokenizer::Iterator StringTokenizer::begin() const
{
	return _tokens.begin();
}


inline StringTokenizer::Iterator StringTokenizer::end() const
{
	return _tokens.end();
}


inline std::string& StringTokenizer::operator [] (std::size_t index)
{
	if (index >= _tokens.size()) throw RangeException();
	return _tokens[index];
}


inline const std::string& StringTokenizer::operator [] (std::size_t index) const
{
	if (index >= _tokens.size()) throw RangeException();
	return _tokens[index];
}


inline std::size_t StringTokenizer::count() const
{
	return _tokens.size();
}


} // namespace Poco


#endif // Foundation_StringTokenizer_INCLUDED
