//
// TextConverter.h
//
// Library: Foundation
// Package: Text
// Module:  TextConverter
//
// Definition of the TextConverter class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_TextConverter_INCLUDED
#define Foundation_TextConverter_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


class TextEncoding;


class Foundation_API TextConverter
	/// A TextConverter converts strings from one encoding
	/// into another.
{
public:
	typedef int (*Transform)(int);
		/// Transform function for convert.
		
	TextConverter(const TextEncoding& inEncoding, const TextEncoding& outEncoding, int defaultChar = '?');
		/// Creates the TextConverter. The encoding objects must not be deleted while the
		/// TextConverter is in use.

	~TextConverter();
		/// Destroys the TextConverter.
		
	int convert(const std::string& source, std::string& destination, Transform trans);
		/// Converts the source string from inEncoding to outEncoding
		/// and appends the result to destination. Every character is
		/// passed to the transform function.
		/// If a character cannot be represented in outEncoding, defaultChar
		/// is used instead.
		/// Returns the number of encoding errors (invalid byte sequences
		/// in source).

	int convert(const void* source, int length, std::string& destination, Transform trans);
		/// Converts the source buffer from inEncoding to outEncoding
		/// and appends the result to destination. Every character is
		/// passed to the transform function.
		/// If a character cannot be represented in outEncoding, defaultChar
		/// is used instead.
		/// Returns the number of encoding errors (invalid byte sequences
		/// in source).

	int convert(const std::string& source, std::string& destination);
		/// Converts the source string from inEncoding to outEncoding
		/// and appends the result to destination.
		/// If a character cannot be represented in outEncoding, defaultChar
		/// is used instead.
		/// Returns the number of encoding errors (invalid byte sequences
		/// in source).

	int convert(const void* source, int length, std::string& destination);
		/// Converts the source buffer from inEncoding to outEncoding
		/// and appends the result to destination.
		/// If a character cannot be represented in outEncoding, defaultChar
		/// is used instead.
		/// Returns the number of encoding errors (invalid byte sequences
		/// in source).

private:
	TextConverter();
	TextConverter(const TextConverter&);
	TextConverter& operator = (const TextConverter&);

	const TextEncoding& _inEncoding;
	const TextEncoding& _outEncoding;
	int                 _defaultChar;
};


} // namespace Poco


#endif // Foundation_TextConverter_INCLUDED
