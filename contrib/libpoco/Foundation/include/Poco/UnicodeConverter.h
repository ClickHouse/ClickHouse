//
// UnicodeConverter.h
//
// $Id: //poco/1.4/Foundation/include/Poco/UnicodeConverter.h#1 $
//
// Library: Foundation
// Package: Text
// Module:  UnicodeConverter
//
// Definition of the UnicodeConverter class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_UnicodeConverter_INCLUDED
#define Foundation_UnicodeConverter_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/UTFString.h"


namespace Poco {


class Foundation_API UnicodeConverter
	/// A convenience class that converts strings from
	/// UTF-8 encoded std::strings to UTF-16 or UTF-32 encoded std::wstrings
	/// and vice-versa.
	///
	/// This class is mainly used for working with the Unicode Windows APIs
	/// and probably won't be of much use anywhere else ???
{
public:
	static void convert(const std::string& utf8String, UTF32String& utf32String);
		/// Converts the given UTF-8 encoded string into an UTF-32 encoded wide string.

	static void convert(const char* utf8String,  std::size_t length, UTF32String& utf32String);
		/// Converts the given UTF-8 encoded character sequence into an UTF-32 encoded wide string.

	static void convert(const char* utf8String, UTF32String& utf32String);
		/// Converts the given zero-terminated UTF-8 encoded character sequence into an UTF-32 encoded wide string.

	static void convert(const std::string& utf8String, UTF16String& utf16String);
		/// Converts the given UTF-8 encoded string into an UTF-16 encoded wide string.

	static void convert(const char* utf8String,  std::size_t length, UTF16String& utf16String);	
		/// Converts the given UTF-8 encoded character sequence into an UTF-16 encoded wide string.

	static void convert(const char* utf8String, UTF16String& utf16String);	
		/// Converts the given zero-terminated UTF-8 encoded character sequence into an UTF-16 encoded wide string.

	static void convert(const UTF16String& utf16String, std::string& utf8String);
		/// Converts the given UTF-16 encoded wide string into an UTF-8 encoded string.

	static void convert(const UTF32String& utf32String, std::string& utf8String);
		/// Converts the given UTF-32 encoded wide string into an UTF-8 encoded string.

	static void convert(const UTF16Char* utf16String,  std::size_t length, std::string& utf8String);
		/// Converts the given zero-terminated UTF-16 encoded wide character sequence into an UTF-8 encoded string.

	static void convert(const UTF32Char* utf16String, std::size_t length, std::string& utf8String);
		/// Converts the given zero-terminated UTF-32 encoded wide character sequence into an UTF-8 encoded string.

	static void convert(const UTF16Char* utf16String, std::string& utf8String);
		/// Converts the given UTF-16 encoded zero terminated character sequence into an UTF-8 encoded string.

	static void convert(const UTF32Char* utf32String, std::string& utf8String);
		/// Converts the given UTF-32 encoded zero terminated character sequence into an UTF-8 encoded string.

	template <typename F, typename T>
	static void toUTF32(const F& f, T& t)
	{
		convert(f, t);
	}

	template <typename F, typename T>
	static void toUTF32(const F& f, std::size_t l, T& t)
	{
		convert(f, l, t);
	}

	template <typename F, typename T>
	static void toUTF16(const F& f, T& t)
	{
		convert(f, t);
	}

	template <typename F, typename T>
	static void toUTF16(const F& f, std::size_t l, T& t)
	{
		convert(f, l, t);
	}

	template <typename F, typename T>
	static void toUTF8(const F& f, T& t)
	{
		convert(f, t);
	}

	template <typename F, typename T>
	static void toUTF8(const F& f, std::size_t l, T& t)
	{
		convert(f, l, t);
	}

	template <typename T>
	static T to(const char* pChar)
	{
		T utfStr;
		Poco::UnicodeConverter::convert(pChar, utfStr);
		return utfStr;
	}


	template <typename T>
	static T to(const std::string& str)
	{
		T utfStr;
		Poco::UnicodeConverter::convert(str, utfStr);
		return utfStr;
	}

	template <typename T>
	static std::size_t UTFStrlen(const T* ptr)
		/// Returns the length (in characters) of a zero-terminated UTF string.
	{
		if (ptr == 0) return 0;
		const T* p;
		for (p = ptr; *p; ++p);
		return p - ptr;
	}
};


} // namespace Poco


#endif // Foundation_UnicodeConverter_INCLUDED
