//
// UTFString.h
//
// $Id: //poco/1.4/Foundation/include/Poco/UTFString.h#2 $
//
// Library: Foundation
// Package: Text
// Module:  UTFString
//
// Definitions of strings for UTF encodings.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_UTFString_INCLUDED
#define Foundation_UTFString_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Types.h"
#include <string>


namespace Poco {


struct UTF16CharTraits
{
	typedef std::fpos<mbstate_t> u16streampos;
	typedef UInt16               char_type;
	typedef int                  int_type;
	typedef std::streamoff       off_type;
	typedef u16streampos         pos_type;
	typedef mbstate_t            state_type;

	static void assign(char_type& c1, const char_type& c2)
	{
		c1 = c2;
	}
	
	static bool eq(char_type c1, char_type c2)
	{
		return c1 == c2;
	}
	
	static bool lt(char_type c1, char_type c2)
	{
		return c1 < c2;
	}

	static int compare(const char_type* s1, const char_type* s2, size_t n)
	{
		for (; n; --n, ++s1, ++s2)
		{
			if (lt(*s1, *s2))
				return -1;
			if (lt(*s2, *s1))
				return 1;
		}
		return 0;
	}

	static size_t length(const char_type* s)
	{
		size_t len = 0;
		for (; !eq(*s, char_type(0)); ++s)
			++len;
		return len;
	}

	static const char_type* find(const char_type* s, size_t n, const char_type& a)
	{
		for (; n; --n)
		{
			if (eq(*s, a))
				return s;
			++s;
		}
		return 0;
	}

	static char_type* move(char_type* s1, const char_type* s2, size_t n)
	{
		char_type* r = s1;
		if (s1 < s2)
		{
			for (; n; --n, ++s1, ++s2)
				assign(*s1, *s2);
		}
		else if (s2 < s1)
		{
			s1 += n;
			s2 += n;
			for (; n; --n)
				assign(*--s1, *--s2);
		}
		return r;
	}

	static char_type* copy(char_type* s1, const char_type* s2, size_t n)
	{
		poco_assert(s2 < s1 || s2 >= s1 + n);
		char_type* r = s1;
		for (; n; --n, ++s1, ++s2)
			assign(*s1, *s2);
		return r;
	}

	static char_type* assign(char_type* s, size_t n, char_type a)
	{
		char_type* r = s;
		for (; n; --n, ++s)
			assign(*s, a);
		return r;
	}

	static int_type  not_eof(int_type c)
	{
		return eq_int_type(c, eof()) ? ~eof() : c;
	}
	
	static char_type to_char_type(int_type c)
	{
		return char_type(c);
	}
	
	static int_type to_int_type(char_type c)
	{
		return int_type(c);
	}
	
	static bool eq_int_type(int_type c1, int_type c2)
	{
		return c1 == c2;
	}
	
	static int_type eof()
	{
		return int_type(0xDFFF);
	}
};


struct UTF32CharTraits
{
	typedef std::fpos<mbstate_t> u32streampos;
	typedef UInt32               char_type;
	typedef int                  int_type;
	typedef std::streamoff       off_type;
	typedef u32streampos         pos_type;
	typedef mbstate_t            state_type;

	static void assign(char_type& c1, const char_type& c2)
	{
		c1 = c2;
	}
	
	static bool eq(char_type c1, char_type c2)
	{
		return c1 == c2;
	}
	
	static bool lt(char_type c1, char_type c2)
	{
		return c1 < c2;
	}

	static int compare(const char_type* s1, const char_type* s2, size_t n)
	{
		for (; n; --n, ++s1, ++s2)
		{
			if (lt(*s1, *s2))
				return -1;
			if (lt(*s2, *s1))
				return 1;
		}
		return 0;
	}

	static size_t length(const char_type* s)
	{
		size_t len = 0;
		for (; !eq(*s, char_type(0)); ++s)
			++len;
		return len;
	}

	static const char_type* find(const char_type* s, size_t n, const char_type& a)
	{
		for (; n; --n)
		{
			if (eq(*s, a))
				return s;
			++s;
		}
		return 0;
	}

	static char_type* move(char_type* s1, const char_type* s2, size_t n)
	{
		char_type* r = s1;
		if (s1 < s2)
		{
			for (; n; --n, ++s1, ++s2)
				assign(*s1, *s2);
		}
		else if (s2 < s1)
		{
			s1 += n;
			s2 += n;
			for (; n; --n)
				assign(*--s1, *--s2);
		}
		return r;
	}

	static char_type* copy(char_type* s1, const char_type* s2, size_t n)
	{
		poco_assert(s2 < s1 || s2 >= s1 + n);
		char_type* r = s1;
		for (; n; --n, ++s1, ++s2)
			assign(*s1, *s2);
		return r;
	}

	static char_type* assign(char_type* s, size_t n, char_type a)
	{
		char_type* r = s;
		for (; n; --n, ++s)
			assign(*s, a);
		return r;
	}

	static int_type  not_eof(int_type c)
	{
		return eq_int_type(c, eof()) ? ~eof() : c;
	}
	
	static char_type to_char_type(int_type c)
	{
		return char_type(c);
	}
	
	static int_type to_int_type(char_type c)
	{
		return int_type(c);
	}
	
	static bool eq_int_type(int_type c1, int_type c2)
	{
		return c1 == c2;
	}
	
	static int_type eof()
	{
		return int_type(0xDFFF);
	}
};


//#if defined(POCO_ENABLE_CPP11) //TODO
	//	typedef char16_t       UTF16Char;
	//	typedef std::u16string UTF16String;
	//	typedef char32_t       UTF32Char;
	//	typedef std::u32string UTF32String;
//#else
	#ifdef POCO_NO_WSTRING
		typedef Poco::UInt16                                  UTF16Char;
		typedef std::basic_string<UTF16Char, UTF16CharTraits> UTF16String;
		typedef UInt32                                        UTF32Char;
		typedef std::basic_string<UTF32Char, UTF32CharTraits> UTF32String;
	#else // POCO_NO_WSTRING
		#if defined(POCO_OS_FAMILY_WINDOWS)
			typedef wchar_t                                       UTF16Char;
			typedef std::wstring                                  UTF16String;
			typedef UInt32                                        UTF32Char;
			typedef std::basic_string<UTF32Char, UTF32CharTraits> UTF32String;
		#elif defined(__SIZEOF_WCHAR_T__) //gcc
			#if (__SIZEOF_WCHAR_T__ == 2)
				typedef wchar_t                                       UTF16Char;
				typedef std::wstring                                  UTF16String;
				typedef UInt32                                        UTF32Char;
				typedef std::basic_string<UTF32Char, UTF32CharTraits> UTF32String;
			#elif (__SIZEOF_WCHAR_T__ == 4)
				typedef Poco::UInt16                                  UTF16Char;
				typedef std::basic_string<UTF16Char, UTF16CharTraits> UTF16String;
				typedef wchar_t                                       UTF32Char;
				typedef std::wstring                                  UTF32String;
			#endif
		#else // default to 32-bit wchar_t
			typedef Poco::UInt16                                  UTF16Char;
			typedef std::basic_string<UTF16Char, UTF16CharTraits> UTF16String;
			typedef wchar_t                                       UTF32Char;
			typedef std::wstring                                  UTF32String;
		#endif //POCO_OS_FAMILY_WINDOWS
	#endif //POCO_NO_WSTRING
//#endif // POCO_ENABLE_CPP11


} // namespace Poco


#endif // Foundation_UTFString_INCLUDED
