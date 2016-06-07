//
// NumericString.h
//
// $Id: //poco/1.4/Foundation/include/Poco/NumericString.h#1 $
//
// Library: Foundation
// Package: Core
// Module:  NumericString
//
// Numeric string utility functions.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_NumericString_INCLUDED
#define Foundation_NumericString_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Buffer.h"
#include "Poco/FPEnvironment.h"
#ifdef min
	#undef min
#endif
#ifdef max
	#undef max
#endif
#include <limits>
#include <cmath>
#if !defined(POCO_NO_LOCALE)
	#include <locale>
#endif

// binary numbers are supported, thus 64 (bits) + 1 (string terminating zero)
#define POCO_MAX_INT_STRING_LEN 65
// value from strtod.cc (double_conversion::kMaxSignificantDecimalDigits)
#define POCO_MAX_FLT_STRING_LEN 780

#define POCO_FLT_INF "inf"
#define POCO_FLT_NAN "nan"
#define POCO_FLT_EXP 'e'


namespace Poco {


inline char decimalSeparator()
	/// Returns decimal separator from global locale or
	/// default '.' for platforms where locale is unavailable.
{
#if !defined(POCO_NO_LOCALE)
	return std::use_facet<std::numpunct<char> >(std::locale()).decimal_point();
#else
	return '.';
#endif
}


inline char thousandSeparator()
	/// Returns thousand separator from global locale or
	/// default ',' for platforms where locale is unavailable.
{
#if !defined(POCO_NO_LOCALE)
	return std::use_facet<std::numpunct<char> >(std::locale()).thousands_sep();
#else
	return ',';
#endif
}


//
// String to Number Conversions
//

template <typename I>
bool strToInt(const char* pStr, I& result, short base, char thSep = ',')
	/// Converts zero-terminated character array to integer number;
	/// Thousand separators are recognized for base10 and current locale;
	/// it is silently skipped but not verified for correct positioning.
	/// Function returns true if succesful. If parsing was unsuccesful,
	/// the return value is false with the result value undetermined.
{
	if (!pStr) return false;
	while (isspace(*pStr)) ++pStr;
	if (*pStr == '\0') return false;
	short sign = 1;
	if ((base == 10) && (*pStr == '-'))
	{
		// Unsigned types can't be negative so abort parsing
		if (std::numeric_limits<I>::min() >= 0) return false;
		sign = -1;
		++pStr;
	}
	else if (*pStr == '+') ++pStr;

	// parser states:
	const char STATE_SIGNIFICANT_DIGITS = 1;
	char state = 0;
	
	result = 0;
	I limitCheck = std::numeric_limits<I>::max() / base;
	for (; *pStr != '\0'; ++pStr)
	{
		switch (*pStr)
		{
		case 'x': case 'X': 
			if (base != 0x10) return false;

		case '0': 
			if (state < STATE_SIGNIFICANT_DIGITS) break;

		case '1': case '2': case '3': case '4':
		case '5': case '6': case '7':
			if (state < STATE_SIGNIFICANT_DIGITS) state = STATE_SIGNIFICANT_DIGITS;
			if (result > limitCheck) return false;
			result = result * base + (*pStr - '0');

			break;

		case '8': case '9':
			if ((base == 10) || (base == 0x10))
			{
				if (state < STATE_SIGNIFICANT_DIGITS) state = STATE_SIGNIFICANT_DIGITS;
				if (result > limitCheck) return false;
				result = result * base + (*pStr - '0');
			}
			else return false;

			break;

		case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
			if (base != 0x10) return false;
			if (state < STATE_SIGNIFICANT_DIGITS) state = STATE_SIGNIFICANT_DIGITS;
			if (result > limitCheck) return false;
			result = result * base + (10 + *pStr - 'a');

			break;

		case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
			if (base != 0x10) return false;

			if (state < STATE_SIGNIFICANT_DIGITS) state = STATE_SIGNIFICANT_DIGITS;
			if (result > limitCheck) return false;
			result = result * base + (10 + *pStr - 'A');

			break;

		case 'U':
		case 'u':
		case 'L':
		case 'l':
			goto done;

		case '.':
			if ((base == 10) && (thSep == '.')) break;
			else return false;

		case ',':
			if ((base == 10) && (thSep == ',')) break;
			else return false;

		case ' ':
			if ((base == 10) && (thSep == ' ')) break;
		case '\t':
		case '\n':
		case '\v':
		case '\f':
		case '\r':
			goto done;

		default:
			return false;
		}
	}

done:
	if ((sign < 0) && (base == 10)) result *= sign;

	return true;
}


template <typename I>
bool strToInt(const std::string& str, I& result, short base, char thSep = ',')
	/// Converts string to integer number;
	/// This is a wrapper function, for details see see the
	/// bool strToInt(const char*, I&, short, char) implementation.
{
	return strToInt(str.c_str(), result, base, thSep);
}


//
// Number to String Conversions
//

namespace Impl {

	class Ptr
		/// Utility char pointer wrapper class.
		/// Class ensures increment/decrement remain within boundaries.
	{
	public:
		Ptr(char* ptr, std::size_t offset): _beg(ptr), _cur(ptr), _end(ptr + offset)
		{
		}
	
		char*& operator ++ () // prefix
		{
			check(_cur + 1);
			return ++_cur;
		}

		char* operator ++ (int) // postfix
		{
			check(_cur + 1);
			char* tmp = _cur++;
			return tmp;
		}
	
		char*& operator -- () // prefix
		{
			check(_cur - 1);
			return --_cur;
		}

		char* operator -- (int) // postfix
		{
			check(_cur - 1);
			char* tmp = _cur--;
			return tmp;
		}

		char*& operator += (int incr)
		{
			check(_cur + incr);
			return _cur += incr;
		}

		char*& operator -= (int decr)
		{
			check(_cur - decr);
			return _cur -= decr;
		}

		operator char* () const
		{
			return _cur;
		}

		std::size_t span() const
		{
			return _end - _beg;
		}

	private:
		void check(char* ptr)
		{
			if (ptr > _end) throw RangeException();
		}

		const char* _beg;
		char*       _cur;
		const char* _end;
};	

} // namespace Impl


template <typename T>
bool intToStr(T value,
	unsigned short base,
	char* result,
	std::size_t& size,
	bool prefix = false,
	int width = -1,
	char fill = ' ',
	char thSep = 0)
	/// Converts integer to string. Numeric bases from binary to hexadecimal are supported.
	/// If width is non-zero, it pads the return value with fill character to the specified width.
	/// When padding is zero character ('0'), it is prepended to the number itself; all other
	/// paddings are prepended to the formatted result with minus sign or base prefix included
	/// If prefix is true and base is octal or hexadecimal, respective prefix ('0' for octal, 
	/// "0x" for hexadecimal) is prepended. For all other bases, prefix argument is ignored.
	/// Formatted string has at least [width] total length.
{
	if (base < 2 || base > 0x10)
	{
		*result = '\0';
		return false;
	}

	Impl::Ptr ptr(result, size);
	int thCount = 0;
	T tmpVal;
	do
	{
		tmpVal = value;
		value /= base;
		*ptr++ = "FEDCBA9876543210123456789ABCDEF"[15 + (tmpVal - value * base)];
		if (thSep && (base == 10) && (++thCount == 3))
		{
			*ptr++ = thSep;
			thCount = 0;
		}
	} while (value);

	if ('0' == fill)
	{
		if (tmpVal < 0) --width;
		if (prefix && base == 010) --width;
		if (prefix && base == 0x10) width -= 2;
		while ((ptr - result) < width) *ptr++ = fill;
	}

	if (prefix && base == 010) *ptr++ = '0';
	else if (prefix && base == 0x10)
	{
		*ptr++ = 'x';
		*ptr++ = '0';
	}

	if (tmpVal < 0) *ptr++ = '-';

	if ('0' != fill)
	{
		while ((ptr - result) < width) *ptr++ = fill;
	}

	size = ptr - result;
	poco_assert_dbg (size <= ptr.span());
	poco_assert_dbg ((-1 == width) || (size >= size_t(width)));
	*ptr-- = '\0';

	char* ptrr = result;
	char tmp;
	while(ptrr < ptr)
	{
		 tmp    = *ptr;
		*ptr--  = *ptrr;
		*ptrr++ = tmp;
	}

	return true;
}


template <typename T>
bool uIntToStr(T value,
	unsigned short base,
	char* result,
	std::size_t& size,
	bool prefix = false,
	int width = -1,
	char fill = ' ',
	char thSep = 0)
	/// Converts unsigned integer to string. Numeric bases from binary to hexadecimal are supported.
	/// If width is non-zero, it pads the return value with fill character to the specified width.
	/// When padding is zero character ('0'), it is prepended to the number itself; all other
	/// paddings are prepended to the formatted result with minus sign or base prefix included
	/// If prefix is true and base is octal or hexadecimal, respective prefix ('0' for octal,
	/// "0x" for hexadecimal) is prepended. For all other bases, prefix argument is ignored.
	/// Formatted string has at least [width] total length.
{
	if (base < 2 || base > 0x10)
	{
		*result = '\0';
		return false;
	}
	
	Impl::Ptr ptr(result, size);
	int thCount = 0;
	T tmpVal;
	do
	{
		tmpVal = value;
		value /= base;
		*ptr++ = "FEDCBA9876543210123456789ABCDEF"[15 + (tmpVal - value * base)];
		if (thSep && (base == 10) && (++thCount == 3))
		{
			*ptr++ = thSep;
			thCount = 0;
		}
	} while (value);
	
	if ('0' == fill)
	{
		if (prefix && base == 010) --width;
		if (prefix && base == 0x10) width -= 2;
		while ((ptr - result) < width) *ptr++ = fill;
	}
	
	if (prefix && base == 010) *ptr++ = '0';
	else if (prefix && base == 0x10)
	{
		*ptr++ = 'x';
		*ptr++ = '0';
	}
	
	if ('0' != fill)
	{
		while ((ptr - result) < width) *ptr++ = fill;
	}
	
	size = ptr - result;
	poco_assert_dbg (size <= ptr.span());
	poco_assert_dbg ((-1 == width) || (size >= size_t(width)));
	*ptr-- = '\0';
	
	char* ptrr = result;
	char tmp;
	while(ptrr < ptr)
	{
		tmp    = *ptr;
		*ptr--  = *ptrr;
		*ptrr++ = tmp;
	}
	
	return true;
}


template <typename T>
bool intToStr (T number, unsigned short base, std::string& result, bool prefix = false, int width = -1, char fill = ' ', char thSep = 0)
	/// Converts integer to string; This is a wrapper function, for details see see the
	/// bool intToStr(T, unsigned short, char*, int, int, char, char) implementation.
{
	char res[POCO_MAX_INT_STRING_LEN] = {0};
	std::size_t size = POCO_MAX_INT_STRING_LEN;
	bool ret = intToStr(number, base, res, size, prefix, width, fill, thSep);
	result.assign(res, size);
	return ret;
}
	
	
template <typename T>
bool uIntToStr (T number, unsigned short base, std::string& result, bool prefix = false, int width = -1, char fill = ' ', char thSep = 0)
	/// Converts unsigned integer to string; This is a wrapper function, for details see see the
	/// bool uIntToStr(T, unsigned short, char*, int, int, char, char) implementation.
{
	char res[POCO_MAX_INT_STRING_LEN] = {0};
	std::size_t size = POCO_MAX_INT_STRING_LEN;
	bool ret = uIntToStr(number, base, res, size, prefix, width, fill, thSep);
	result.assign(res, size);
	return ret;
}


//
// Wrappers for double-conversion library (http://code.google.com/p/double-conversion/).
//
// Library is the implementation of the algorithm described in Florian Loitsch's paper:
// http://florian.loitsch.com/publications/dtoa-pldi2010.pdf
//

Foundation_API void floatToStr(char* buffer,
	int bufferSize,
	float value,
	int lowDec = -std::numeric_limits<double>::digits10,
	int highDec = std::numeric_limits<double>::digits10);
	/// Converts a float value to string. Converted string must be shorter than bufferSize.
	/// Conversion is done by computing the shortest string of digits that correctly represents
	/// the input number. Depending on lowDec and highDec values, the function returns
	/// decimal or exponential representation.


Foundation_API std::string& floatToStr(std::string& str,
	float value,
	int precision = -1,
	int width = 0,
	char thSep = 0,
	char decSep = 0);
	/// Converts a float value, assigns it to the supplied string and returns the reference.
	/// This function calls floatToStr(char*, int, float, int, int) and formats the result according to
	/// precision (total number of digits after the decimal point, -1 means ignore precision argument) 
	/// and width (total length of formatted string).


Foundation_API void doubleToStr(char* buffer,
	int bufferSize,
	double value,
	int lowDec = -std::numeric_limits<double>::digits10,
	int highDec = std::numeric_limits<double>::digits10);
	/// Converts a double value to string. Converted string must be shorter than bufferSize.
	/// Conversion is done by computing the shortest string of digits that correctly represents
	/// the input number. Depending on lowDec and highDec values, the function returns
	/// decimal or exponential representation.


Foundation_API std::string& doubleToStr(std::string& str,
	double value,
	int precision = -1,
	int width = 0,
	char thSep = 0,
	char decSep = 0);
	/// Converts a double value, assigns it to the supplied string and returns the reference.
	/// This function calls doubleToStr(char*, int, float, int, int) and formats the result according to
	/// precision (total number of digits after the decimal point, -1 means ignore precision argument) 
	/// and width (total length of formatted string).


Foundation_API float strToFloat(const char* str);
	/// Converts the string of characters into single-precision floating point number.
	/// Function uses double_convesrion::DoubleToStringConverter to do the conversion.


Foundation_API bool strToFloat(const std::string&, float& result, char decSep = '.', char thSep = ',');
	/// Converts the string of characters into single-precision floating point number.
	/// The conversion result is assigned to the result parameter.
	/// If decimal separator and/or thousand separator are different from defaults, they should be
	/// supplied to ensure proper conversion.
	/// 
	/// Returns true if succesful, false otherwise.


Foundation_API double strToDouble(const char* str);
	/// Converts the string of characters into double-precision floating point number.


Foundation_API bool strToDouble(const std::string& str, double& result, char decSep = '.', char thSep = ',');
	/// Converts the string of characters into double-precision floating point number.
	/// The conversion result is assigned to the result parameter.
	/// If decimal separator and/or thousand separator are different from defaults, they should be
	/// supplied to ensure proper conversion.
	/// 
	/// Returns true if succesful, false otherwise.

//
// end double-conversion functions declarations
//


} // namespace Poco


#endif // Foundation_NumericString_INCLUDED
