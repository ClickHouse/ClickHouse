//
// NumberFormatter.h
//
// Library: Foundation
// Package: Core
// Module:  NumberFormatter
//
// Definition of the NumberFormatter class.
//
// Copyright (c) 2004-2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_NumberFormatter_INCLUDED
#define Foundation_NumberFormatter_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/NumericString.h"


namespace Poco {


class Foundation_API NumberFormatter
	/// The NumberFormatter class provides static methods
	/// for formatting numeric values into strings.
	///
	/// There are two kind of static member functions:
	///    * format* functions return a std::string containing
	///      the formatted value.
	///    * append* functions append the formatted value to
	///      an existing string.
{
public:
	enum BoolFormat
	{
		FMT_TRUE_FALSE,
		FMT_YES_NO,
		FMT_ON_OFF
	};

	static const unsigned NF_MAX_INT_STRING_LEN = 32; // increase for 64-bit binary formatting support
	static const unsigned NF_MAX_FLT_STRING_LEN = POCO_MAX_FLT_STRING_LEN;

	static std::string format(int value);
		/// Formats an integer value in decimal notation.

	static std::string format(int value, int width);
		/// Formats an integer value in decimal notation,
		/// right justified in a field having at least
		/// the specified width.

	static std::string format0(int value, int width);
		/// Formats an integer value in decimal notation,
		/// right justified and zero-padded in a field
		/// having at least the specified width.

	static std::string formatHex(int value, bool prefix = false);
		/// Formats an int value in hexadecimal notation.
		/// If prefix is true, "0x" prefix is prepended to the 
		/// resulting string.
		/// The value is treated as unsigned.

	static std::string formatHex(int value, int width, bool prefix = false);
		/// Formats a int value in hexadecimal notation,
		/// right justified and zero-padded in
		/// a field having at least the specified width.
		/// If prefix is true, "0x" prefix is prepended to the 
		/// resulting string.
		/// The value is treated as unsigned.

	static std::string format(unsigned value);
		/// Formats an unsigned int value in decimal notation.

	static std::string format(unsigned value, int width);
		/// Formats an unsigned long int in decimal notation,
		/// right justified in a field having at least the
		/// specified width.

	static std::string format0(unsigned int value, int width);
		/// Formats an unsigned int value in decimal notation,
		/// right justified and zero-padded in a field having at
		/// least the specified width.

	static std::string formatHex(unsigned value, bool prefix = false);
		/// Formats an unsigned int value in hexadecimal notation.
		/// If prefix is true, "0x" prefix is prepended to the 
		/// resulting string.

	static std::string formatHex(unsigned value, int width, bool prefix = false);
		/// Formats a int value in hexadecimal notation,
		/// right justified and zero-padded in
		/// a field having at least the specified width.
		/// If prefix is true, "0x" prefix is prepended to the 
		/// resulting string.

	static std::string format(long value);
		/// Formats a long value in decimal notation.

	static std::string format(long value, int width);
		/// Formats a long value in decimal notation,
		/// right justified in a field having at least the
		/// specified width.

	static std::string format0(long value, int width);
		/// Formats a long value in decimal notation,
		/// right justified and zero-padded in a field
		/// having at least the specified width.

	static std::string formatHex(long value, bool prefix = false);
		/// Formats an unsigned long value in hexadecimal notation.
		/// If prefix is true, "0x" prefix is prepended to the 
		/// resulting string.
		/// The value is treated as unsigned.

	static std::string formatHex(long value, int width, bool prefix = false);
		/// Formats an unsigned long value in hexadecimal notation,
		/// right justified and zero-padded in a field having at least the
		/// specified width.
		/// If prefix is true, "0x" prefix is prepended to the 
		/// resulting string.
		/// The value is treated as unsigned.

	static std::string format(unsigned long value);
		/// Formats an unsigned long value in decimal notation.

	static std::string format(unsigned long value, int width);
		/// Formats an unsigned long value in decimal notation,
		/// right justified in a field having at least the specified
		/// width.

	static std::string format0(unsigned long value, int width);
		/// Formats an unsigned long value in decimal notation,
		/// right justified and zero-padded
		/// in a field having at least the specified width.

	static std::string formatHex(unsigned long value, bool prefix = false);
		/// Formats an unsigned long value in hexadecimal notation.
		/// If prefix is true, "0x" prefix is prepended to the 
		/// resulting string.

	static std::string formatHex(unsigned long value, int width, bool prefix = false);
		/// Formats an unsigned long value in hexadecimal notation,
		/// right justified and zero-padded in a field having at least the
		/// specified width.
		/// If prefix is true, "0x" prefix is prepended to the 
		/// resulting string.

#ifdef POCO_HAVE_INT64

#ifdef POCO_LONG_IS_64_BIT

	static std::string format(long long value);
		/// Formats a 64-bit integer value in decimal notation.

	static std::string format(long long value, int width);
		/// Formats a 64-bit integer value in decimal notation,
		/// right justified in a field having at least the specified width.

	static std::string format0(long long value, int width);
		/// Formats a 64-bit integer value in decimal notation,
		/// right justified and zero-padded in a field having at least
		/// the specified width.

	static std::string formatHex(long long value, bool prefix = false);
		/// Formats a 64-bit integer value in hexadecimal notation.
		/// If prefix is true, "0x" prefix is prepended to the 
		/// resulting string.
		/// The value is treated as unsigned.

	static std::string formatHex(long long value, int width, bool prefix = false);
		/// Formats a 64-bit integer value in hexadecimal notation,
		/// right justified and zero-padded in a field having at least
		/// the specified width.
		/// The value is treated as unsigned.
		/// If prefix is true, "0x" prefix is prepended to the resulting string.

	static std::string format(unsigned long long value);
		/// Formats an unsigned 64-bit integer value in decimal notation.

	static std::string format(unsigned long long value, int width);
		/// Formats an unsigned 64-bit integer value in decimal notation,
		/// right justified in a field having at least the specified width.

	static std::string format0(unsigned long long value, int width);
		/// Formats an unsigned 64-bit integer value in decimal notation,
		/// right justified and zero-padded in a field having at least the
		/// specified width.

	static std::string formatHex(unsigned long long value, bool prefix = false);
		/// Formats a 64-bit integer value in hexadecimal notation.
		/// If prefix is true, "0x" prefix is prepended to the 
		/// resulting string.

	static std::string formatHex(unsigned long long value, int width, bool prefix = false);
		/// Formats a 64-bit integer value in hexadecimal notation,
		/// right justified and zero-padded in a field having at least
		/// the specified width. If prefix is true, "0x" prefix is
		/// prepended to the resulting string.

#else // ifndef POCO_LONG_IS_64_BIT

	static std::string format(Int64 value);
		/// Formats a 64-bit integer value in decimal notation.

	static std::string format(Int64 value, int width);
		/// Formats a 64-bit integer value in decimal notation,
		/// right justified in a field having at least the specified width.

	static std::string format0(Int64 value, int width);
		/// Formats a 64-bit integer value in decimal notation,
		/// right justified and zero-padded in a field having at least
		/// the specified width.

	static std::string formatHex(Int64 value, bool prefix = false);
		/// Formats a 64-bit integer value in hexadecimal notation.
		/// If prefix is true, "0x" prefix is prepended to the 
		/// resulting string.
		/// The value is treated as unsigned.

	static std::string formatHex(Int64 value, int width, bool prefix = false);
		/// Formats a 64-bit integer value in hexadecimal notation,
		/// right justified and zero-padded in a field having at least
		/// the specified width.
		/// The value is treated as unsigned.
		/// If prefix is true, "0x" prefix is prepended to the resulting string.

	static std::string format(UInt64 value);
		/// Formats an unsigned 64-bit integer value in decimal notation.

	static std::string format(UInt64 value, int width);
		/// Formats an unsigned 64-bit integer value in decimal notation,
		/// right justified in a field having at least the specified width.

	static std::string format0(UInt64 value, int width);
		/// Formats an unsigned 64-bit integer value in decimal notation,
		/// right justified and zero-padded in a field having at least the
		/// specified width.

	static std::string formatHex(UInt64 value, bool prefix = false);
		/// Formats a 64-bit integer value in hexadecimal notation.
		/// If prefix is true, "0x" prefix is prepended to the 
		/// resulting string.

	static std::string formatHex(UInt64 value, int width, bool prefix = false);
		/// Formats a 64-bit integer value in hexadecimal notation,
		/// right justified and zero-padded in a field having at least
		/// the specified width. If prefix is true, "0x" prefix is
		/// prepended to the resulting string.

#endif // ifdef POCO_LONG_IS_64_BIT

#endif // ifdef POCO_HAVE_INT64

	static std::string format(float value);
		/// Formats a float value in decimal floating-point notation,
		/// according to std::printf's %g format with a precision of 8 fractional digits.

	static std::string format(float value, int precision);
		/// Formats a double value in decimal floating-point notation,
		/// according to std::printf's %f format with the given precision.

	static std::string format(float value, int width, int precision);
		/// Formats a double value in decimal floating-point notation,
		/// right justified in a field of the specified width,
		/// with the number of fractional digits given in precision.

	static std::string format(double value);
		/// Formats a double value in decimal floating-point notation,
		/// according to std::printf's %g format with a precision of 16 fractional digits.

	static std::string format(double value, int precision);
		/// Formats a double value in decimal floating-point notation,
		/// according to std::printf's %f format with the given precision.

	static std::string format(double value, int width, int precision);
		/// Formats a double value in decimal floating-point notation,
		/// right justified in a field of the specified width,
		/// with the number of fractional digits given in precision.

	static std::string format(const void* ptr);
		/// Formats a pointer in an eight (32-bit architectures) or
		/// sixteen (64-bit architectures) characters wide
		/// field in hexadecimal notation.

	static std::string format(bool value, BoolFormat format = FMT_TRUE_FALSE);
		/// Formats a bool value in decimal/text notation,
		/// according to format parameter.

	static void append(std::string& str, int value);
		/// Formats an integer value in decimal notation.

	static void append(std::string& str, int value, int width);
		/// Formats an integer value in decimal notation,
		/// right justified in a field having at least
		/// the specified width.

	static void append0(std::string& str, int value, int width);
		/// Formats an integer value in decimal notation,
		/// right justified and zero-padded in a field
		/// having at least the specified width.

	static void appendHex(std::string& str, int value);
		/// Formats an int value in hexadecimal notation.
		/// The value is treated as unsigned.

	static void appendHex(std::string& str, int value, int width);
		/// Formats a int value in hexadecimal notation,
		/// right justified and zero-padded in
		/// a field having at least the specified width.
		/// The value is treated as unsigned.

	static void append(std::string& str, unsigned value);
		/// Formats an unsigned int value in decimal notation.

	static void append(std::string& str, unsigned value, int width);
		/// Formats an unsigned long int in decimal notation,
		/// right justified in a field having at least the
		/// specified width.

	static void append0(std::string& str, unsigned int value, int width);
		/// Formats an unsigned int value in decimal notation,
		/// right justified and zero-padded in a field having at
		/// least the specified width.

	static void appendHex(std::string& str, unsigned value);
		/// Formats an unsigned int value in hexadecimal notation.

	static void appendHex(std::string& str, unsigned value, int width);
		/// Formats a int value in hexadecimal notation,
		/// right justified and zero-padded in
		/// a field having at least the specified width.

	static void append(std::string& str, long value);
		/// Formats a long value in decimal notation.

	static void append(std::string& str, long value, int width);
		/// Formats a long value in decimal notation,
		/// right justified in a field having at least the
		/// specified width.

	static void append0(std::string& str, long value, int width);
		/// Formats a long value in decimal notation,
		/// right justified and zero-padded in a field
		/// having at least the specified width.

	static void appendHex(std::string& str, long value);
		/// Formats an unsigned long value in hexadecimal notation.
		/// The value is treated as unsigned.

	static void appendHex(std::string& str, long value, int width);
		/// Formats an unsigned long value in hexadecimal notation,
		/// right justified and zero-padded in a field having at least the
		/// specified width.
		/// The value is treated as unsigned.

	static void append(std::string& str, unsigned long value);
		/// Formats an unsigned long value in decimal notation.

	static void append(std::string& str, unsigned long value, int width);
		/// Formats an unsigned long value in decimal notation,
		/// right justified in a field having at least the specified
		/// width.

	static void append0(std::string& str, unsigned long value, int width);
		/// Formats an unsigned long value in decimal notation,
		/// right justified and zero-padded
		/// in a field having at least the specified width.

	static void appendHex(std::string& str, unsigned long value);
		/// Formats an unsigned long value in hexadecimal notation.

	static void appendHex(std::string& str, unsigned long value, int width);
		/// Formats an unsigned long value in hexadecimal notation,
		/// right justified and zero-padded in a field having at least the
		/// specified width.

#ifdef POCO_HAVE_INT64

#ifdef POCO_LONG_IS_64_BIT

	static void append(std::string& str, long long value);
		/// Formats a 64-bit integer value in decimal notation.

	static void append(std::string& str, long long value, int width);
		/// Formats a 64-bit integer value in decimal notation,
		/// right justified in a field having at least the specified width.

	static void append0(std::string& str, long long value, int width);
		/// Formats a 64-bit integer value in decimal notation,
		/// right justified and zero-padded in a field having at least
		/// the specified width.

	static void appendHex(std::string& str, long long value);
		/// Formats a 64-bit integer value in hexadecimal notation.
		/// The value is treated as unsigned.

	static void appendHex(std::string& str, long long value, int width);
		/// Formats a 64-bit integer value in hexadecimal notation,
		/// right justified and zero-padded in a field having at least
		/// the specified width.
		/// The value is treated as unsigned.

	static void append(std::string& str, unsigned long long value);
		/// Formats an unsigned 64-bit integer value in decimal notation.

	static void append(std::string& str, unsigned long long value, int width);
		/// Formats an unsigned 64-bit integer value in decimal notation,
		/// right justified in a field having at least the specified width.

	static void append0(std::string& str, unsigned long long value, int width);
		/// Formats an unsigned 64-bit integer value in decimal notation,
		/// right justified and zero-padded in a field having at least the
		/// specified width.

	static void appendHex(std::string& str, unsigned long long value);
		/// Formats a 64-bit integer value in hexadecimal notation.

	static void appendHex(std::string& str, unsigned long long value, int width);
		/// Formats a 64-bit integer value in hexadecimal notation,
		/// right justified and zero-padded in a field having at least
		/// the specified width.

#else // ifndef POCO_LONG_IS_64_BIT

	static void append(std::string& str, Int64 value);
		/// Formats a 64-bit integer value in decimal notation.

	static void append(std::string& str, Int64 value, int width);
		/// Formats a 64-bit integer value in decimal notation,
		/// right justified in a field having at least the specified width.

	static void append0(std::string& str, Int64 value, int width);
		/// Formats a 64-bit integer value in decimal notation,
		/// right justified and zero-padded in a field having at least
		/// the specified width.

	static void appendHex(std::string& str, Int64 value);
		/// Formats a 64-bit integer value in hexadecimal notation.
		/// The value is treated as unsigned.

	static void appendHex(std::string& str, Int64 value, int width);
		/// Formats a 64-bit integer value in hexadecimal notation,
		/// right justified and zero-padded in a field having at least
		/// the specified width.
		/// The value is treated as unsigned.

	static void append(std::string& str, UInt64 value);
		/// Formats an unsigned 64-bit integer value in decimal notation.

	static void append(std::string& str, UInt64 value, int width);
		/// Formats an unsigned 64-bit integer value in decimal notation,
		/// right justified in a field having at least the specified width.

	static void append0(std::string& str, UInt64 value, int width);
		/// Formats an unsigned 64-bit integer value in decimal notation,
		/// right justified and zero-padded in a field having at least the
		/// specified width.

	static void appendHex(std::string& str, UInt64 value);
		/// Formats a 64-bit integer value in hexadecimal notation.

	static void appendHex(std::string& str, UInt64 value, int width);
		/// Formats a 64-bit integer value in hexadecimal notation,
		/// right justified and zero-padded in a field having at least
		/// the specified width.

#endif // ifdef POCO_LONG_IS_64_BIT

#endif // ifdef POCO_HAVE_INT64

	static void append(std::string& str, float value);
		/// Formats a float value in decimal floating-point notation,
		/// according to std::printf's %g format with a precision of 8 fractional digits.

	static void append(std::string& str, float value, int precision);
		/// Formats a double value in decimal floating-point notation,
		/// according to std::printf's %f format with the given precision.

	static void append(std::string& str, float value, int width, int precision);
		/// Formats a double value in decimal floating-point notation,
		/// right justified in a field of the specified width,
		/// with the number of fractional digits given in precision.

	static void append(std::string& str, double value);
		/// Formats a double value in decimal floating-point notation,
		/// according to std::printf's %g format with a precision of 16 fractional digits.

	static void append(std::string& str, double value, int precision);
		/// Formats a double value in decimal floating-point notation,
		/// according to std::printf's %f format with the given precision.

	static void append(std::string& str, double value, int width, int precision);
		/// Formats a double value in decimal floating-point notation,
		/// right justified in a field of the specified width,
		/// with the number of fractional digits given in precision.

	static void append(std::string& str, const void* ptr);
		/// Formats a pointer in an eight (32-bit architectures) or
		/// sixteen (64-bit architectures) characters wide
		/// field in hexadecimal notation.

private:
};


//
// inlines
//

inline std::string NumberFormatter::format(int value)
{
	std::string result;
	intToStr(value, 10, result);
	return result;
}


inline std::string NumberFormatter::format(int value, int width)
{
	std::string result;
	intToStr(value, 10, result, false, width, ' ');
	return result;
}


inline std::string NumberFormatter::format0(int value, int width)
{
	std::string result;
	intToStr(value, 10, result, false, width, '0');
	return result;
}


inline std::string NumberFormatter::formatHex(int value, bool prefix)
{
	std::string result;
	uIntToStr(static_cast<unsigned int>(value), 0x10, result, prefix);
	return result;
}


inline std::string NumberFormatter::formatHex(int value, int width, bool prefix)
{
	std::string result;
	uIntToStr(static_cast<unsigned int>(value), 0x10, result, prefix, width, '0');
	return result;
}


inline std::string NumberFormatter::format(unsigned value)
{
	std::string result;
	uIntToStr(value, 10, result);
	return result;
}


inline std::string NumberFormatter::format(unsigned value, int width)
{
	std::string result;
	uIntToStr(value, 10, result, false, width, ' ');
	return result;
}


inline std::string NumberFormatter::format0(unsigned int value, int width)
{
	std::string result;
	uIntToStr(value, 10, result, false, width, '0');
	return result;
}


inline std::string NumberFormatter::formatHex(unsigned value, bool prefix)
{
	std::string result;
	uIntToStr(value, 0x10, result, prefix);
	return result;
}


inline std::string NumberFormatter::formatHex(unsigned value, int width, bool prefix)
{
	std::string result;
	uIntToStr(value, 0x10, result, prefix, width, '0');
	return result;
}


inline std::string NumberFormatter::format(long value)
{
	std::string result;
	intToStr(value, 10, result);
	return result;
}


inline std::string NumberFormatter::format(long value, int width)
{
	std::string result;
	intToStr(value, 10, result, false, width, ' ');
	return result;
}


inline std::string NumberFormatter::format0(long value, int width)
{
	std::string result;
	intToStr(value, 10, result, false, width, '0');
	return result;
}


inline std::string NumberFormatter::formatHex(long value, bool prefix)
{
	std::string result;
	uIntToStr(static_cast<unsigned long>(value), 0x10, result, prefix);
	return result;
}


inline std::string NumberFormatter::formatHex(long value, int width, bool prefix)
{
	std::string result;
	uIntToStr(static_cast<unsigned long>(value), 0x10, result, prefix, width, '0');
	return result;
}


inline std::string NumberFormatter::format(unsigned long value)
{
	std::string result;
	uIntToStr(value, 10, result);
	return result;
}


inline std::string NumberFormatter::format(unsigned long value, int width)
{
	std::string result;
	uIntToStr(value, 10, result, false, width, ' ');
	return result;
}


inline std::string NumberFormatter::format0(unsigned long value, int width)
{
	std::string result;
	uIntToStr(value, 10, result, false, width, '0');
	return result;
}


inline std::string NumberFormatter::formatHex(unsigned long value, bool prefix)
{
	std::string result;
	uIntToStr(value, 0x10, result, prefix);
	return result;
}


inline std::string NumberFormatter::formatHex(unsigned long value, int width, bool prefix)
{
	std::string result;
	uIntToStr(value, 0x10, result, prefix, width, '0');
	return result;
}


#ifdef POCO_HAVE_INT64

#ifdef POCO_LONG_IS_64_BIT


inline std::string NumberFormatter::format(long long value)
{
	std::string result;
	intToStr(value, 10, result);
	return result;
}


inline std::string NumberFormatter::format(long long value, int width)
{
	std::string result;
	intToStr(value, 10, result, false, width, ' ');
	return result;
}


inline std::string NumberFormatter::format0(long long value, int width)
{
	std::string result;
	intToStr(value, 10, result, false, width, '0');
	return result;
}


inline std::string NumberFormatter::formatHex(long long value, bool prefix)
{
	std::string result;
	uIntToStr(static_cast<unsigned long long>(value), 0x10, result, prefix);
	return result;
}


inline std::string NumberFormatter::formatHex(long long value, int width, bool prefix)
{
	std::string result;
	uIntToStr(static_cast<unsigned long long>(value), 0x10, result, prefix, width, '0');
	return result;
}


inline std::string NumberFormatter::format(unsigned long long value)
{
	std::string result;
	uIntToStr(value, 10, result);
	return result;
}


inline std::string NumberFormatter::format(unsigned long long value, int width)
{
	std::string result;
	uIntToStr(value, 10, result, false, width, ' ');
	return result;
}


inline std::string NumberFormatter::format0(unsigned long long value, int width)
{
	std::string result;
	uIntToStr(value, 10, result, false, width, '0');
	return result;
}


inline std::string NumberFormatter::formatHex(unsigned long long value, bool prefix)
{
	std::string result;
	uIntToStr(value, 0x10, result, prefix);
	return result;
}


inline std::string NumberFormatter::formatHex(unsigned long long value, int width, bool prefix)
{
	std::string result;
	uIntToStr(value, 0x10, result, prefix, width, '0');
	return result;
}


#else // ifndef POCO_LONG_IS_64_BIT


inline std::string NumberFormatter::format(Int64 value)
{
	std::string result;
	intToStr(value, 10, result);
	return result;
}


inline std::string NumberFormatter::format(Int64 value, int width)
{
	std::string result;
	intToStr(value, 10, result, false, width, ' ');
	return result;
}


inline std::string NumberFormatter::format0(Int64 value, int width)
{
	std::string result;
	intToStr(value, 10, result, false, width, '0');
	return result;
}


inline std::string NumberFormatter::formatHex(Int64 value, bool prefix)
{
	std::string result;
	uIntToStr(static_cast<UInt64>(value), 0x10, result, prefix);
	return result;
}


inline std::string NumberFormatter::formatHex(Int64 value, int width, bool prefix)
{
	std::string result;
	uIntToStr(static_cast<UInt64>(value), 0x10, result, prefix, width, '0');
	return result;
}


inline std::string NumberFormatter::format(UInt64 value)
{
	std::string result;
	uIntToStr(value, 10, result);
	return result;
}


inline std::string NumberFormatter::format(UInt64 value, int width)
{
	std::string result;
	uIntToStr(value, 10, result, false, width, ' ');
	return result;
}


inline std::string NumberFormatter::format0(UInt64 value, int width)
{
	std::string result;
	uIntToStr(value, 10, result, false, width, '0');
	return result;
}


inline std::string NumberFormatter::formatHex(UInt64 value, bool prefix)
{
	std::string result;
	uIntToStr(value, 0x10, result, prefix);
	return result;
}


inline std::string NumberFormatter::formatHex(UInt64 value, int width, bool prefix)
{
	std::string result;
	uIntToStr(value, 0x10, result, prefix, width, '0');
	return result;
}


#endif // ifdef POCO_LONG_IS_64_BIT

#endif // ifdef POCO_HAVE_INT64


inline std::string NumberFormatter::format(float value)
{
	char buffer[POCO_MAX_FLT_STRING_LEN];
	floatToStr(buffer, POCO_MAX_FLT_STRING_LEN, value);
	return std::string(buffer);
}


inline std::string NumberFormatter::format(float value, int precision)
{
	char buffer[POCO_MAX_FLT_STRING_LEN];
	floatToFixedStr(buffer, POCO_MAX_FLT_STRING_LEN, value, precision);
	return std::string(buffer);
}


inline std::string NumberFormatter::format(float value, int width, int precision)
{
	std::string result;
	floatToFixedStr(result, value, precision, width);
	return result;
}


inline std::string NumberFormatter::format(double value)
{
	char buffer[POCO_MAX_FLT_STRING_LEN];
	doubleToStr(buffer, POCO_MAX_FLT_STRING_LEN, value);
	return std::string(buffer);
}


inline std::string NumberFormatter::format(double value, int precision)
{
	char buffer[POCO_MAX_FLT_STRING_LEN];
	doubleToFixedStr(buffer, POCO_MAX_FLT_STRING_LEN, value, precision);
	return std::string(buffer);
}


inline std::string NumberFormatter::format(double value, int width, int precision)
{
	std::string result;
	doubleToFixedStr(result, value, precision, width);
	return result;
}


inline std::string NumberFormatter::format(const void* ptr)
{
	std::string result;
	append(result, ptr);
	return result;
}


} // namespace Poco


#endif // Foundation_NumberFormatter_INCLUDED
