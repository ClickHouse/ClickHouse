//
// NumberParser.h
//
// $Id: //poco/1.4/Foundation/include/Poco/NumberParser.h#1 $
//
// Library: Foundation
// Package: Core
// Module:  NumberParser
//
// Definition of the NumberParser class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_NumberParser_INCLUDED
#define Foundation_NumberParser_INCLUDED


#include "Poco/Foundation.h"
#include <string>
#undef min
#undef max
#include <limits>


namespace Poco {


class Foundation_API NumberParser
	/// The NumberParser class provides static methods
	/// for parsing numbers out of strings.
	///
	/// Note that leading or trailing whitespace is not allowed
	/// in the string. Poco::trim() or Poco::trimInPlace()
	/// can be used to remove leading or trailing whitespace.
{
public:
	static const unsigned short NUM_BASE_OCT = 010;
	static const unsigned short NUM_BASE_DEC = 10;
	static const unsigned short NUM_BASE_HEX = 0x10;

	static int parse(const std::string& s, char thousandSeparator = ',');
		/// Parses an integer value in decimal notation from the given string.
		/// Throws a SyntaxException if the string does not hold a number in decimal notation.
	
	static bool tryParse(const std::string& s, int& value, char thousandSeparator = ',');
		/// Parses an integer value in decimal notation from the given string.
		/// Returns true if a valid integer has been found, false otherwise. 
		/// If parsing was not successful, value is undefined.
	
	static unsigned parseUnsigned(const std::string& s, char thousandSeparator = ',');
		/// Parses an unsigned integer value in decimal notation from the given string.
		/// Throws a SyntaxException if the string does not hold a number in decimal notation.

	static bool tryParseUnsigned(const std::string& s, unsigned& value, char thousandSeparator = ',');
		/// Parses an unsigned integer value in decimal notation from the given string.
		/// Returns true if a valid integer has been found, false otherwise. 
		/// If parsing was not successful, value is undefined.

	static unsigned parseHex(const std::string& s);
		/// Parses an integer value in hexadecimal notation from the given string.
		/// Throws a SyntaxException if the string does not hold a number in
		/// hexadecimal notation.

	static bool tryParseHex(const std::string& s, unsigned& value);
		/// Parses an unsigned integer value in hexadecimal notation from the given string.
		/// Returns true if a valid integer has been found, false otherwise. 
		/// If parsing was not successful, value is undefined.

	static unsigned parseOct(const std::string& s);
		/// Parses an integer value in octal notation from the given string.
		/// Throws a SyntaxException if the string does not hold a number in
		/// hexadecimal notation.

	static bool tryParseOct(const std::string& s, unsigned& value);
		/// Parses an unsigned integer value in octal notation from the given string.
		/// Returns true if a valid integer has been found, false otherwise. 
		/// If parsing was not successful, value is undefined.

#if defined(POCO_HAVE_INT64)

	static Int64 parse64(const std::string& s, char thousandSeparator = ',');
		/// Parses a 64-bit integer value in decimal notation from the given string.
		/// Throws a SyntaxException if the string does not hold a number in decimal notation.

	static bool tryParse64(const std::string& s, Int64& value, char thousandSeparator = ',');
		/// Parses a 64-bit integer value in decimal notation from the given string.
		/// Returns true if a valid integer has been found, false otherwise. 
		/// If parsing was not successful, value is undefined.

	static UInt64 parseUnsigned64(const std::string& s, char thousandSeparator = ',');
		/// Parses an unsigned 64-bit integer value in decimal notation from the given string.
		/// Throws a SyntaxException if the string does not hold a number in decimal notation.

	static bool tryParseUnsigned64(const std::string& s, UInt64& value, char thousandSeparator = ',');
		/// Parses an unsigned 64-bit integer value in decimal notation from the given string.
		/// Returns true if a valid integer has been found, false otherwise. 
		/// If parsing was not successful, value is undefined.

	static UInt64 parseHex64(const std::string& s);
		/// Parses a 64 bit-integer value in hexadecimal notation from the given string.
		/// Throws a SyntaxException if the string does not hold a number in hexadecimal notation.

	static bool tryParseHex64(const std::string& s, UInt64& value);
		/// Parses an unsigned 64-bit integer value in hexadecimal notation from the given string.
		/// Returns true if a valid integer has been found, false otherwise. 
		/// If parsing was not successful, value is undefined.

	static UInt64 parseOct64(const std::string& s);
		/// Parses a 64 bit-integer value in octal notation from the given string.
		/// Throws a SyntaxException if the string does not hold a number in hexadecimal notation.

	static bool tryParseOct64(const std::string& s, UInt64& value);
		/// Parses an unsigned 64-bit integer value in octal notation from the given string.
		/// Returns true if a valid integer has been found, false otherwise. 
		/// If parsing was not successful, value is undefined.

#endif // defined(POCO_HAVE_INT64)

	static double parseFloat(const std::string& s, char decimalSeparator = '.', char thousandSeparator = ',');
		/// Parses a double value in decimal floating point notation
		/// from the given string.
		/// Throws a SyntaxException if the string does not hold a floating-point 
		/// number in decimal notation.
		
	static bool tryParseFloat(const std::string& s, double& value, char decimalSeparator = '.', char thousandSeparator = ',');
		/// Parses a double value in decimal floating point notation
		/// from the given string.
		/// Returns true if a valid floating point number has been found,
		/// false otherwise.
		/// If parsing was not successful, value is undefined.

	static bool parseBool(const std::string& s);
		/// Parses a bool value in decimal or string notation
		/// from the given string.
		/// Valid forms are: "0", "1", "true", "on", false", "yes", "no", "off".
		/// String forms are NOT case sensitive.
		/// Throws a SyntaxException if the string does not hold a valid bool number

	static bool tryParseBool(const std::string& s, bool& value);
		/// Parses a bool value in decimal or string notation
		/// from the given string.
		/// Valid forms are: "0", "1", "true", "on", false", "yes", "no", "off".
		/// String forms are NOT case sensitive.
		/// Returns true if a valid bool number has been found,
		/// false otherwise.
		/// If parsing was not successful, value is undefined.
};


} // namespace Poco


#endif // Foundation_NumberParser_INCLUDED
