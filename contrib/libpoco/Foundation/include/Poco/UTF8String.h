//
// UTF8String.h
//
// $Id: //poco/1.4/Foundation/include/Poco/UTF8String.h#1 $
//
// Library: Foundation
// Package: Text
// Module:  UTF8String
//
// Definition of the UTF8 string functions.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_UTF8String_INCLUDED
#define Foundation_UTF8String_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


struct Foundation_API UTF8
	/// This class provides static methods that are UTF-8 capable variants
	/// of the same functions in Poco/String.h.
	///
	/// The various variants of icompare() provide case insensitive comparison
	/// for UTF-8 encoded strings.
	///
	/// toUpper(), toUpperInPlace(), toLower() and toLowerInPlace() provide
	/// Unicode-based character case transformation for UTF-8 encoded strings.
	///
	/// removeBOM() removes the UTF-8 Byte Order Mark sequence (0xEF, 0xBB, 0xBF)
	/// from the beginning of the given string, if it's there.
{
	static int icompare(const std::string& str, std::string::size_type pos, std::string::size_type n, std::string::const_iterator it2, std::string::const_iterator end2);
	static int icompare(const std::string& str1, const std::string& str2);
	static int icompare(const std::string& str1, std::string::size_type n1, const std::string& str2, std::string::size_type n2);
	static int icompare(const std::string& str1, std::string::size_type n, const std::string& str2);
	static int icompare(const std::string& str1, std::string::size_type pos, std::string::size_type n, const std::string& str2);
	static int icompare(const std::string& str1, std::string::size_type pos1, std::string::size_type n1, const std::string& str2, std::string::size_type pos2, std::string::size_type n2);
	static int icompare(const std::string& str1, std::string::size_type pos1, std::string::size_type n, const std::string& str2, std::string::size_type pos2);
	static int icompare(const std::string& str, std::string::size_type pos, std::string::size_type n, const std::string::value_type* ptr);
	static int icompare(const std::string& str, std::string::size_type pos, const std::string::value_type* ptr);
	static int icompare(const std::string& str, const std::string::value_type* ptr);

	static std::string toUpper(const std::string& str);
	static std::string& toUpperInPlace(std::string& str);
	static std::string toLower(const std::string& str);
	static std::string& toLowerInPlace(std::string& str);
	
	static void removeBOM(std::string& str);
		/// Remove the UTF-8 Byte Order Mark sequence (0xEF, 0xBB, 0xBF)
		/// from the beginning of the string, if it's there.
};


} // namespace Poco


#endif // Foundation_UTF8String_INCLUDED
