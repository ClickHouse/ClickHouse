//
// Unicode.h
//
// $Id: //poco/Main/Data/ODBC/include/Poco/Data/ODBC/Unicode_WIN32.h#4 $
//
// Library: ODBC
// Package: ODBC
// Module:  Unicode_WIN32
//
// Definition of Unicode_WIN32.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_ODBC_Unicode_WIN32_INCLUDED
#define Data_ODBC_Unicode_WIN32_INCLUDED


namespace Poco {
namespace Data {
namespace ODBC {


inline void makeUTF16(SQLCHAR* pSQLChar, SQLINTEGER length, std::wstring& target)
	/// Utility function for conversion from UTF-8 to UTF-16
{
	int len = length;
	if (SQL_NTS == len) 
		len = (int) std::strlen((const char *) pSQLChar);

	UnicodeConverter::toUTF16((const char *) pSQLChar, len, target);
}


inline void makeUTF8(Poco::Buffer<wchar_t>& buffer, SQLINTEGER length, SQLPOINTER pTarget, SQLINTEGER targetLength)
	/// Utility function for conversion from UTF-16 to UTF-8. Length is in bytes.
{
	if (buffer.sizeBytes() < length)
		throw InvalidArgumentException("Specified length exceeds available length.");
	else if ((length % 2) != 0)
		throw InvalidArgumentException("Length must be an even number.");

	length /= sizeof(wchar_t);
	std::string result;
	UnicodeConverter::toUTF8(buffer.begin(), length, result);
	
	std::memset(pTarget, 0, targetLength);
	std::strncpy((char*) pTarget, result.c_str(), result.size() < targetLength ? result.size() : targetLength);
}


} } } // namespace Poco::Data::ODBC


#endif // Data_ODBC_Unicode_WIN32_INCLUDED
