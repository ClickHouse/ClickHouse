//
// Unicode.h
//
// $Id: //poco/Main/Data/ODBC/include/Poco/Data/ODBC/Unicode_UNIXODBC.h#4 $
//
// Library: ODBC
// Package: ODBC
// Module:  Unicode_UNIX
//
// Definition of Unicode_UNIX.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_ODBC_Unicode_UNIX_INCLUDED
#define Data_ODBC_Unicode_UNIX_INCLUDED


namespace Poco {
namespace Data {
namespace ODBC {


void makeUTF16(SQLCHAR* pSQLChar, SQLINTEGER length, std::string& target);
	/// Utility function for conversion from UTF-8 to UTF-16


inline void makeUTF16(SQLCHAR* pSQLChar, SQLSMALLINT length, std::string& target)
	/// Utility function for conversion from UTF-8 to UTF-16.
{
	makeUTF16(pSQLChar, (SQLINTEGER) length, target);
}


void makeUTF8(Poco::Buffer<SQLWCHAR>& buffer, SQLINTEGER length, SQLPOINTER pTarget, SQLINTEGER targetLength);
	/// Utility function for conversion from UTF-16 to UTF-8.


inline void makeUTF8(Poco::Buffer<SQLWCHAR>& buffer, int length, SQLPOINTER pTarget, SQLSMALLINT targetLength)
	/// Utility function for conversion from UTF-16 to UTF-8.
{
	makeUTF8(buffer, length, pTarget, (SQLINTEGER) targetLength);
}


} } } // namespace Poco::Data::ODBC


#endif // Data_ODBC_Unicode_UNIX_INCLUDED
