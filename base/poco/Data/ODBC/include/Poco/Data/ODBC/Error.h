//
// Error.h
//
// Library: Data/ODBC
// Package: ODBC
// Module:  Error
//
// Definition of Error.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_ODBC_Error_INCLUDED
#define Data_ODBC_Error_INCLUDED


#include "Poco/Data/ODBC/ODBC.h"
#include "Poco/Data/ODBC/Utility.h"
#include "Poco/Data/ODBC/Diagnostics.h"
#include "Poco/Format.h"
#include <vector>
#ifdef POCO_OS_FAMILY_WINDOWS
#include <windows.h>
#endif
#include <sqlext.h>


namespace Poco {
namespace Data {
namespace ODBC {


template <typename H, SQLSMALLINT handleType>
class Error
	/// Class encapsulating ODBC diagnostic record collection. Collection is generated
	/// during construction. Class provides access and string generation for the collection
	/// as well as individual diagnostic records.
{
public:
	explicit Error(const H& handle) : _diagnostics(handle)
		/// Creates the Error.
	{
	}

	~Error()
		/// Destroys the Error.
	{
	}

	const Diagnostics<H, handleType>& diagnostics() const
		/// Returns the associated diagnostics.
	{
		return _diagnostics;
	}

	int count() const
		/// Returns the count of diagnostic records.
	{
		return (int) _diagnostics.count();
	}

	std::string& toString(int index, std::string& str) const
		/// Generates the string for the diagnostic record.
	{
		if ((index < 0) || (index > (count() - 1))) 
			return str;

		std::string s;
		Poco::format(s, 
			"===========================\n"
			"ODBC Diagnostic record #%d:\n"
			"===========================\n"
			"SQLSTATE = %s\nNative Error Code = %ld\n%s\n\n",
			index + 1,
			_diagnostics.sqlState(index),
			_diagnostics.nativeError(index),
			_diagnostics.message(index));

		str.append(s);

		return str;
	}

	std::string toString() const
		/// Generates the string for the diagnostic record collection.
	{
		std::string str;

		Poco::format(str, 
			"Connection:%s\nServer:%s\n",
			_diagnostics.connectionName(),
			_diagnostics.serverName());

		std::string s;
		for (int i = 0; i < count(); ++i)
		{
			s.clear();
			str.append(toString(i, s));
		}

		return str;
	}

private:
	Error();

	Diagnostics<H, handleType> _diagnostics;
};


typedef Error<SQLHENV, SQL_HANDLE_ENV> EnvironmentError;
typedef Error<SQLHDBC, SQL_HANDLE_DBC> ConnectionError;
typedef Error<SQLHSTMT, SQL_HANDLE_STMT> StatementError;
typedef Error<SQLHSTMT, SQL_HANDLE_DESC> DescriptorError;


} } } // namespace Poco::Data::ODBC


#endif
