//
// ODBCException.h
//
// Library: Data/ODBC
// Package: ODBC
// Module:  ODBCException
//
// Definition of ODBCException.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_ODBC_ODBCException_INCLUDED
#define Data_ODBC_ODBCException_INCLUDED


#include "Poco/Data/ODBC/ODBC.h"
#include "Poco/Data/ODBC/Utility.h"
#include "Poco/Data/ODBC/Diagnostics.h"
#include "Poco/Data/ODBC/Error.h"
#include "Poco/Data/DataException.h"
#include "Poco/Format.h"


namespace Poco {
namespace Data {
namespace ODBC {


POCO_DECLARE_EXCEPTION(ODBC_API, ODBCException, Poco::Data::DataException)
POCO_DECLARE_EXCEPTION(ODBC_API, InsufficientStorageException, ODBCException)
POCO_DECLARE_EXCEPTION(ODBC_API, UnknownDataLengthException, ODBCException)
POCO_DECLARE_EXCEPTION(ODBC_API, DataTruncatedException, ODBCException)


template <class H, SQLSMALLINT handleType>
class HandleException: public ODBCException
{
public:
	HandleException(const H& handle): _error(handle)
		/// Creates HandleException
	{
		message(_error.toString());
	}

	HandleException(const H& handle, const std::string& msg): 
		ODBCException(msg), 
		_error(handle)
		/// Creates HandleException
	{
		extendedMessage(_error.toString());
	}							

	HandleException(const H& handle, const std::string& msg, const std::string& arg): 
		ODBCException(msg, arg), 
		_error(handle)
		/// Creates HandleException
	{
	}

	HandleException(const H& handle, const std::string& msg, const Poco::Exception& exc): 
		ODBCException(msg, exc),
		_error(handle)
		/// Creates HandleException
	{
	}

	HandleException(const HandleException& exc): 
		ODBCException(exc),
		_error(exc._error)
		/// Creates HandleException
	{
	}

	~HandleException() throw()
		/// Destroys HandleException
	{
	}

	HandleException& operator = (const HandleException& exc)
		/// Assignment operator
	{
		if (&exc != this) _error = exc._error;

		return *this;
	}

	const char* name() const throw()
		/// Returns the name of the exception
	{
		return "ODBC handle exception";
	}

	const char* className() const throw()
		/// Returns the HandleException class name.
	{
		return typeid(*this).name();
	}

	Poco::Exception* clone() const
		/// Clones the HandleException
	{
		return new HandleException(*this);
	}

	void rethrow() const
		/// Re-throws the HandleException.
	{
		throw *this;
	}

	const Diagnostics<H, handleType>& diagnostics() const
		/// Returns error diagnostics.
	{
		return _error.diagnostics();
	}

	std::string toString() const
		/// Returns the formatted error diagnostics for the handle.
	{
		return Poco::format("ODBC Error: %s\n===================\n%s\n",
			std::string(what()),
			_error.toString());
	}

	static std::string errorString(const H& handle)
		/// Returns the error diagnostics string for the handle.
	{
		return Error<H, handleType>(handle).toString();
	}

private:
	Error<H, handleType> _error;
};


typedef HandleException<SQLHENV, SQL_HANDLE_ENV>   EnvironmentException;
typedef HandleException<SQLHDBC, SQL_HANDLE_DBC>   ConnectionException;
typedef HandleException<SQLHSTMT, SQL_HANDLE_STMT> StatementException;
typedef HandleException<SQLHDESC, SQL_HANDLE_DESC> DescriptorException;


} } } // namespace Poco::Data::ODBC


#endif
