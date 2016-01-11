//
// Diagnostics.h
//
// $Id: //poco/Main/Data/ODBC/include/Poco/Data/ODBC/Diagnostics.h#4 $
//
// Library: ODBC
// Package: ODBC
// Module:  Diagnostics
//
// Definition of Diagnostics.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_ODBC_Diagnostics_INCLUDED
#define Data_ODBC_Diagnostics_INCLUDED


#include "Poco/Data/ODBC/ODBC.h"
#include <vector>
#include <cstring>
#ifdef POCO_OS_FAMILY_WINDOWS
#include <windows.h>
#endif
#include <sqlext.h>


namespace Poco {
namespace Data {
namespace ODBC {


template <typename H, SQLSMALLINT handleType>
class Diagnostics
	/// Utility class providing functionality for retrieving ODBC diagnostic
	/// records. Diagnostics object must be created with corresponding handle
	/// as constructor argument. During construction, diagnostic records fields 
	/// are populated and the object is ready for querying.
{
public:

	static const unsigned int SQL_STATE_SIZE = SQL_SQLSTATE_SIZE + 1;
	static const unsigned int SQL_MESSAGE_LENGTH = SQL_MAX_MESSAGE_LENGTH + 1;
	static const unsigned int SQL_NAME_LENGTH = 128;
	static const std::string  DATA_TRUNCATED;

	struct DiagnosticFields
	{
		/// SQLGetDiagRec fields
		SQLCHAR    _sqlState[SQL_STATE_SIZE];
		SQLCHAR    _message[SQL_MESSAGE_LENGTH];
		SQLINTEGER _nativeError;
	};

	typedef std::vector<DiagnosticFields> FieldVec;
	typedef typename FieldVec::const_iterator Iterator;

	explicit Diagnostics(const H& handle): _handle(handle)
		/// Creates and initializes the Diagnostics.
	{
		std::memset(_connectionName, 0, sizeof(_connectionName));
		std::memset(_serverName, 0, sizeof(_serverName));
		diagnostics();
	}

	~Diagnostics()
		/// Destroys the Diagnostics.
	{
	}

	std::string sqlState(int index) const
		/// Returns SQL state.
	{
		poco_assert (index < count());
		return std::string((char*) _fields[index]._sqlState);
	}

	std::string message(int index) const
		/// Returns error message.
	{
		poco_assert (index < count());
		return std::string((char*) _fields[index]._message);
	}

	long nativeError(int index) const
		/// Returns native error code.
	{
		poco_assert (index < count());
		return _fields[index]._nativeError;
	}

	std::string connectionName() const
		/// Returns the connection name. 
		/// If there is no active connection, connection name defaults to NONE.
		/// If connection name is not applicable for query context (such as when querying environment handle),
		/// connection name defaults to NOT_APPLICABLE.
	{
		return std::string((char*) _connectionName);
	}

	std::string serverName() const
		/// Returns the server name.
		/// If the connection has not been established, server name defaults to NONE.
		/// If server name is not applicable for query context (such as when querying environment handle),
		/// connection name defaults to NOT_APPLICABLE.
	{
		return std::string((char*) _serverName);
	}

	int count() const
		/// Returns the number of contained diagnostic records.
	{
		return (int) _fields.size();
	}

	void reset()
		/// Resets the diagnostic fields container.
	{
		_fields.clear();
	}

	const FieldVec& fields() const
	{
		return _fields;
	}

	Iterator begin() const
	{
		return _fields.begin();
	}

	Iterator end() const
	{
		return _fields.end();
	}

	const Diagnostics& diagnostics()
	{
		DiagnosticFields df;
		SQLSMALLINT count = 1;
		SQLSMALLINT messageLength = 0;
		static const std::string none = "None";
		static const std::string na = "Not applicable";

		reset();

		while (!Utility::isError(SQLGetDiagRec(handleType, 
			_handle, 
			count, 
			df._sqlState, 
			&df._nativeError, 
			df._message, 
			SQL_MESSAGE_LENGTH, 
			&messageLength))) 
		{
			if (1 == count)
			{
				// success of the following two calls is optional
				// (they fail if connection has not been established yet
				//  or return empty string if not applicable for the context)
				if (Utility::isError(SQLGetDiagField(handleType, 
					_handle, 
					count, 
					SQL_DIAG_CONNECTION_NAME, 
					_connectionName, 
					sizeof(_connectionName), 
					&messageLength)))
				{
					std::size_t len = sizeof(_connectionName) > none.length() ? 
						none.length() : sizeof(_connectionName) - 1;
					std::memcpy(_connectionName, none.c_str(), len);
				}
				else if (0 == _connectionName[0]) 
				{
					std::size_t len = sizeof(_connectionName) > na.length() ? 
						na.length() : sizeof(_connectionName) - 1;
					std::memcpy(_connectionName, na.c_str(), len);
				}
				
				if (Utility::isError(SQLGetDiagField(handleType, 
					_handle, 
					count, 
					SQL_DIAG_SERVER_NAME, 
					_serverName, 
					sizeof(_serverName), 
					&messageLength)))
				{
					std::size_t len = sizeof(_serverName) > none.length() ? 
						none.length() : sizeof(_serverName) - 1;
					std::memcpy(_serverName, none.c_str(), len);
				}
				else if (0 == _serverName[0]) 
				{
					std::size_t len = sizeof(_serverName) > na.length() ? 
						na.length() : sizeof(_serverName) - 1;
					std::memcpy(_serverName, na.c_str(), len);
				}
			}

			_fields.push_back(df);

			std::memset(df._sqlState, 0, SQL_STATE_SIZE);
			std::memset(df._message, 0, SQL_MESSAGE_LENGTH);
			df._nativeError = 0;

			++count;
		}

		return *this;
	}

private:

	Diagnostics();

	/// SQLGetDiagField fields
	SQLCHAR _connectionName[SQL_NAME_LENGTH];
	SQLCHAR _serverName[SQL_NAME_LENGTH];

	/// Diagnostics container
	FieldVec _fields;

	/// Context handle
	const H& _handle;
};


typedef Diagnostics<SQLHENV, SQL_HANDLE_ENV> EnvironmentDiagnostics;
typedef Diagnostics<SQLHDBC, SQL_HANDLE_DBC> ConnectionDiagnostics;
typedef Diagnostics<SQLHSTMT, SQL_HANDLE_STMT> StatementDiagnostics;
typedef Diagnostics<SQLHDESC, SQL_HANDLE_DESC> DescriptorDiagnostics;


} } } // namespace Poco::Data::ODBC


#endif
