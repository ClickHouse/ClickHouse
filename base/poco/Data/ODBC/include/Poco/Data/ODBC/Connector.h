//
// Connector.h
//
// Library: Data/ODBC
// Package: ODBC
// Module:  Connector
//
// Definition of the Connector class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_ODBC_Connector_INCLUDED
#define Data_ODBC_Connector_INCLUDED


#include "Poco/Data/ODBC/ODBC.h"
#include "Poco/Data/Connector.h"


namespace Poco {
namespace Data {
namespace ODBC {


class ODBC_API Connector: public Poco::Data::Connector
	/// Connector instantiates SqLite SessionImpl objects.
{
public:
	static const std::string KEY;
		/// Keyword for creating ODBC sessions.

	Connector();
		/// Creates the Connector.

	~Connector();
		/// Destroys the Connector.

	const std::string& name() const;
		/// Returns the name associated with this connector.

	Poco::AutoPtr<Poco::Data::SessionImpl> createSession(const std::string& connectionString,
		std::size_t timeout = Poco::Data::SessionImpl::LOGIN_TIMEOUT_DEFAULT);
		/// Creates a ODBC SessionImpl object and initializes it with the given connectionString.

	static void registerConnector();
		/// Registers the Connector under the Keyword Connector::KEY at the Poco::Data::SessionFactory

	static void unregisterConnector();
		/// Unregisters the Connector under the Keyword Connector::KEY at the Poco::Data::SessionFactory

	static void bindStringToLongVarChar(bool flag = true);
		/// If set to true (default), std::string is bound to SQL_LONGVARCHAR.
		///
		/// This can cause issues with SQL Server, resulting in an error
		/// ("The data types varchar and text are incompatible in the equal to operator")
		/// when comparing against a VARCHAR. 
		///
		/// Set this to false to bind std::string to SQL_VARCHAR.
		///
		/// NOTE: This is a global setting, affecting all sessions.
		/// This setting should not be changed after the first Session has
		/// been created.

	static bool stringBoundToLongVarChar();
		/// Returns true if std::string is bound to SQL_LONGVARCHAR,
		/// otherwise false (bound to SQL_VARCHAR).

private:
	static bool _bindStringToLongVarChar;
};


///
/// inlines
///
inline const std::string& Connector::name() const
{
	return KEY;
}


inline bool Connector::stringBoundToLongVarChar()
{
	return _bindStringToLongVarChar;
}


} } } // namespace Poco::Data::ODBC


#endif // Data_ODBC_Connector_INCLUDED
