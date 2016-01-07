//
// Connector.h
//
// $Id: //poco/Main/Data/ODBC/include/Poco/Data/ODBC/Connector.h#2 $
//
// Library: ODBC
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


// Note: to avoid static (de)initialization problems,
// during connector automatic (un)registration, it is 
// best to have this as a macro.
#define POCO_DATA_ODBC_CONNECTOR_NAME "odbc"


namespace Poco {
namespace Data {
namespace ODBC {


class ODBC_API Connector: public Poco::Data::Connector
	/// Connector instantiates SqLite SessionImpl objects.
{
public:
	static const std::string KEY;
		/// Keyword for creating ODBC sessions

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
};


///
/// inlines
///

inline const std::string& Connector::name() const
{
	static const std::string n(POCO_DATA_ODBC_CONNECTOR_NAME);
	return n;
}


} } } // namespace Poco::Data::ODBC


// 
// Automatic Connector registration
// 

struct ODBC_API ODBCConnectorRegistrator
	/// Connector registering class.
	/// A global instance of this class is instantiated
	/// with sole purpose to automatically register the 
	/// ODBC connector with central Poco Data registry.
{
	ODBCConnectorRegistrator()
		/// Calls Poco::Data::ODBC::registerConnector();
	{
		Poco::Data::ODBC::Connector::registerConnector();
	}

	~ODBCConnectorRegistrator()
		/// Calls Poco::Data::ODBC::unregisterConnector();
	{
		try
		{
			Poco::Data::ODBC::Connector::unregisterConnector();
		}
		catch (...)
		{
			poco_unexpected();
		}
	}
};


#if !defined(POCO_NO_AUTOMATIC_LIB_INIT)
	#if defined(POCO_OS_FAMILY_WINDOWS) && !defined(__GNUC__)
		extern "C" const struct ODBC_API ODBCConnectorRegistrator pocoODBCConnectorRegistrator;
		#if defined(ODBC_EXPORTS)
			#if defined(_WIN64)
				#define POCO_DATA_ODBC_FORCE_SYMBOL(s) __pragma(comment (linker, "/export:"#s))
			#elif defined(_WIN32)
				#define POCO_DATA_ODBC_FORCE_SYMBOL(s) __pragma(comment (linker, "/export:_"#s))
			#endif
		#else  // !ODBC_EXPORTS
			#if defined(_WIN64)
				#define POCO_DATA_ODBC_FORCE_SYMBOL(s) __pragma(comment (linker, "/include:"#s))
			#elif defined(_WIN32)
				#define POCO_DATA_ODBC_FORCE_SYMBOL(s) __pragma(comment (linker, "/include:_"#s))
			#endif
		#endif // ODBC_EXPORTS
	#else // !POCO_OS_FAMILY_WINDOWS
			#define POCO_DATA_ODBC_FORCE_SYMBOL(s) extern "C" const struct ODBCConnectorRegistrator s;
	#endif // POCO_OS_FAMILY_WINDOWS
	POCO_DATA_ODBC_FORCE_SYMBOL(pocoODBCConnectorRegistrator)
#endif // POCO_NO_AUTOMATIC_LIB_INIT

// 
// End automatic Connector registration
// 


#endif // Data_ODBC_Connector_INCLUDED
