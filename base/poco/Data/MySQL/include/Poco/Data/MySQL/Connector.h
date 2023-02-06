//
// Connector.h
//
// Library: Data/MySQL
// Package: MySQL
// Module:  Connector
//
// Definition of the Connector class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_MySQL_Connector_INCLUDED
#define Data_MySQL_Connector_INCLUDED


#include "Poco/Data/MySQL/MySQL.h"
#include "Poco/Data/Connector.h"


namespace Poco {
namespace Data {
namespace MySQL {


class MySQL_API Connector: public Poco::Data::Connector
	/// Connector instantiates MySQL SessionImpl objects.
{
public:

	static std::string KEY;

	Connector();
		/// Creates the Connector.
	
	virtual ~Connector();
		/// Destroys the Connector.

	virtual const std::string& name() const;
		/// Returns the name associated with this connector.

	virtual Poco::AutoPtr<Poco::Data::SessionImpl> createSession(const std::string& connectionString,
		std::size_t timeout = Poco::Data::SessionImpl::LOGIN_TIMEOUT_DEFAULT);
		/// Creates a MySQL SessionImpl object and initializes it with the given connectionString.

	static void registerConnector();
		/// Registers the Connector under the Keyword Connector::KEY at the Poco::Data::SessionFactory

	static void unregisterConnector();
		/// Unregisters the Connector under the Keyword Connector::KEY at the Poco::Data::SessionFactory
};


} } } // namespace Poco::Data::MySQL


#endif // Data_MySQL_Connector_INCLUDED
