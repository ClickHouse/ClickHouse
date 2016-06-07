//
// Utility.h
//
// $Id: //poco/Main/Data/MySQL/include/Poco/Data/MySQL/Utility.h#2 $
//
// Library: MySQL
// Package: MySQL
// Module:  Utility
//
// Definition of Utility.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MySQL_Utility_INCLUDED
#define MySQL_Utility_INCLUDED


#include "Poco/Data/MySQL/MySQL.h"
#include "Poco/Data/Session.h"


struct st_mysql;
typedef struct st_mysql MYSQL;


namespace Poco {
namespace Data {
namespace MySQL {


class MySQL_API Utility
	/// Various utility functions for MySQL.
{
public:

	static std::string serverInfo(MYSQL* pHandle);
		/// Returns server info.

	static std::string serverInfo(Poco::Data::Session& session);
		/// Returns server info.

	static unsigned long serverVersion(MYSQL* pHandle);
		/// Returns server version.

	static unsigned long serverVersion(Poco::Data::Session& session);
		/// Returns server version.

	static std::string hostInfo(MYSQL* pHandle);
		/// Returns host info.

	static std::string hostInfo(Poco::Data::Session& session);
		/// Returns host info.

	static bool hasMicrosecond();
		/// Rturns true if microseconds are suported.

	static MYSQL* handle(Poco::Data::Session& session);
		/// Returns native MySQL handle for the session.
};


//
// inlines
//


inline MYSQL* Utility::handle(Session& session)
{
	return Poco::AnyCast<MYSQL*>(session.getProperty("handle"));
}


} } } // namespace Poco::Data::MySQL


#endif // MySQL_Utility_INCLUDED
