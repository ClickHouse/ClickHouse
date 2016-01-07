//
// Net.cpp
//
// $Id: //poco/1.4/Net/src/Net.cpp#10 $
//
// Library: Net
// Package: NetCore
// Module:  NetCore
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/Net.h"


#include "Poco/Net/SocketDefs.h"
#include "Poco/Net/NetException.h"


namespace Poco {
namespace Net {


void Net_API initializeNetwork()
{
#if defined(POCO_OS_FAMILY_WINDOWS)
	WORD    version = MAKEWORD(2, 2);
	WSADATA data;
	if (WSAStartup(version, &data) != 0)
		throw NetException("Failed to initialize network subsystem");
#endif
}


void Net_API uninitializeNetwork()
{
#if defined(POCO_OS_FAMILY_WINDOWS)
	WSACleanup();
#endif
}


} } // namespace Poco::Net


#if defined(POCO_OS_FAMILY_WINDOWS) && !defined(POCO_NO_AUTOMATIC_LIB_INIT)

	struct NetworkInitializer
		/// Network initializer for windows statically
		/// linked library.
	{
		NetworkInitializer()
			/// Calls Poco::Net::initializeNetwork();
		{
			Poco::Net::initializeNetwork();
		}

		~NetworkInitializer()
			/// Calls Poco::Net::uninitializeNetwork();
		{
			try
			{
				Poco::Net::uninitializeNetwork();
			}
			catch (...)
			{
				poco_unexpected();
			}
		}
	};

	const NetworkInitializer pocoNetworkInitializer;

#endif
