//
// NTPEventArgs.cpp
//
// Library: Net
// Package: NTP
// Module:  NTPEventArgs
//
// Implementation of NTPEventArgs
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/NTPEventArgs.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/DNS.h"
#include "Poco/Exception.h"
#include "Poco/Net/NetException.h"


using Poco::IOException;
using Poco::InvalidArgumentException;


namespace Poco {
namespace Net {


NTPEventArgs::NTPEventArgs(const SocketAddress& address):
	_address(address), _packet()
{
}


NTPEventArgs::~NTPEventArgs()
{
}


std::string NTPEventArgs::hostName() const
{
	try
	{
		return DNS::resolve(_address.host().toString()).name();
	}
	catch (HostNotFoundException&) 
	{
	}
	catch (NoAddressFoundException&) 
	{
	}
	catch (DNSException&)
	{
	}
	catch (IOException&)
	{
	}
	return _address.host().toString();
}


std::string NTPEventArgs::hostAddress() const
{
	return _address.host().toString();
}


} } // namespace Poco::Net
