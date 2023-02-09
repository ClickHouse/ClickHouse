//
// NetException.h
//
// Library: Net
// Package: NetCore
// Module:  NetException
//
// Definition of the NetException class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_NetException_INCLUDED
#define Net_NetException_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Exception.h"


namespace Poco {
namespace Net {


POCO_DECLARE_EXCEPTION(Net_API, NetException, Poco::IOException)
POCO_DECLARE_EXCEPTION(Net_API, InvalidAddressException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, InvalidSocketException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, ServiceNotFoundException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, ConnectionAbortedException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, ConnectionResetException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, ConnectionRefusedException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, DNSException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, HostNotFoundException, DNSException)
POCO_DECLARE_EXCEPTION(Net_API, NoAddressFoundException, DNSException)
POCO_DECLARE_EXCEPTION(Net_API, InterfaceNotFoundException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, NoMessageException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, MessageException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, MultipartException, MessageException)
POCO_DECLARE_EXCEPTION(Net_API, HTTPException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, NotAuthenticatedException, HTTPException)
POCO_DECLARE_EXCEPTION(Net_API, UnsupportedRedirectException, HTTPException)
POCO_DECLARE_EXCEPTION(Net_API, FTPException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, SMTPException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, POP3Exception, NetException)
POCO_DECLARE_EXCEPTION(Net_API, ICMPException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, NTPException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, HTMLFormException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, WebSocketException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, UnsupportedFamilyException, NetException)
POCO_DECLARE_EXCEPTION(Net_API, AddressFamilyMismatchException, NetException)


} } // namespace Poco::Net


#endif // Net_NetException_INCLUDED
