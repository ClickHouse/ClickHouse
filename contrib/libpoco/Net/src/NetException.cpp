//
// NetException.cpp
//
// $Id: //poco/1.4/Net/src/NetException.cpp#4 $
//
// Library: Net
// Package: NetCore
// Module:  NetException
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/NetException.h"
#include <typeinfo>


using Poco::IOException;


namespace Poco {
namespace Net {


POCO_IMPLEMENT_EXCEPTION(NetException, IOException, "Net Exception")
POCO_IMPLEMENT_EXCEPTION(InvalidAddressException, NetException, "Invalid address")
POCO_IMPLEMENT_EXCEPTION(InvalidSocketException, NetException, "Invalid socket")
POCO_IMPLEMENT_EXCEPTION(ServiceNotFoundException, NetException, "Service not found")
POCO_IMPLEMENT_EXCEPTION(ConnectionAbortedException, NetException, "Software caused connection abort")
POCO_IMPLEMENT_EXCEPTION(ConnectionResetException, NetException, "Connection reset by peer")
POCO_IMPLEMENT_EXCEPTION(ConnectionRefusedException, NetException, "Connection refused")
POCO_IMPLEMENT_EXCEPTION(DNSException, NetException, "DNS error")
POCO_IMPLEMENT_EXCEPTION(HostNotFoundException, DNSException, "Host not found")
POCO_IMPLEMENT_EXCEPTION(NoAddressFoundException, DNSException, "No address found")
POCO_IMPLEMENT_EXCEPTION(InterfaceNotFoundException, NetException, "Interface not found")
POCO_IMPLEMENT_EXCEPTION(NoMessageException, NetException, "No message received")
POCO_IMPLEMENT_EXCEPTION(MessageException, NetException, "Malformed message")
POCO_IMPLEMENT_EXCEPTION(MultipartException, MessageException, "Malformed multipart message")
POCO_IMPLEMENT_EXCEPTION(HTTPException, NetException, "HTTP Exception")
POCO_IMPLEMENT_EXCEPTION(NotAuthenticatedException, HTTPException, "No authentication information found")
POCO_IMPLEMENT_EXCEPTION(UnsupportedRedirectException, HTTPException, "Unsupported HTTP redirect (protocol change)")
POCO_IMPLEMENT_EXCEPTION(FTPException, NetException, "FTP Exception")
POCO_IMPLEMENT_EXCEPTION(SMTPException, NetException, "SMTP Exception")
POCO_IMPLEMENT_EXCEPTION(POP3Exception, NetException, "POP3 Exception")
POCO_IMPLEMENT_EXCEPTION(ICMPException, NetException, "ICMP Exception")
POCO_IMPLEMENT_EXCEPTION(NTPException, NetException, "NTP Exception")
POCO_IMPLEMENT_EXCEPTION(HTMLFormException, NetException, "HTML Form Exception")
POCO_IMPLEMENT_EXCEPTION(WebSocketException, NetException, "WebSocket Exception")
POCO_IMPLEMENT_EXCEPTION(UnsupportedFamilyException, NetException, "Unknown or unsupported socket family.")


} } // namespace Poco::Net
