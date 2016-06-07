//
// ICMPPacket.cpp
//
// $Id: //poco/1.4/Net/src/ICMPPacket.cpp#2 $
//
// Library: Net
// Package: ICMP
// Module:  ICMPPacket
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/ICMPPacket.h"
#include "Poco/Net/ICMPv4PacketImpl.h"
#include "Poco/Net/NetException.h"
#include "Poco/Timestamp.h"
#include "Poco/Timespan.h"
#include "Poco/NumberFormatter.h"
#include <sstream>


using Poco::InvalidArgumentException;
using Poco::NotImplementedException;
using Poco::Timestamp;
using Poco::Timespan;
using Poco::NumberFormatter;
using Poco::UInt8;
using Poco::UInt16;
using Poco::Int32;


namespace Poco {
namespace Net {


ICMPPacket::ICMPPacket(IPAddress::Family family, int dataSize):_pImpl(0)
{
	if (family == IPAddress::IPv4)
		_pImpl = new ICMPv4PacketImpl(dataSize);
#if defined(POCO_HAVE_IPv6)
	else if (family == IPAddress::IPv6)
		throw NotImplementedException("ICMPv6 packets not implemented.");
#endif
	else throw InvalidArgumentException("Invalid or unsupported address family passed to ICMPPacket");
}


ICMPPacket::~ICMPPacket()
{
	delete _pImpl;
}


void ICMPPacket::setDataSize(int dataSize)
{
	_pImpl->setDataSize(dataSize);
}


int ICMPPacket::getDataSize() const
{
	return _pImpl->getDataSize();
}


int ICMPPacket::packetSize() const
{
	return _pImpl->packetSize();
}


int ICMPPacket::maxPacketSize() const
{
	return _pImpl->maxPacketSize();
}


const Poco::UInt8* ICMPPacket::packet()
{
	return _pImpl->packet();
}


struct timeval ICMPPacket::time(Poco::UInt8* buffer, int length) const
{
	return _pImpl->time(buffer, length);
}


bool ICMPPacket::validReplyID(Poco::UInt8* buffer, int length) const
{
	return _pImpl->validReplyID(buffer, length);
}


std::string ICMPPacket::errorDescription(Poco::UInt8* buffer, int length)
{
	return _pImpl->errorDescription(buffer, length);
}


std::string ICMPPacket::typeDescription(int typeId)
{
	return _pImpl->typeDescription(typeId);
}


} } // namespace Poco::Net
