//
// ICMPPacketImpl.cpp
//
// $Id: //poco/1.4/Net/src/ICMPPacketImpl.cpp#2 $
//
// Library: Net
// Package: ICMP
// Module:  ICMPPacketImpl
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/ICMPPacketImpl.h"
#include "Poco/Net/NetException.h"
#include "Poco/Timestamp.h"
#include "Poco/Timespan.h"
#include "Poco/NumberFormatter.h"
#include <sstream>


using Poco::InvalidArgumentException;
using Poco::Timestamp;
using Poco::Timespan;
using Poco::NumberFormatter;
using Poco::UInt8;
using Poco::UInt16;
using Poco::Int32;


namespace Poco {
namespace Net {


const UInt16 ICMPPacketImpl::MAX_PACKET_SIZE = 4096;
const UInt16 ICMPPacketImpl::MAX_SEQ_VALUE   = 65535;


ICMPPacketImpl::ICMPPacketImpl(int dataSize):
	_seq(0),
	_pPacket(new UInt8[MAX_PACKET_SIZE]),
	_dataSize(dataSize)
{
	if (_dataSize > MAX_PACKET_SIZE)
		throw InvalidArgumentException("Packet size must be <= " + NumberFormatter::format(MAX_PACKET_SIZE));
}


ICMPPacketImpl::~ICMPPacketImpl()
{
	delete [] _pPacket;
}


void ICMPPacketImpl::setDataSize(int dataSize)
{
	_dataSize = dataSize;
	initPacket();
}


int ICMPPacketImpl::getDataSize() const
{
	return _dataSize;
}


const Poco::UInt8* ICMPPacketImpl::packet(bool init)
{
	if (init) initPacket();
	return _pPacket;
}


unsigned short ICMPPacketImpl::checksum(UInt16 *addr, Int32 len)
{
	Int32 nleft = len;
	UInt16* w   = addr;
	UInt16 answer;
	Int32 sum = 0;

	while (nleft > 1)  
	{
		sum   += *w++;
		nleft -= sizeof(UInt16);
	}

	if (nleft == 1) 
	{
		UInt16 u = 0;
		*(UInt8*) (&u) = *(UInt8*) w;
		sum += u;
	}

	sum = (sum >> 16) + (sum & 0xffff);
	sum += (sum >> 16);
	answer = ~sum;
	return answer;
}


} } // namespace Poco::Net
