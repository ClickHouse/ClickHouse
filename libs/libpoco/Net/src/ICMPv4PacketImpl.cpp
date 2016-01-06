//
// ICMPv4PacketImpl.cpp
//
// $Id: //poco/1.4/Net/src/ICMPv4PacketImpl.cpp#2 $
//
// Library: Net
// Package: ICMP
// Module:  ICMPv4PacketImpl
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/ICMPv4PacketImpl.h"
#include "Poco/Net/NetException.h"
#include "Poco/Timestamp.h"
#include "Poco/Timespan.h"
#include "Poco/NumberFormatter.h"
#if !defined(POCO_VXWORKS)
#include "Poco/Process.h"
#endif
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


const UInt8 ICMPv4PacketImpl::DESTINATION_UNREACHABLE_TYPE       = 3;
const Poco::UInt8 ICMPv4PacketImpl::SOURCE_QUENCH_TYPE     = 4;
const Poco::UInt8 ICMPv4PacketImpl::REDIRECT_MESSAGE_TYPE  = 5;
const UInt8 ICMPv4PacketImpl::TIME_EXCEEDED_TYPE                 = 11;
const Poco::UInt8 ICMPv4PacketImpl::PARAMETER_PROBLEM_TYPE = 12;


const std::string ICMPv4PacketImpl::MESSAGE_TYPE[] = 
{
	"Echo Reply",
	"ICMP 1",
	"ICMP 2",
	"Dest Unreachable",
	"Source Quench",
	"Redirect",
	"ICMP 6",
	"ICMP 7",
	"Echo",
	"ICMP 9",
	"ICMP 10",
	"Time Exceeded",
	"Parameter Problem",
	"Timestamp",
	"Timestamp Reply",
	"Info Request",
	"Info Reply",
	"Unknown type"
};


const std::string ICMPv4PacketImpl::DESTINATION_UNREACHABLE_CODE[] = 
{
	"Net unreachable",
	"Host unreachable",
	"Protocol unreachable",
	"Port unreachable",
	"Fragmentation needed and DF set",
	"Source route failed",
	"Unknown code"
};


const std::string ICMPv4PacketImpl::REDIRECT_MESSAGE_CODE[] = 
{
	"Redirect datagrams for the network",
	"Redirect datagrams for the host",
	"Redirect datagrams for the type of service and network",
	"Redirect datagrams for the type of service and host",
	"Unknown code"
};


const std::string ICMPv4PacketImpl::TIME_EXCEEDED_CODE[] = 
{
	"Time to live exceeded in transit",
	"Fragment reassembly time exceeded",
	"Unknown code"
};


const std::string ICMPv4PacketImpl::PARAMETER_PROBLEM_CODE[] = 
{
	"Pointer indicates the error",
	"Unknown code"
};


ICMPv4PacketImpl::ICMPv4PacketImpl(int dataSize)
	: ICMPPacketImpl(dataSize),
	_seq(0)
{
	initPacket();
}


ICMPv4PacketImpl::~ICMPv4PacketImpl()
{
}


int ICMPv4PacketImpl::packetSize() const
{
	return getDataSize() + sizeof(Header);
}


void ICMPv4PacketImpl::initPacket()
{
	if (_seq >= MAX_SEQ_VALUE) resetSequence();

	Header* icp = (Header*) packet(false);
	icp->type     = ECHO_REQUEST;
	icp->code     = 0;
	icp->checksum = 0;
	icp->seq      = ++_seq;
#if defined(POCO_VXWORKS)
	icp->id       = 0;
#else
	icp->id       = static_cast<UInt16>(Poco::Process::id());
#endif

	struct timeval* ptp = (struct timeval *) (icp + 1);
	*ptp = time();

	icp->checksum = checksum((UInt16*) icp, getDataSize() + sizeof(Header));
}


struct timeval ICMPv4PacketImpl::time(Poco::UInt8* buffer, int length) const
{
	struct timeval tv;

	if (0 == buffer || 0 == length)
	{
		Timespan value(Timestamp().epochMicroseconds());
		tv.tv_sec  = (long) value.totalSeconds();
		tv.tv_usec = (long) value.useconds();
	}
	else
	{
		struct timeval* ptv = (struct timeval*) data(buffer, length);
		if (ptv) tv = *ptv;
		else throw InvalidArgumentException("Invalid packet.");
	}
	return tv;
}


ICMPv4PacketImpl::Header* ICMPv4PacketImpl::header(Poco::UInt8* buffer, int length) const
{
	poco_check_ptr (buffer);

	int offset = (buffer[0] & 0x0F) * 4;
	if ((offset + sizeof(Header)) > length) return 0;

	buffer += offset;
	return (Header *) buffer;
}


Poco::UInt8* ICMPv4PacketImpl::data(Poco::UInt8* buffer, int length) const
{
	return ((Poco::UInt8*) header(buffer, length)) + sizeof(Header);
}


bool ICMPv4PacketImpl::validReplyID(Poco::UInt8* buffer, int length) const
{
	Header *icp = header(buffer, length);
#if defined(POCO_VXWORKS)
	return icp && icp->id == 0;
#else
	return icp && (static_cast<Poco::UInt16>(Process::id()) == icp->id);
#endif
}


std::string ICMPv4PacketImpl::errorDescription(unsigned char* buffer, int length)
{
	Header *icp = header(buffer, length);

	if (!icp) return "Invalid header.";
	if (ECHO_REPLY == icp->type) return std::string(); // not an error

	UInt8 pointer = 0;
	if (PARAMETER_PROBLEM == icp->type)
	{
		UInt8 mask = 0x00FF;
		pointer = icp->id & mask;
	}

	MessageType type = static_cast<MessageType>(icp->type);
	int code = icp->code;
	std::ostringstream err;

	switch (type)
	{
	case DESTINATION_UNREACHABLE_TYPE:
		if (code >= NET_UNREACHABLE && code < DESTINATION_UNREACHABLE_UNKNOWN)
			err << DESTINATION_UNREACHABLE_CODE[code];
		else
			err << DESTINATION_UNREACHABLE_CODE[DESTINATION_UNREACHABLE_UNKNOWN];
		break;
	
	case SOURCE_QUENCH_TYPE:		
		err << "Source quench";
		break;
	
	case REDIRECT_MESSAGE_TYPE:
		if (code >= REDIRECT_NETWORK && code < REDIRECT_MESSAGE_UNKNOWN) 
			err << REDIRECT_MESSAGE_CODE[code];
		else
			err << REDIRECT_MESSAGE_CODE[REDIRECT_MESSAGE_UNKNOWN];
		break;

	case TIME_EXCEEDED_TYPE:
		if (code >= TIME_TO_LIVE || code < TIME_EXCEEDED_UNKNOWN)
			err << TIME_EXCEEDED_CODE[code];
		else
			err << TIME_EXCEEDED_CODE[TIME_EXCEEDED_UNKNOWN];
		break;
	
	case PARAMETER_PROBLEM_TYPE:
		if (POINTER_INDICATES_THE_ERROR != code)
			code = PARAMETER_PROBLEM_UNKNOWN;
		err << PARAMETER_PROBLEM_CODE[code] << ": error in octet #" << pointer;
		break;
	
	default:
		err << "Unknown type.";
		break;
	}

	return err.str();
}

std::string ICMPv4PacketImpl::typeDescription(int typeId)
{
	poco_assert (typeId >= ECHO_REPLY && typeId < MESSAGE_TYPE_LENGTH);

	return MESSAGE_TYPE[typeId];
}


} } // namespace Poco::Net
