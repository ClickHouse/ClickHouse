//
// NTPPacket.cpp
//
// Library: Net
// Package: NTP
// Module:  NTPPacket
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/NTPPacket.h"
#include "Poco/Net/NetException.h"
#include "Poco/Timestamp.h"
#include "Poco/ByteOrder.h"


namespace Poco {
namespace Net {


#if !defined(POCO_COMPILER_SUN)
#pragma pack(push, 1)
#else
#pragma pack(1)
#endif
struct NTPPacketData 
{
	Poco::Int8 mode:3;
	Poco::Int8 vn:3;
	Poco::Int8 li:2;
	Poco::Int8 stratum;
	Poco::Int8 pool;
	Poco::Int8 prec;
	Poco::Int32 rootdelay;
	Poco::Int32 rootdisp;
	Poco::Int32 refid;
	Poco::Int64 rts;
	Poco::Int64 ots;
	Poco::Int64 vts;
	Poco::Int64 tts;
};
#if !defined(POCO_COMPILER_SUN)
#pragma pack(pop)
#else
#pragma pack()
#endif


NTPPacket::NTPPacket() :
	// the next 3 fields must be in reverse order from spec
	_leapIndicator(3),
	_version(4),
	_mode(3),

	_stratum(0),
	_pool(6),
	_precision(-18),
	_rootDelay(0),
	_rootDispersion(0),
	_referenceId(0),
	_referenceTimestamp(0),
	_receiveTimestamp(0),
	_transmitTimestamp(0)
{
	Poco::Timestamp ts;
	_originateTimestamp = ts.utcTime() - 2874597888;
}


NTPPacket::NTPPacket(Poco::UInt8 *packet)
{
	setPacket(packet);
}


NTPPacket::~NTPPacket()
{
}


void NTPPacket::packet(Poco::UInt8 *packet) const
{
	NTPPacketData *p = (NTPPacketData*)packet;

	p->li = _leapIndicator;
	p->vn = _version;
	p->mode = _mode;
	p->stratum = _stratum;
	p->pool = _pool;
	p->prec = _precision;
	p->rootdelay = Poco::ByteOrder::toNetwork(_rootDelay);
	p->rootdisp = Poco::ByteOrder::toNetwork(_rootDispersion);
	p->refid = Poco::ByteOrder::toNetwork(_referenceId);
	p->rts = Poco::ByteOrder::toNetwork(_referenceTimestamp);
	p->ots = Poco::ByteOrder::toNetwork(_originateTimestamp);
	p->vts = Poco::ByteOrder::toNetwork(_receiveTimestamp);
	p->tts = Poco::ByteOrder::toNetwork(_transmitTimestamp);
}


void NTPPacket::setPacket(Poco::UInt8 *packet)
{
	NTPPacketData *p = (NTPPacketData*)packet;

	_leapIndicator = p->li;
	_version = p->vn;
	_mode = p->mode;
	_stratum = p->stratum;
	_pool = p->pool;
	_precision = p->prec;
	_rootDelay = Poco::ByteOrder::fromNetwork(p->rootdelay);
	_rootDispersion = Poco::ByteOrder::fromNetwork(p->rootdisp);
	_referenceId = Poco::ByteOrder::fromNetwork(p->refid);
	_referenceTimestamp = Poco::ByteOrder::fromNetwork(p->rts);
	_originateTimestamp = Poco::ByteOrder::fromNetwork(p->ots);
	_receiveTimestamp = Poco::ByteOrder::fromNetwork(p->vts);
	_transmitTimestamp = Poco::ByteOrder::fromNetwork(p->tts);
}


Poco::Timestamp NTPPacket::referenceTime() const
{
	return convertTime(_referenceTimestamp);
}


Poco::Timestamp NTPPacket::originateTime() const
{
	return convertTime(_originateTimestamp);
}


Poco::Timestamp NTPPacket::receiveTime() const
{
	return convertTime(_receiveTimestamp);
}


Poco::Timestamp NTPPacket::transmitTime() const
{
	return convertTime(_transmitTimestamp);
}


Poco::Timestamp NTPPacket::convertTime(Poco::Int64 tm) const
{
	const unsigned long seventyYears = 2208988800UL;
	Poco::UInt32 secsSince1900 = UInt32(Poco::ByteOrder::toLittleEndian(tm) >> 32);
	unsigned long epoch = secsSince1900 - seventyYears;
	return Poco::Timestamp::fromEpochTime(epoch);
}


} } // namespace Poco::Net
