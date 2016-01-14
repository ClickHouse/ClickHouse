//
// ICMPPacketImpl.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/ICMPPacketImpl.h#1 $
//
// Library: Net
// Package: ICMP
// Module:  ICMPPacketImpl
//
// Definition of the ICMPPacketImpl class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_ICMPPacketImpl_INCLUDED
#define Net_ICMPPacketImpl_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Net/Socket.h"


namespace Poco {
namespace Net {


class Net_API ICMPPacketImpl
	/// This is the abstract class for ICMP packet implementations.
{
public:
	ICMPPacketImpl(int dataSize = 48);
		/// Constructor. Creates an ICMPPacketImpl.

	virtual ~ICMPPacketImpl();
		/// Destructor.

	const Poco::UInt8* packet(bool init = true);
		/// Returns raw ICMP packet.
		/// ICMP header and data are included in the packet.
		/// If init is true, initPacket() is called.

	virtual int packetSize() const = 0;
		/// Returns the total size of packet (ICMP header + data) in number of octets.
		/// Must be overriden.

	virtual int maxPacketSize() const;
		/// Returns the maximum permitted size of packet in number of octets.

	Poco::UInt16 sequence() const;
		/// Returns the most recent sequence number generated.

	void setDataSize(int dataSize);
		/// Sets data size.

	int getDataSize() const;
		/// Returns data size.

	virtual struct timeval time(Poco::UInt8* buffer = 0, int length = 0) const = 0;
		/// Returns current epoch time if either argument is equal to zero.
		/// Otherwise, it extracts the time value from the supplied buffer.
		/// 
		/// Supplied buffer includes IP header, ICMP header and data.
		/// Must be overriden.

	virtual bool validReplyID(unsigned char* buffer, int length) const = 0;
		/// Returns true if the extracted id is recognized 
		/// (i.e. equals the process id).
		///	
		/// Supplied buffer includes IP header, ICMP header and data.
		/// Must be overriden.

	virtual std::string errorDescription(Poco::UInt8* buffer, int length) = 0;
		/// Returns error description string.
		/// If supplied buffer contains an ICMP echo reply packet, an
		/// empty string is returned indicating the absence of error.
		///	
		/// Supplied buffer includes IP header, ICMP header and data.
		/// Must be overriden.

	virtual std::string typeDescription(int typeId) = 0;
		/// Returns the description of the packet type.
		/// Must be overriden.

	static const Poco::UInt16 MAX_PACKET_SIZE;
	static const Poco::UInt16 MAX_SEQ_VALUE;

protected:
	Poco::UInt16 nextSequence();
		/// Increments sequence number and returns the new value.

	void resetSequence();
		/// Resets the sequence to zero.

	virtual void initPacket() = 0;
		/// (Re)assembles the packet.
		/// Must be overriden.

	Poco::UInt16 checksum(Poco::UInt16 *addr, Poco::Int32 len);
		/// Calculates the checksum for supplied buffer.

private:
	Poco::UInt16 _seq;
	Poco::UInt8* _pPacket;
	int _dataSize;
	
};


//
// inlines
//
inline Poco::UInt16 ICMPPacketImpl::sequence() const
{
	return _seq;
}


inline Poco::UInt16 ICMPPacketImpl::nextSequence()
{
	return ++_seq;
}


inline void ICMPPacketImpl::resetSequence()
{
	_seq = 0;
}


inline int ICMPPacketImpl::maxPacketSize() const
{
	return MAX_PACKET_SIZE;
}


} } // namespace Poco::Net


#endif // Net_ICMPPacketImpl_INCLUDED
