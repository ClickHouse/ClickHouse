//
// ICMPPacket.h
//
// Library: Net
// Package: ICMP
// Module:  ICMPPacket
//
// Definition of the ICMPPacket class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_ICMPPacket_INCLUDED
#define Net_ICMPPacket_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Net/Socket.h"
#include "Poco/Net/ICMPPacketImpl.h"


namespace Poco {
namespace Net {


class Net_API ICMPPacket
	/// This class is the ICMP packet abstraction. 
{
public:
	ICMPPacket(SocketAddress::Family family, int dataSize = 48);
		/// Creates an ICMPPacket of specified family.

	~ICMPPacket();
		/// Destroys the ICMPPacket.

	const Poco::UInt8* packet();
		/// Returns raw ICMP packet. ICMP header and data are included in the returned packet.

	int packetSize() const;
		/// Returns the total length of packet (header + data);

	Poco::UInt16 sequence() const;
		/// Returns the most recent sequence number generated.

	void setDataSize(int dataSize);
		/// Sets data size.

	int getDataSize() const;
		/// Returns data size.

	int maxPacketSize() const;
		/// Returns the total length of packet (header + data);

	struct timeval time(Poco::UInt8* buffer = 0, int length = 0) const;
		/// Returns current epoch time if either buffer or length are equal to zero.
		/// Otherwise, it extracts the time value from the supplied buffer and 
		/// returns the extracted value.
		/// 
		/// Supplied buffer includes IP header, ICMP header and data.

	bool validReplyID(Poco::UInt8* buffer, int length) const;
		/// Returns true if the extracted id is recognized 
		/// (equals the process id).
		///	
		/// Supplied buffer includes IP header, ICMP header and data.

	std::string errorDescription(Poco::UInt8* buffer, int length);
		/// Returns error description string.
		/// If supplied buffer contains an ICMP echo reply packet, an
		/// empty string is returned indicating the absence of error.
		///	
		/// Supplied buffer includes IP header, ICMP header and data.

	std::string typeDescription(int typeId);
		/// Returns the description of the packet type.

private:
	ICMPPacketImpl* _pImpl;
};


} } // namespace Poco::Net


#endif // Net_ICMPPacket_INCLUDED
