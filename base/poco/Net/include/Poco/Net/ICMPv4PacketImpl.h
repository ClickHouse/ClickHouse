//
// ICMPv4PacketImpl.h
//
// Library: Net
// Package: ICMP
// Module:  ICMPv4PacketImpl
//
// Definition of the ICMPv4PacketImpl class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_ICMPv4PacketImpl_INCLUDED
#define Net_ICMPv4PacketImpl_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Net/Socket.h"
#include "Poco/Net/ICMPPacketImpl.h"
#include <cstddef>


namespace Poco {
namespace Net {


class Net_API ICMPv4PacketImpl : public ICMPPacketImpl
	/// This class implements the ICMPv4 packet.
	/// Parts are based on original ICMP code by
	/// Mike Muuss
	/// U. S. Army Ballistic Research Laboratory
 	/// December, 1983
{
public:
	// ICMPv4 header
	struct Header
	{
		Poco::UInt8 type;          // ICMP packet type
		Poco::UInt8 code;          // Type sub code
		Poco::UInt16 checksum;
		Poco::UInt16 id;
		Poco::UInt16 seq;
	};

	// compile-time shield against misalignment
#if POCO_OS != POCO_OS_ANDROID
	poco_static_assert (offsetof(Header, code) == 0x01);
	poco_static_assert (offsetof(Header, checksum) == 0x02);
	poco_static_assert (offsetof(Header, id) == 0x04);
	poco_static_assert (offsetof(Header, seq) == 0x06);
#endif

	enum MessageType
	{
		ECHO_REPLY,
		ICMP_1,
		ICMP_2,
		DESTINATION_UNREACHABLE,
		SOURCE_QUENCH,
		REDIRECT,
		ICMP_6,
		ICMP_7,
		ECHO_REQUEST,
		ICMP_9,
		ICMP_10,
		TIME_EXCEEDED,
		PARAMETER_PROBLEM,
		TIMESTAMP_REQUEST,
		TIMESTAMP_REPLY,
		INFORMATION_REQUEST,
		INFORMATION_REPLY,
		MESSAGE_TYPE_UNKNOWN,  // non-standard default, must remain last but one
		MESSAGE_TYPE_LENGTH    // length indicator, must remain last
	};

	enum DestinationUnreachableCode
	{
		NET_UNREACHABLE,
		HOST_UNREACHABLE,
		PROTOCOL_UNREACHABLE,
		PORT_UNREACHABLE,
		FRAGMENTATION_NEEDED_AND_DF_SET,
		SOURCE_ROUTE_FAILED,
		DESTINATION_UNREACHABLE_UNKNOWN, // non-standard default, must remain last but one
		DESTINATION_UNREACHABLE_LENGTH   // length indicator, must remain last
	};

	enum RedirectMessageCode
	{
		REDIRECT_NETWORK,
		REDIRECT_HOST,
		REDIRECT_SERVICE_NETWORK,
		REDIRECT_SERVICE_HOST,
		REDIRECT_MESSAGE_UNKNOWN, // non-standard default, must remain last but one
		REDIRECT_MESSAGE_LENGTH   // length indicator, must remain last
	};

	enum TimeExceededCode
	{
		TIME_TO_LIVE,
		FRAGMENT_REASSEMBLY,
		TIME_EXCEEDED_UNKNOWN, // non-standard default, must remain last but one
		TIME_EXCEEDED_LENGTH   // length indicator, must remain last
	};

	enum ParameterProblemCode
	{
		POINTER_INDICATES_THE_ERROR,
		PARAMETER_PROBLEM_UNKNOWN, // non-standard default, must remain last but one
		PARAMETER_PROBLEM_LENGTH   // length indicator, must remain last
	};

	ICMPv4PacketImpl(int dataSize = 48);
		/// Constructor. Creates an ICMPv4PacketImpl.

	~ICMPv4PacketImpl();
		/// Destructor.

	int packetSize() const;
		/// Returns the total length of packet (header + data);

	struct timeval time(Poco::UInt8* buffer = 0, int length = 0) const;
		/// Returns current epoch time if either buffer or length are equal to zero.
		/// Otherwise, it extracts the time value from the supplied buffer.
		///
		/// Buffer includes IP header, ICMP header and data.

	bool validReplyID(Poco::UInt8* buffer, int length) const;
		/// Returns true if the extracted id is recognized
		/// (i.e. equals the process id).
		///
		/// Buffer includes IP header, ICMP header and data.

	virtual std::string errorDescription(Poco::UInt8* buffer, int length);
		/// Returns error description string.
		/// If supplied buffer contains ICMPv4 echo reply packet, an
		/// empty string is returned indicating the absence of error.
		///
		/// Buffer includes IP header, ICMP header and data.

	virtual std::string typeDescription(int typeId);
		/// Returns the description of the packet type.

	static const Poco::UInt16 MAX_PACKET_SIZE;
	static const std::string MESSAGE_TYPE[MESSAGE_TYPE_LENGTH];
	static const Poco::UInt8 DESTINATION_UNREACHABLE_TYPE; // 3
	static const Poco::UInt8 SOURCE_QUENCH_TYPE;           // 4
	static const Poco::UInt8 REDIRECT_MESSAGE_TYPE;        // 5
	static const Poco::UInt8 TIME_EXCEEDED_TYPE;           // 11
	static const Poco::UInt8 PARAMETER_PROBLEM_TYPE;       // 12

private:
	void initPacket();
	Header* header(Poco::UInt8* buffer, int length) const;
	Poco::UInt8* data(Poco::UInt8* buffer, int length) const;

	static const std::string DESTINATION_UNREACHABLE_CODE[DESTINATION_UNREACHABLE_LENGTH];
	static const std::string REDIRECT_MESSAGE_CODE[REDIRECT_MESSAGE_LENGTH];
	static const std::string TIME_EXCEEDED_CODE[TIME_EXCEEDED_LENGTH];
	static const std::string PARAMETER_PROBLEM_CODE[PARAMETER_PROBLEM_LENGTH];

	Poco::UInt16 _seq;
};


} } // namespace Poco::Net


#endif // Net_ICMPv4PacketImpl_INCLUDED
