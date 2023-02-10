//
// NTPPacket.h
//
// Library: Net
// Package: NTP
// Module:  NTPPacket
//
// Definition of the NTPPacket class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_NTPPacket_INCLUDED
#define Net_NTPPacket_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Net/Net.h"
#include "Poco/Timestamp.h"

namespace Poco {
namespace Net {


class Net_API NTPPacket
	/// This class is the NTP packet abstraction. 
{
public:
	NTPPacket();
		/// Creates an NTPPacket.

	NTPPacket(Poco::UInt8 *packet);
		/// Creates an NTPPacket.
		///
		/// Assumed to have at least 48 bytes.

	~NTPPacket();
		/// Destroys the NTPPacket.

	void packet(Poco::UInt8 *packet) const;
		/// Returns the NTP packet.
		///
		/// Assumed to have at least 48 bytes.

	void setPacket(Poco::UInt8 *packet);
		/// Returns the NTP packet.
		///
		/// Assumed to have exactly 48 bytes.

	Poco::Int8 leapIndicator() const;
		/// Returns the leap indicator.

	Poco::Int8 version() const;
		/// Returns the version.

	Poco::Int8 mode() const;
		/// Returns the mode.

	Poco::Int8 stratum() const;
		/// Returns the stratum.

	Poco::Int8 pool() const;
		/// Returns the pool.

	Poco::Int8 precision() const;
		/// Returns the precision

	Poco::Int32 rootDelay() const;
		/// Returns the root delay

	Poco::Int32 rootDispersion() const;
		/// Returns the root dispersion

	Poco::Int32 referenceId() const;
		/// Returns the reference id

	Poco::Int64 referenceTimestamp() const;
		/// Returns the reference timestamp

	Poco::Int64 originateTimestamp() const;
		/// Returns the originate timestamp

	Poco::Int64 receiveTimestamp() const;
		/// Returns the receive timestamp

	Poco::Int64 transmitTimestamp() const;
		/// Returns the transmit timestamp

	Poco::Timestamp referenceTime() const;
		/// Returns the reference time

	Poco::Timestamp originateTime() const;
		/// Returns the originate time

	Poco::Timestamp receiveTime() const;
		/// Returns the receive time

	Poco::Timestamp transmitTime() const;
		/// Returns the transmit time
private:
	Poco::Timestamp convertTime(Poco::Int64 tm) const;

	Poco::Int8 _leapIndicator;
	Poco::Int8 _version;
	Poco::Int8 _mode;
	Poco::Int8 _stratum;
	Poco::Int8 _pool;
	Poco::Int8 _precision;
	Poco::Int32 _rootDelay;
	Poco::Int32 _rootDispersion;
	Poco::Int32 _referenceId;
	Poco::Int64 _referenceTimestamp;
	Poco::Int64 _originateTimestamp;
	Poco::Int64 _receiveTimestamp;
	Poco::Int64 _transmitTimestamp;
};


//
// inlines
//
inline Poco::Int8 NTPPacket::leapIndicator() const
{
	return _leapIndicator;
}


inline Poco::Int8 NTPPacket::version() const
{
	return _version;
}


inline Poco::Int8 NTPPacket::mode() const
{
	return _mode;
}


inline Poco::Int8 NTPPacket::stratum() const
{
	return _stratum;
}


inline Poco::Int8 NTPPacket::pool() const
{
	return _pool;
}


inline Poco::Int8 NTPPacket::precision() const
{
	return _precision;
}


inline Poco::Int32 NTPPacket::rootDelay() const
{
	return _rootDelay;
}


inline Poco::Int32 NTPPacket::rootDispersion() const
{
	return _rootDispersion;
}


inline Poco::Int32 NTPPacket::referenceId() const
{
	return _referenceId;
}


inline Poco::Int64 NTPPacket::referenceTimestamp() const
{
	return _referenceTimestamp;
}


inline Poco::Int64 NTPPacket::originateTimestamp() const
{
	return _originateTimestamp;
}


inline Poco::Int64 NTPPacket::receiveTimestamp() const
{
	return _receiveTimestamp;
}


inline Poco::Int64 NTPPacket::transmitTimestamp() const
{
	return _transmitTimestamp;
}


} } // namespace Poco::Net


#endif // Net_NTPPacket_INCLUDED
