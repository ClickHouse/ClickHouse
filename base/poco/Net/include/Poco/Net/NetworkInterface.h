//
// NetworkInterface.h
//
// Library: Net
// Package: Sockets
// Module:  NetworkInterface
//
// Definition of the NetworkInterface class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_NetworkInterface_INCLUDED
#define Net_NetworkInterface_INCLUDED


#include "Poco/Net/Net.h"


#ifdef POCO_NET_HAS_INTERFACE


#include "Poco/Net/IPAddress.h"
#include "Poco/Mutex.h"
#include "Poco/Tuple.h"
#include <map>
#include <ostream>


namespace Poco {
namespace Net {


class NetworkInterfaceImpl;


class Net_API NetworkInterface
	/// This class represents a network interface.
	/// 
	/// NetworkInterface is used with MulticastSocket to specify
	/// multicast interfaces for sending and receiving multicast
	/// messages.
	/// 
	/// The class also provides static member functions for
	/// enumerating or searching network interfaces and their
	/// respective configuration values.
	///
	/// On Windows, detection capabilities vary depending on the
	/// OS version/service pack. Although the best effort is made
	/// not to attempt access to non-existent features through a 
	/// combination of compile/runtime checks, when running binaries
	/// compiled on a newer version of the OS on an older one
	/// problems may occur; if possible, it is best to run
	/// binaries on the OS version where they were compiled.
	/// This particularly applies to OS versions older than Vista
	/// and XP.
{
public:
	typedef std::vector<NetworkInterface>                List;
	typedef List                                         NetworkInterfaceList;//@deprecated
	typedef std::map<unsigned, NetworkInterface>         Map;
	typedef Poco::Tuple<IPAddress, IPAddress, IPAddress> AddressTuple;
	typedef std::vector<AddressTuple>                    AddressList;
	typedef AddressList::iterator                        AddressIterator;
	typedef AddressList::const_iterator                  ConstAddressIterator;
	typedef std::vector<unsigned char>                   MACAddress;

	enum AddressType
	{
		IP_ADDRESS,
		SUBNET_MASK,
		BROADCAST_ADDRESS
	};

	enum Type
	{
		NI_TYPE_ETHERNET_CSMACD,
		NI_TYPE_ISO88025_TOKENRING,
		NI_TYPE_FRAMERELAY,
		NI_TYPE_PPP,
		NI_TYPE_SOFTWARE_LOOPBACK,
		NI_TYPE_ATM,
		NI_TYPE_IEEE80211,
		NI_TYPE_TUNNEL,
		NI_TYPE_IEEE1394,
		NI_TYPE_OTHER
	};

	enum IPVersion
	{
		IPv4_ONLY,    /// Return interfaces with IPv4 address only
		IPv6_ONLY,    /// Return interfaces with IPv6 address only
		IPv4_OR_IPv6  /// Return interfaces with IPv4 or IPv6 address
	};

	static const unsigned NO_INDEX = ~0;
#if defined(POCO_OS_FAMILY_WINDOWS)
	static const char MAC_SEPARATOR = '-';
#else
	static const char MAC_SEPARATOR = ':';
#endif

	NetworkInterface(unsigned index = NO_INDEX);
		/// Creates a NetworkInterface representing the
		/// default interface.
		///
		/// The name is empty, the IP address is the wildcard
		/// address and the index is max value of unsigned integer.
	
	NetworkInterface(const NetworkInterface& interfc);
		/// Creates the NetworkInterface by copying another one.

	~NetworkInterface();
		/// Destroys the NetworkInterface.

	NetworkInterface& operator = (const NetworkInterface& interfc);
		/// Assigns another NetworkInterface.
	
	bool operator < (const NetworkInterface& other) const;
		/// Operator less-than.
	
	bool operator == (const NetworkInterface& other) const;
		/// Operator equal. Compares interface indices.

	void swap(NetworkInterface& other);
		/// Swaps the NetworkInterface with another one.	
		
	unsigned index() const;
		/// Returns the interface OS index.
		
	const std::string& name() const;
		/// Returns the interface name.
		
	const std::string& displayName() const;
		/// Returns the interface display name.
		///
		/// On Windows platforms, this is currently the network adapter
		/// name. This may change to the "friendly name" of the network
		/// connection in a future version, however. 
		///
		/// On other platforms this is the same as name().

	const std::string& adapterName() const;
		/// Returns the interface adapter name.
		///
		/// On Windows platforms, this is the network adapter LUID.
		/// The adapter name is used by some Windows Net APIs like DHCP. 
		///
		/// On other platforms this is the same as name().

	const IPAddress& firstAddress(IPAddress::Family family) const;
		/// Returns the first IP address bound to the interface.
		/// Throws NotFoundException if the address family is not
		/// configured on the interface.

	void firstAddress(IPAddress& addr, IPAddress::Family family = IPAddress::IPv4) const;
		/// Returns the first IP address bound to the interface.
		/// If the address family is not configured on the interface,
		/// the address returned in addr will be unspecified (wildcard).

	const IPAddress& address(unsigned index = 0) const;
		/// Returns the IP address bound to the interface at index position.

	void addAddress(const IPAddress& address);
		/// Adds address to the interface.

	void addAddress(const IPAddress& address, const IPAddress& subnetMask, const IPAddress& broadcastAddress);
		/// Adds address to the interface.

	const AddressList& addressList() const;
		/// Returns the list of IP addresses bound to the interface.

	const IPAddress& subnetMask(unsigned index = 0) const;
		/// Returns the subnet mask for this network interface.

	const IPAddress& broadcastAddress(unsigned index = 0) const;
		/// Returns the broadcast address for this network interface.

	const IPAddress& destAddress(unsigned index = 0) const;
		/// Returns the IPv4 point-to-point destination address for this network interface.

	const MACAddress& macAddress() const;
		/// Returns MAC (Media Access Control) address for the interface.

	unsigned mtu() const;
		/// Returns the MTU for this interface.

	NetworkInterface::Type type() const;
		/// returns the MIB IfType of the interface.

	bool supportsIP() const;
		/// Returns true if the interface supports IP.

	bool supportsIPv4() const;
		/// Returns true if the interface supports IPv4.

	bool supportsIPv6() const;
		/// Returns true if the interface supports IPv6.	

	bool supportsBroadcast() const;
		/// Returns true if the interface supports broadcast.

	bool supportsMulticast() const;
		/// Returns true if the interface supports multicast.

	bool isLoopback() const;
		/// Returns true if the interface is loopback.

	bool isPointToPoint() const;
		/// Returns true if the interface is point-to-point.

	bool isRunning() const;
		/// Returns true if the interface is running.

	bool isUp() const;
		/// Returns true if the interface is up.

	static NetworkInterface forName(const std::string& name, bool requireIPv6 = false);
		/// Returns the NetworkInterface for the given name.
		/// 
		/// If requireIPv6 is false, an IPv4 interface is returned.
		/// Otherwise, an IPv6 interface is returned.
		///
		/// Throws an InterfaceNotFoundException if an interface
		/// with the give name does not exist.

	static NetworkInterface forName(const std::string& name, IPVersion ipVersion);
		/// Returns the NetworkInterface for the given name.
		/// 
		/// The ipVersion argument can be used to specify whether
		/// an IPv4 (IPv4_ONLY) or IPv6 (IPv6_ONLY) interface is required, 
		/// or whether the caller does not care (IPv4_OR_IPv6).
		///
		/// Throws an InterfaceNotFoundException if an interface
		/// with the give name does not exist.
		
	static NetworkInterface forAddress(const IPAddress& address);
		/// Returns the NetworkInterface for the given IP address.
		///
		/// Throws an InterfaceNotFoundException if an interface
		/// with the give address does not exist.

	static NetworkInterface forIndex(unsigned index);
		/// Returns the NetworkInterface for the given interface index.
		///
		/// Throws an InterfaceNotFoundException if an interface
		/// with the given index does not exist.

	static List list(bool ipOnly = true, bool upOnly = true);
		/// Returns a list with all network interfaces
		/// on the system.
		///
		/// If ipOnly is true, only interfaces supporting IP
		/// are returned. Otherwise, all system network interfaces
		/// are returned.
		///
		/// If upOnly is true, only interfaces being up are returned.
		/// Otherwise, both interfaces being up and down are returned.
		///
		/// If there are multiple addresses bound to one interface,
		/// multiple NetworkInterface entries are listed for
		/// the same interface.

	static Map map(bool ipOnly = true, bool upOnly = true);
		/// Returns a map containing system network interfaces
		/// Map is keyed by interface system indices.
		///
		/// If ipOnly is true, only interfaces supporting IP
		/// are returned. Otherwise, all system network interfaces
		/// are returned.
		///
		/// If upOnly is true, only interfaces being up are returned.
		/// Otherwise, both interfaces being up and down are returned.
		///
		/// If there are multiple addresses bound to one interface,
		/// they are contained within the NetworkInterface (second) 
		/// member of the pair.

protected:
	NetworkInterface(const std::string& name, const std::string& displayName, const std::string& adapterName, const IPAddress& address, unsigned index, MACAddress* pMACAddress = 0);
		/// Creates the NetworkInterface.

	NetworkInterface(const std::string& name, const std::string& displayName, const std::string& adapterName, unsigned index, MACAddress* pMACAddress = 0);
		/// Creates the NetworkInterface.

	NetworkInterface(const std::string& name, const IPAddress& address, unsigned index, MACAddress* pMACAddress = 0);
		/// Creates the NetworkInterface.

	NetworkInterface(const std::string& name,
		const std::string& displayName,
		const std::string& adapterName,
		const IPAddress& address,
		const IPAddress& subnetMask,
		const IPAddress& broadcastAddress,
		unsigned index,
		MACAddress* pMACAddress = 0);
		/// Creates the NetworkInterface.

	NetworkInterface(const std::string& name,
		const IPAddress& address,
		const IPAddress& subnetMask,
		const IPAddress& broadcastAddress,
		unsigned index,
		MACAddress* pMACAddress = 0);
		/// Creates the NetworkInterface.

	IPAddress interfaceNameToAddress(const std::string& interfaceName) const;
		/// Determines the IPAddress bound to the interface with the given name.

	unsigned interfaceNameToIndex(const std::string& interfaceName) const;
		/// Determines the interface index of the interface with the given name.

	NetworkInterfaceImpl& impl() { return *_pImpl; };

private:
	NetworkInterfaceImpl* _pImpl;

	static Poco::FastMutex _mutex;
};


///
/// inlines
///


inline bool NetworkInterface::operator < (const NetworkInterface& other) const
{
	return this->index() < other.index();
}


inline bool NetworkInterface::operator == (const NetworkInterface& other) const
{
	return this->index() == other.index();
}


} } // namespace Poco::Net


Net_API std::ostream& operator << (std::ostream& ostr, const Poco::Net::NetworkInterface::MACAddress& addr);


#endif // POCO_NET_HAS_INTERFACE


#endif // Net_NetworkInterface_INCLUDED
