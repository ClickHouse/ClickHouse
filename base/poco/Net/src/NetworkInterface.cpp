//
// NetworkInterface.cpp
//
// Library: Net
// Package: NetCore
// Module:  NetworkInterface
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/NetworkInterface.h"


#ifdef POCO_NET_HAS_INTERFACE


#include "Poco/Net/DatagramSocket.h"
#include "Poco/Net/NetException.h"
#include "Poco/NumberFormatter.h"
#include "Poco/NumberParser.h"
#include "Poco/StringTokenizer.h"
#include "Poco/RefCountedObject.h"
#include "Poco/Format.h"
#include <cstring>
#include <fstream>
#include <iostream>
#include <iomanip>

#include <sys/types.h>
#include <ifaddrs.h>
#include <net/if.h>

using Poco::NumberFormatter;
using Poco::FastMutex;
using Poco::format;


std::ostream& operator << (std::ostream& os, const Poco::Net::NetworkInterface::MACAddress& mac)
{
	std::ios state(0);
	state.copyfmt(os);
	for (unsigned i = 0; i < mac.size(); ++i)
	{
		if (i > 0) os << Poco::Net::NetworkInterface::MAC_SEPARATOR;
		os << std::hex << std::setw(2) << std::setfill('0') << (unsigned) mac[i];
	}
	os.copyfmt(state);
	return os;
}


namespace Poco {
namespace Net {


//
// NetworkInterfaceImpl
//

class NetworkInterfaceImpl: public Poco::RefCountedObject
{
public:
	typedef NetworkInterface::AddressTuple AddressTuple;
	typedef NetworkInterface::AddressList  AddressList;
	typedef NetworkInterface::Type         Type;

	NetworkInterfaceImpl(unsigned index);
	NetworkInterfaceImpl(const std::string& name, const std::string& displayName, const std::string& adapterName, const IPAddress& address, unsigned index, NetworkInterface::MACAddress* pMACAddress = 0);
	NetworkInterfaceImpl(const std::string& name, const std::string& displayName, const std::string& adapterName, unsigned index = 0, NetworkInterface::MACAddress* pMACAddress = 0);
	NetworkInterfaceImpl(const std::string& name,
		const std::string& displayName,
		const std::string& adapterName,
		const IPAddress& address,
		const IPAddress& subnetMask,
		const IPAddress& broadcastAddress,
		unsigned index,
		NetworkInterface::MACAddress* pMACAddress = 0);

	unsigned index() const;
	const std::string& name() const;
	const std::string& displayName() const;
	const std::string& adapterName() const;
	const IPAddress& firstAddress(IPAddress::Family family) const;
	void addAddress(const AddressTuple& address);
	const IPAddress& address(unsigned index) const;
	const NetworkInterface::AddressList& addressList() const;
	bool hasAddress(const IPAddress& address) const;
	const IPAddress& subnetMask(unsigned index) const;
	const IPAddress& broadcastAddress(unsigned index) const;
	const IPAddress& destAddress(unsigned index) const;
	const NetworkInterface::MACAddress& macAddress() const;
	bool supportsIPv4() const;
	bool supportsIPv6() const;

	void setName(const std::string& name);
	void setDisplayName(const std::string& name);
	void setAdapterName(const std::string& name);
	void addAddress(const IPAddress& addr);
	void setMACAddress(const NetworkInterface::MACAddress& addr);
	void setMACAddress(const void *addr, std::size_t len);

	unsigned mtu() const;
	unsigned ifindex() const;
	Type type() const;

	bool broadcast() const;
	bool loopback() const;
	bool multicast() const;
	bool pointToPoint() const;
	bool running() const;
	bool up() const;

	void setFlags(short flags);

	void setUp(bool up);
	void setMTU(unsigned mtu);
	void setType(Type type);
	void setIndex(unsigned index);
	void setPhyParams();

protected:
	~NetworkInterfaceImpl();

private:
	std::string _name;
	std::string _displayName;
	std::string _adapterName;
	AddressList _addressList;
	unsigned    _index;
	bool        _broadcast;
	bool        _loopback;
	bool        _multicast;
	bool        _pointToPoint;
	bool        _up;
	bool        _running;
	unsigned    _mtu;
	Type        _type;

	NetworkInterface::MACAddress _macAddress;

	friend class NetworkInterface;
};


NetworkInterfaceImpl::NetworkInterfaceImpl(unsigned index):
	_index(index),
	_broadcast(false),
	_loopback(false),
	_multicast(false),
	_pointToPoint(false),
	_up(false),
	_running(false),
	_mtu(0),
	_type(NetworkInterface::NI_TYPE_OTHER)
{
}


NetworkInterfaceImpl::NetworkInterfaceImpl(const std::string& name, const std::string& displayName, const std::string& adapterName, const IPAddress& address, unsigned index, NetworkInterface::MACAddress* pMACAddress):
	_name(name),
	_displayName(displayName),
	_adapterName(adapterName),
	_index(index),
	_broadcast(false),
	_loopback(false),
	_multicast(false),
	_pointToPoint(false),
	_up(false),
	_running(false),
	_mtu(0),
	_type(NetworkInterface::NI_TYPE_OTHER)
{
	_addressList.push_back(AddressTuple(address, IPAddress(), IPAddress()));
	setPhyParams();
	if (pMACAddress) setMACAddress(*pMACAddress);
}


NetworkInterfaceImpl::NetworkInterfaceImpl(const std::string& name, const std::string& displayName, const std::string& adapterName, unsigned index, NetworkInterface::MACAddress* pMACAddress):
	_name(name),
	_displayName(displayName),
	_adapterName(adapterName),
	_index(index),
	_broadcast(false),
	_loopback(false),
	_multicast(false),
	_pointToPoint(false),
	_up(false),
	_running(false),
	_mtu(0),
	_type(NetworkInterface::NI_TYPE_OTHER)
{
	setPhyParams();
	if (pMACAddress) setMACAddress(*pMACAddress);
}


NetworkInterfaceImpl::NetworkInterfaceImpl(const std::string& name,
	const std::string& displayName,
	const std::string& adapterName,
	const IPAddress& address,
	const IPAddress& subnetMask,
	const IPAddress& broadcastAddress,
	unsigned index,
	NetworkInterface::MACAddress* pMACAddress):
	_name(name),
	_displayName(displayName),
	_adapterName(adapterName),
	_index(index),
	_broadcast(false),
	_loopback(false),
	_multicast(false),
	_pointToPoint(false),
	_up(false),
	_running(false),
	_mtu(0),
	_type(NetworkInterface::NI_TYPE_OTHER)
{
	_addressList.push_back(AddressTuple(address, subnetMask, broadcastAddress));
	setPhyParams();
	if (pMACAddress) setMACAddress(*pMACAddress);
}


void NetworkInterfaceImpl::setPhyParams()
{
#if !defined(POCO_OS_FAMILY_WINDOWS) && !defined(POCO_VXWORKS)
	struct ifreq ifr;
	std::strncpy(ifr.ifr_name, _name.c_str(), IFNAMSIZ);
	DatagramSocket ds(SocketAddress::IPv4);

	ds.impl()->ioctl(SIOCGIFFLAGS, &ifr);
	setFlags(ifr.ifr_flags);

	ds.impl()->ioctl(SIOCGIFMTU, &ifr);
	setMTU(ifr.ifr_mtu);
#endif
}


NetworkInterfaceImpl::~NetworkInterfaceImpl()
{
}


bool NetworkInterfaceImpl::supportsIPv4() const
{
	AddressList::const_iterator it = _addressList.begin();
	AddressList::const_iterator end = _addressList.end();
	for (; it != end; ++it)
	{
		if (IPAddress::IPv4 == it->get<NetworkInterface::IP_ADDRESS>().family())
			return true;
	}

	return false;
}


bool NetworkInterfaceImpl::supportsIPv6() const
{
#ifdef POCO_HAVE_IPv6
	AddressList::const_iterator it = _addressList.begin();
	AddressList::const_iterator end = _addressList.end();
	for (; it != end; ++it)
	{
		if (IPAddress::IPv6 == it->get<NetworkInterface::IP_ADDRESS>().family())
			return true;
	}
#endif
	return false;
}


inline unsigned NetworkInterfaceImpl::index() const
{
	return _index;
}


inline const std::string& NetworkInterfaceImpl::name() const
{
	return _name;
}


inline const std::string& NetworkInterfaceImpl::displayName() const
{
	return _displayName;
}


inline const std::string& NetworkInterfaceImpl::adapterName() const
{
	return _adapterName;
}


const IPAddress& NetworkInterfaceImpl::firstAddress(IPAddress::Family family) const
{
	AddressList::const_iterator it = _addressList.begin();
	AddressList::const_iterator end = _addressList.end();
	for (;it != end; ++it)
	{
		const IPAddress& addr = it->get<NetworkInterface::IP_ADDRESS>();
		if (addr.family() == family) return addr;
	}

	throw NotFoundException(format("%s family address not found.", (family == IPAddress::IPv4) ? std::string("IPv4") : std::string("IPv6")));
}


inline void NetworkInterfaceImpl::addAddress(const AddressTuple& address)
{
	_addressList.push_back(address);
}


bool NetworkInterfaceImpl::hasAddress(const IPAddress& address) const
{
	NetworkInterface::ConstAddressIterator it = _addressList.begin();
	NetworkInterface::ConstAddressIterator end = _addressList.end();
	for (; it != end; ++it)
	{
		if (it->get<NetworkInterface::IP_ADDRESS>() == address)
			return true;
	}
	return false;
}


inline const IPAddress& NetworkInterfaceImpl::address(unsigned index) const
{
	if (index < _addressList.size()) return _addressList[index].get<NetworkInterface::IP_ADDRESS>();
	else throw NotFoundException(Poco::format("No address with index %u.", index));
}


inline const NetworkInterface::AddressList& NetworkInterfaceImpl::addressList() const
{
	return _addressList;
}


const IPAddress& NetworkInterfaceImpl::subnetMask(unsigned index) const
{
	if (index < _addressList.size())
		return _addressList[index].get<NetworkInterface::SUBNET_MASK>();

	throw NotFoundException(Poco::format("No subnet mask with index %u.", index));
}


const IPAddress& NetworkInterfaceImpl::broadcastAddress(unsigned index) const
{
	if (index < _addressList.size())
		return _addressList[index].get<NetworkInterface::BROADCAST_ADDRESS>();

	throw NotFoundException(Poco::format("No subnet mask with index %u.", index));
}


const IPAddress& NetworkInterfaceImpl::destAddress(unsigned index) const
{
	if (!pointToPoint())
		throw InvalidAccessException("Only PPP addresses have destination address.");
	else if (index < _addressList.size())
		return _addressList[index].get<NetworkInterface::BROADCAST_ADDRESS>();

	throw NotFoundException(Poco::format("No address with index %u.", index));
}


const NetworkInterface::MACAddress& NetworkInterfaceImpl::macAddress() const
{
	return _macAddress;
}


inline unsigned NetworkInterfaceImpl::mtu() const
{
	return _mtu;
}


inline NetworkInterface::Type NetworkInterfaceImpl::type() const
{
	return _type;
}


inline bool NetworkInterfaceImpl::broadcast() const
{
	return _broadcast;
}


inline bool NetworkInterfaceImpl::loopback() const
{
	return _loopback;
}


inline bool NetworkInterfaceImpl::multicast() const
{
	return _multicast;
}


inline bool NetworkInterfaceImpl::pointToPoint() const
{
	return _pointToPoint;
}


inline bool NetworkInterfaceImpl::running() const
{
	return _running;
}


inline bool NetworkInterfaceImpl::up() const
{
	return _up;
}




void NetworkInterfaceImpl::setFlags(short flags)
{
#ifdef POCO_OS_FAMILY_UNIX
	_broadcast = ((flags & IFF_BROADCAST) != 0);
	_loopback = ((flags & IFF_LOOPBACK) != 0);
	_multicast = ((flags & IFF_MULTICAST) != 0);
	_pointToPoint = ((flags & IFF_POINTOPOINT) != 0);
	_running = ((flags & IFF_RUNNING) != 0);
	_up = ((flags & IFF_UP) != 0);
#endif
}




inline void NetworkInterfaceImpl::setUp(bool up)
{
	_up = up;
}


inline void NetworkInterfaceImpl::setMTU(unsigned mtu)
{
	_mtu = mtu;
}


inline void NetworkInterfaceImpl::setType(Type type)
{
	_type = type;
}


inline void NetworkInterfaceImpl::setIndex(unsigned index)
{
	_index = index;
}


inline void NetworkInterfaceImpl::setName(const std::string& name)
{
	_name = name;
}


inline void NetworkInterfaceImpl::setDisplayName(const std::string& name)
{
	_displayName = name;
}


inline void NetworkInterfaceImpl::setAdapterName(const std::string& name)
{
	_adapterName = name;
}


inline void NetworkInterfaceImpl::addAddress(const IPAddress& addr)
{
	_addressList.push_back(addr);
}


inline void NetworkInterfaceImpl::setMACAddress(const NetworkInterface::MACAddress& addr)
{
	_macAddress = addr;
}


inline void NetworkInterfaceImpl::setMACAddress(const void *addr, std::size_t len)
{
	_macAddress.clear();
	_macAddress.insert(_macAddress.end(), static_cast<const unsigned char*>(addr), static_cast<const unsigned char*>(addr) + len);
}


//
// NetworkInterface
//


FastMutex NetworkInterface::_mutex;


NetworkInterface::NetworkInterface(unsigned index):
	_pImpl(new NetworkInterfaceImpl(index))
{
}


NetworkInterface::NetworkInterface(const NetworkInterface& interfc):
	_pImpl(interfc._pImpl)
{
	_pImpl->duplicate();
}


NetworkInterface::NetworkInterface(const std::string& name, const std::string& displayName, const std::string& adapterName, const IPAddress& address, unsigned index, MACAddress* pMACAddress):
	_pImpl(new NetworkInterfaceImpl(name, displayName, adapterName, address, index, pMACAddress))
{
}


NetworkInterface::NetworkInterface(const std::string& name, const std::string& displayName, const std::string& adapterName, unsigned index, MACAddress* pMACAddress):
	_pImpl(new NetworkInterfaceImpl(name, displayName, adapterName, index, pMACAddress))
{
}


NetworkInterface::NetworkInterface(const std::string& name, const IPAddress& address, unsigned index, MACAddress* pMACAddress):
	_pImpl(new NetworkInterfaceImpl(name, name, name, address, index, pMACAddress))
{
}


NetworkInterface::NetworkInterface(const std::string& name,
	const std::string& displayName,
	const std::string& adapterName,
	const IPAddress& address,
	const IPAddress& subnetMask,
	const IPAddress& broadcastAddress,
	unsigned index,
	MACAddress* pMACAddress):
	_pImpl(new NetworkInterfaceImpl(name, displayName, adapterName, address, subnetMask, broadcastAddress, index, pMACAddress))
{
}


NetworkInterface::NetworkInterface(const std::string& name,
	const IPAddress& address,
	const IPAddress& subnetMask,
	const IPAddress& broadcastAddress,
	unsigned index,
	MACAddress* pMACAddress):
	_pImpl(new NetworkInterfaceImpl(name, name, name, address, subnetMask, broadcastAddress, index, pMACAddress))
{
}


NetworkInterface::~NetworkInterface()
{
	_pImpl->release();
}


NetworkInterface& NetworkInterface::operator = (const NetworkInterface& interfc)
{
	NetworkInterface tmp(interfc);
	swap(tmp);
	return *this;
}


void NetworkInterface::swap(NetworkInterface& other)
{
	using std::swap;
	swap(_pImpl, other._pImpl);
}


unsigned NetworkInterface::index() const
{
	return _pImpl->index();
}


const std::string& NetworkInterface::name() const
{
	return _pImpl->name();
}


const std::string& NetworkInterface::displayName() const
{
	return _pImpl->displayName();
}


const std::string& NetworkInterface::adapterName() const
{
	return _pImpl->adapterName();
}


const IPAddress& NetworkInterface::firstAddress(IPAddress::Family family) const
{
	return _pImpl->firstAddress(family);
}


void NetworkInterface::firstAddress(IPAddress& addr, IPAddress::Family family) const
{
	try
	{
		addr = firstAddress(family);
	}
	catch (NotFoundException&)
	{
		addr = IPAddress(family);
	}
}


void NetworkInterface::addAddress(const IPAddress& address)
{
	_pImpl->addAddress(AddressTuple(address, IPAddress(), IPAddress()));
}


void NetworkInterface::addAddress(const IPAddress& address, const IPAddress& subnetMask, const IPAddress& broadcastAddress)
{
	_pImpl->addAddress(AddressTuple(address, subnetMask, broadcastAddress));
}


const IPAddress& NetworkInterface::address(unsigned index) const
{
	return _pImpl->address(index);
}


const NetworkInterface::AddressList& NetworkInterface::addressList() const
{
	return _pImpl->addressList();
}


const IPAddress& NetworkInterface::subnetMask(unsigned index) const
{
	return _pImpl->subnetMask(index);
}


const IPAddress& NetworkInterface::broadcastAddress(unsigned index) const
{
	return _pImpl->broadcastAddress(index);
}


const NetworkInterface::MACAddress& NetworkInterface::macAddress() const
{
	return _pImpl->macAddress();
}


const IPAddress& NetworkInterface::destAddress(unsigned index) const
{
	return _pImpl->destAddress(index);
}


unsigned NetworkInterface::mtu() const
{
	return _pImpl->mtu();
}


NetworkInterface::Type NetworkInterface::type() const
{
	return _pImpl->type();
}


bool NetworkInterface::supportsIP() const
{
	return _pImpl->supportsIPv4() || _pImpl->supportsIPv6();
}


bool NetworkInterface::supportsIPv4() const
{
	return _pImpl->supportsIPv4();
}


bool NetworkInterface::supportsIPv6() const
{
	return _pImpl->supportsIPv6();
}


bool NetworkInterface::supportsBroadcast() const
{
	return _pImpl->broadcast();
}


bool NetworkInterface::supportsMulticast() const
{
	return _pImpl->multicast();
}


bool NetworkInterface::isLoopback() const
{
	return _pImpl->loopback();
}


bool NetworkInterface::isPointToPoint() const
{
	return _pImpl->pointToPoint();
}


bool NetworkInterface::isRunning() const
{
	return _pImpl->running();
}


bool NetworkInterface::isUp() const
{
	return _pImpl->up();
}


NetworkInterface NetworkInterface::forName(const std::string& name, bool requireIPv6)
{
	if (requireIPv6) 
		return forName(name, IPv6_ONLY);
	else 
		return forName(name, IPv4_OR_IPv6);
}


NetworkInterface NetworkInterface::forName(const std::string& name, IPVersion ipVersion)
{
	Map map = NetworkInterface::map(false, false);
	Map::const_iterator it = map.begin();
	Map::const_iterator end = map.end();

	for (; it != end; ++it)
	{
		if (it->second.name() == name)
		{
			if (ipVersion == IPv4_ONLY && it->second.supportsIPv4())
				return it->second;
			else if (ipVersion == IPv6_ONLY && it->second.supportsIPv6())
				return it->second;
			else if (ipVersion == IPv4_OR_IPv6)
				return it->second;
		}
	}
	throw InterfaceNotFoundException(name);
}


NetworkInterface NetworkInterface::forAddress(const IPAddress& addr)
{
	Map map = NetworkInterface::map(true, false);
	Map::const_iterator it = map.begin();
	Map::const_iterator end = map.end();

	for (; it != end; ++it)
	{
		const std::size_t count = it->second.addressList().size();
		for (int i = 0; i < count; ++i)
		{
			if (it->second.address(i) == addr)
				return it->second;
		}
	}
	throw InterfaceNotFoundException(addr.toString());
}


NetworkInterface NetworkInterface::forIndex(unsigned i)
{
	if (i != NetworkInterface::NO_INDEX)
	{
		Map map = NetworkInterface::map(false, false);
		Map::const_iterator it = map.find(i);
		if (it != map.end())
			return it->second;
		else
			throw InterfaceNotFoundException("#" + NumberFormatter::format(i));
	}
	throw InterfaceNotFoundException("#" + NumberFormatter::format(i));
}


NetworkInterface::List NetworkInterface::list(bool ipOnly, bool upOnly)
{
	List list;
	Map m = map(ipOnly, upOnly);
	NetworkInterface::Map::const_iterator it = m.begin();
	NetworkInterface::Map::const_iterator end = m.end();
	for (; it != end; ++it)
	{
		int index = it->second.index();
		std::string name = it->second.name();
		std::string displayName = it->second.displayName();
		std::string adapterName = it->second.adapterName();
		NetworkInterface::MACAddress mac = it->second.macAddress();

		typedef NetworkInterface::AddressList List;
		const List& ipList = it->second.addressList();
		if (ipList.size() > 0)
		{
			List::const_iterator ipIt = ipList.begin();
			List::const_iterator ipEnd = ipList.end();
			for(; ipIt != ipEnd; ++ipIt)
			{
				IPAddress addr = ipIt->get<NetworkInterface::IP_ADDRESS>();
				IPAddress mask = ipIt->get<NetworkInterface::SUBNET_MASK>();
				NetworkInterface ni;
				if(mask.isWildcard())
				{
					ni = NetworkInterface(name, displayName, adapterName, addr, index, &mac);
				}
				else
				{
					IPAddress broadcast = ipIt->get<NetworkInterface::BROADCAST_ADDRESS>();
					ni = NetworkInterface(name, displayName, adapterName, addr, mask, broadcast, index, &mac);
				}

				ni._pImpl->_broadcast = it->second._pImpl->_broadcast;
				ni._pImpl->_loopback = it->second._pImpl->_loopback;
				ni._pImpl->_multicast = it->second._pImpl->_multicast;
				ni._pImpl->_pointToPoint = it->second._pImpl->_pointToPoint;
				ni._pImpl->_up = it->second._pImpl->_up;
				ni._pImpl->_running = it->second._pImpl->_running;
				ni._pImpl->_mtu = it->second._pImpl->_mtu;
				ni._pImpl->_type = it->second._pImpl->_type;

				list.push_back(ni);
			}
		}
		else
		{
			list.push_back(NetworkInterface(name, displayName, adapterName, index, &mac));
		}
	}

	return list;
}


} } // namespace Poco::Net


//
// platform-specific code below
//


#if   defined(POCO_OS_FAMILY_BSD) || (POCO_OS == POCO_OS_QNX) || (POCO_OS == POCO_OS_SOLARIS)
//
// BSD variants, QNX(?) and Solaris
//
#include <sys/socket.h>
#include <net/if_dl.h>
#ifndef POCO_NO_NET_IFTYPES
#include <net/if_types.h>
#endif


namespace Poco {
namespace Net {


namespace {


NetworkInterface::Type fromNative(u_char nativeType)
{
	switch (nativeType)
	{
#ifndef POCO_NO_NET_IFTYPES
		case IFT_ETHER:		return NetworkInterface::NI_TYPE_ETHERNET_CSMACD;
		case IFT_ISO88025:	return NetworkInterface::NI_TYPE_ISO88025_TOKENRING;
		case IFT_FRELAY:	return NetworkInterface::NI_TYPE_FRAMERELAY;
		case IFT_PPP:		return NetworkInterface::NI_TYPE_PPP;
		case IFT_LOOP:		return NetworkInterface::NI_TYPE_SOFTWARE_LOOPBACK;
		case IFT_ATM:		return NetworkInterface::NI_TYPE_ATM;
#if (POCO_OS != POCO_OS_SOLARIS)
		case IFT_IEEE1394:	return NetworkInterface::NI_TYPE_IEEE1394;
#endif
#endif
		default:			return NetworkInterface::NI_TYPE_OTHER;

	}
}


void setInterfaceParams(struct ifaddrs* iface, NetworkInterfaceImpl& impl)
{
	struct sockaddr_dl* sdl = (struct sockaddr_dl*) iface->ifa_addr;
	impl.setName(iface->ifa_name);
	impl.setDisplayName(iface->ifa_name);
	impl.setAdapterName(iface->ifa_name);
	impl.setPhyParams();

	impl.setMACAddress(LLADDR(sdl), sdl->sdl_alen);
	impl.setType(fromNative(sdl->sdl_type));
}


} // namespace


NetworkInterface::Map NetworkInterface::map(bool ipOnly, bool upOnly)
{
	FastMutex::ScopedLock lock(_mutex);
	Map result;
	unsigned ifIndex = 0;
	NetworkInterface intf;
	Map::iterator ifIt;

	struct ifaddrs* ifaces = 0;
	struct ifaddrs* currIface = 0;

	if (getifaddrs(&ifaces) < 0)
		throw NetException("cannot get network adapter list");

	try
	{
		for (currIface = ifaces; currIface != 0; currIface = currIface->ifa_next)
		{
			if (!currIface->ifa_addr) continue;

			IPAddress address, subnetMask, broadcastAddress;
			unsigned family = currIface->ifa_addr->sa_family;
			switch (family)
			{
#if defined(POCO_OS_FAMILY_BSD)
			case AF_LINK:
			{
				struct sockaddr_dl* sdl = (struct sockaddr_dl*) currIface->ifa_addr;
				ifIndex = sdl->sdl_index;
				intf = NetworkInterface(ifIndex);
				setInterfaceParams(currIface, intf.impl());
				if ((result.find(ifIndex) == result.end()) && ((upOnly && intf.isUp()) || !upOnly))
					ifIt = result.insert(Map::value_type(ifIndex, intf)).first;
				break;
			}
#endif
			case AF_INET:
				ifIndex = if_nametoindex(currIface->ifa_name);
				ifIt = result.find(ifIndex);
				intf = NetworkInterface(ifIndex);
				setInterfaceParams(currIface, intf.impl());
				if ((ifIt == result.end()) && ((upOnly && intf.isUp()) || !upOnly))
					ifIt = result.insert(Map::value_type(ifIndex, intf)).first;

				address = IPAddress(*(currIface->ifa_addr));

				if (( currIface->ifa_flags & IFF_LOOPBACK ) == 0 && currIface->ifa_netmask)
					subnetMask = IPAddress(*(currIface->ifa_netmask));

				if (currIface->ifa_flags & IFF_BROADCAST && currIface->ifa_broadaddr)
					broadcastAddress = IPAddress(*(currIface->ifa_broadaddr));
				else if (currIface->ifa_flags & IFF_POINTOPOINT && currIface->ifa_dstaddr)
					broadcastAddress = IPAddress(*(currIface->ifa_dstaddr));
				else
					broadcastAddress = IPAddress();
				break;
#if defined(POCO_HAVE_IPv6)
			case AF_INET6:
				ifIndex = if_nametoindex(currIface->ifa_name);
				ifIt = result.find(ifIndex);
				intf = NetworkInterface(ifIndex);
				setInterfaceParams(currIface, intf.impl());
				if ((ifIt == result.end()) && ((upOnly && intf.isUp()) || !upOnly))
					ifIt = result.insert(Map::value_type(ifIndex, intf)).first;

				address = IPAddress(&reinterpret_cast<const struct sockaddr_in6*>(currIface->ifa_addr)->sin6_addr,
					sizeof(struct in6_addr), ifIndex);
				subnetMask = IPAddress(*(currIface->ifa_netmask));
				broadcastAddress = IPAddress();
				break;
#endif
			default:
				continue;
			}

			if (family == AF_INET
#ifdef POCO_HAVE_IPv6
			|| family == AF_INET6
#endif
			)
			{
				if ((upOnly && intf.isUp()) || !upOnly)
				{
					if ((ifIt = result.find(ifIndex)) != result.end())
						ifIt->second.addAddress(address, subnetMask, broadcastAddress);
				}
			}
		}
	}
	catch (...)
	{
	}
	if (ifaces) freeifaddrs(ifaces);

	if (ipOnly)
	{
		Map::iterator it = result.begin();
		Map::iterator end = result.end();
		for (; it != end;)
		{
			if (!it->second.supportsIPv4() && !it->second.supportsIPv6())
				result.erase(it++);
			else ++it;
		}
	}

	return result;
}


} } // namespace Poco::Net


#elif POCO_OS == POCO_OS_LINUX || POCO_OS == POCO_OS_ANDROID
//
// Linux
//


/// #include <sys/types.h>
#if POCO_OS != POCO_OS_ANDROID // Android doesn't have <ifaddrs.h>
/// #include <ifaddrs.h>
#endif
/// #include <net/if.h>
#ifndef POCO_NO_LINUX_IF_PACKET_H
#include <linux/if_packet.h>
#endif
#include <net/if_arp.h>
/// #include <iostream>

namespace Poco {
namespace Net {


namespace {


static NetworkInterface::Type fromNative(unsigned arphrd)
{
	switch (arphrd)
	{
	case ARPHRD_ETHER:     return NetworkInterface::NI_TYPE_ETHERNET_CSMACD;
	case ARPHRD_IEEE802:   return NetworkInterface::NI_TYPE_ISO88025_TOKENRING;
	case ARPHRD_DLCI:      return NetworkInterface::NI_TYPE_FRAMERELAY;
	case ARPHRD_PPP:       return NetworkInterface::NI_TYPE_PPP;
	case ARPHRD_LOOPBACK:  return NetworkInterface::NI_TYPE_SOFTWARE_LOOPBACK;
	case ARPHRD_ATM:       return NetworkInterface::NI_TYPE_ATM;
	case ARPHRD_IEEE80211: return NetworkInterface::NI_TYPE_IEEE80211;
	case ARPHRD_TUNNEL:
	case ARPHRD_TUNNEL6:   return NetworkInterface::NI_TYPE_TUNNEL;
	case ARPHRD_IEEE1394:  return NetworkInterface::NI_TYPE_IEEE1394;
	default:               return NetworkInterface::NI_TYPE_OTHER;
	}
}

#if POCO_OS != POCO_OS_ANDROID

void setInterfaceParams(struct ifaddrs* iface, NetworkInterfaceImpl& impl)
{
	impl.setName(iface->ifa_name);
	impl.setDisplayName(iface->ifa_name);
	impl.setAdapterName(iface->ifa_name);
	impl.setPhyParams();

#ifndef POCO_NO_LINUX_IF_PACKET_H
	struct sockaddr_ll* sdl = (struct sockaddr_ll*) iface->ifa_addr;
	impl.setMACAddress(sdl->sll_addr, sdl->sll_halen);
	impl.setType(fromNative(sdl->sll_hatype));
#else
	std::string ifPath("/sys/class/net/");
	ifPath += iface->ifa_name;

	std::string addrPath(ifPath);
	addrPath += "/address";
	
	std::ifstream addrStream(addrPath.c_str());
	if (addrStream.good())
	{
		std::string addr;
		std::getline(addrStream, addr);
		Poco::StringTokenizer tok(addr, ":");
		std::vector<unsigned char> mac;
		for (Poco::StringTokenizer::Iterator it = tok.begin(); it != tok.end(); ++it)
		{
			mac.push_back(static_cast<unsigned char>(Poco::NumberParser::parseHex(*it)));
		}
		impl.setMACAddress(&mac[0], mac.size());
		addrStream.close();
	}
	
	std::string typePath(ifPath);
	typePath += "/type";
	std::ifstream typeStream(typePath.c_str());
	if (typeStream.good())
	{
		int type;
		typeStream >> type;
		impl.setType(fromNative(type));
		typeStream.close();
	}
#endif // POCO_NO_LINUX_IF_PACKET_H
}

#endif


}


NetworkInterface::Map NetworkInterface::map(bool ipOnly, bool upOnly)
{
#if POCO_OS != POCO_OS_ANDROID
	FastMutex::ScopedLock lock(_mutex);
	Map result;
	unsigned ifIndex = 0;
	NetworkInterface intf;
	Map::iterator ifIt;

	struct ifaddrs* ifaces = 0;
	struct ifaddrs* iface = 0;

	if (getifaddrs(&ifaces) < 0)
		throw NetException("cannot get network adapter list");

	try
	{
		for (iface = ifaces; iface; iface = iface->ifa_next)
		{
			if (!iface->ifa_addr) continue;

			IPAddress address, subnetMask, broadcastAddress;
			unsigned family = iface->ifa_addr->sa_family;
			switch (family)
			{
#ifndef POCO_NO_LINUX_IF_PACKET_H
			case AF_PACKET:
			{
				struct sockaddr_ll* sll = (struct sockaddr_ll*)iface->ifa_addr;
				ifIndex = sll->sll_ifindex;
				intf = NetworkInterface(ifIndex);
				setInterfaceParams(iface, intf.impl());

				if ((result.find(ifIndex) == result.end()) && ((upOnly && intf.isUp()) || !upOnly))
					ifIt = result.insert(Map::value_type(ifIndex, intf)).first;

				break;
			}
#endif // POCO_NO_LINUX_IF_PACKET_H
			case AF_INET:
				ifIndex = if_nametoindex(iface->ifa_name);
				ifIt = result.find(ifIndex);
				intf = NetworkInterface(ifIndex);
				setInterfaceParams(iface, intf.impl());

				if ((ifIt == result.end()) && ((upOnly && intf.isUp()) || !upOnly))
					ifIt = result.insert(Map::value_type(ifIndex, intf)).first;

				address = IPAddress(*(iface->ifa_addr));
				subnetMask = IPAddress(*(iface->ifa_netmask));

				if (iface->ifa_flags & IFF_BROADCAST && iface->ifa_broadaddr)
					broadcastAddress = IPAddress(*(iface->ifa_broadaddr));
				else if (iface->ifa_flags & IFF_POINTOPOINT && iface->ifa_dstaddr)
					broadcastAddress = IPAddress(*(iface->ifa_dstaddr));
				else
					broadcastAddress = IPAddress();

				break;
#if defined(POCO_HAVE_IPv6)
			case AF_INET6:
				ifIndex = if_nametoindex(iface->ifa_name);
				ifIt = result.find(ifIndex);
				intf = NetworkInterface(ifIndex);
				setInterfaceParams(iface, intf.impl());

				if ((ifIt == result.end()) && ((upOnly && intf.isUp()) || !upOnly))
					result.insert(Map::value_type(ifIndex, intf));

				address = IPAddress(&reinterpret_cast<const struct sockaddr_in6*>(iface->ifa_addr)->sin6_addr, sizeof(struct in6_addr), ifIndex);
				subnetMask = IPAddress(*(iface->ifa_netmask));
				broadcastAddress = IPAddress();

				break;
#endif
			default:
				continue;
			}

			if (family == AF_INET
#ifdef POCO_HAVE_IPv6
			|| family == AF_INET6
#endif
			)
			{
				intf = NetworkInterface(std::string(iface->ifa_name), address, subnetMask, broadcastAddress, ifIndex);
				if ((upOnly && intf.isUp()) || !upOnly)
				{
					if ((ifIt = result.find(ifIndex)) != result.end())
						ifIt->second.addAddress(address, subnetMask, broadcastAddress);
				}
			}
		} // for interface
	}
	catch (...)
	{
		if (ifaces) freeifaddrs(ifaces);
		throw;
	}

	if (ifaces) freeifaddrs(ifaces);

	if (ipOnly)
	{
		Map::iterator it = result.begin();
		Map::iterator end = result.end();
		for (; it != end;)
		{
			if (!it->second.supportsIPv4() && !it->second.supportsIPv6())
				result.erase(it++);
			else ++it;
		}
	}

	return result;
#else
	throw Poco::NotImplementedException("Not implemented in Android");
#endif
}


} } // namespace Poco::Net


#else
//
// Non-BSD Unix variants
//
#error TODO
/*
NetworkInterface::NetworkInterfaceList NetworkInterface::list()
{
	FastMutex::ScopedLock lock(_mutex);
	NetworkInterfaceList result;
	DatagramSocket socket;
	// the following code is loosely based
	// on W. Richard Stevens, UNIX Network Programming, pp 434ff.
	int lastlen = 0;
	int len = 100*sizeof(struct ifreq);
	char* buf = 0;
	try
	{
		struct ifconf ifc;
		for (;;)
		{
			buf = new char[len];
			ifc.ifc_len = len;
			ifc.ifc_buf = buf;
			if (::ioctl(socket.impl()->sockfd(), SIOCGIFCONF, &ifc) < 0)
			{
				if (errno != EINVAL || lastlen != 0)
					throw NetException("cannot get network adapter list");
			}
			else
			{
				if (ifc.ifc_len == lastlen)
					break;
				lastlen = ifc.ifc_len;
			}
			len += 10*sizeof(struct ifreq);
			delete [] buf;
		}
		for (const char* ptr = buf; ptr < buf + ifc.ifc_len;)
		{
			const struct ifreq* ifr = reinterpret_cast<const struct ifreq*>(ptr);
#if defined(POCO_HAVE_SALEN)
			len = ifr->ifr_addr.sa_len;
			if (sizeof(struct sockaddr) > len) len = sizeof(struct sockaddr);
#else
			len = sizeof(struct sockaddr);
#endif
			IPAddress addr;
			bool haveAddr = false;
			int ifIndex(-1);
			switch (ifr->ifr_addr.sa_family)
			{
#if defined(POCO_HAVE_IPv6)
			case AF_INET6:
				ifIndex = if_nametoindex(ifr->ifr_name);
				if (len < sizeof(struct sockaddr_in6)) len = sizeof(struct sockaddr_in6);
				addr = IPAddress(&reinterpret_cast<const struct sockaddr_in6*>(&ifr->ifr_addr)->sin6_addr, sizeof(struct in6_addr), ifIndex);
				haveAddr = true;
				break;
#endif
			case AF_INET:
				if (len < sizeof(struct sockaddr_in)) len = sizeof(struct sockaddr_in);
				addr = IPAddress(ifr->ifr_addr);
				haveAddr = true;
				break;
			default:
				break;
			}
			if (haveAddr)
			{
				std::string name(ifr->ifr_name);
				result.push_back(NetworkInterface(name, name, name, addr, ifIndex));
			}
			len += sizeof(ifr->ifr_name);
			ptr += len;
		}
	}
	catch (...)
	{
		delete [] buf;
		throw;
	}
	delete [] buf;
	return result;
}
*/

} } // namespace Poco::Net


#endif


#endif // POCO_NET_HAS_INTERFACE
