//
// NetworkInterfaceTest.cpp
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NetworkInterfaceTest.h"


#ifdef POCO_NET_HAS_INTERFACE


#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Net/NetworkInterface.h"
#include "Poco/Net/IPAddress.h"
#include <iostream>
#include <iomanip>


using Poco::Net::NetworkInterface;
using Poco::Net::IPAddress;
using Poco::NotFoundException;


NetworkInterfaceTest::NetworkInterfaceTest(const std::string& name): CppUnit::TestCase(name)
{
}


NetworkInterfaceTest::~NetworkInterfaceTest()
{
}


void NetworkInterfaceTest::testMap()
{
	try
	{
		NetworkInterface::Map m = NetworkInterface::map(false, false);
		assert (!m.empty());
		for (NetworkInterface::Map::const_iterator it = m.begin(); it != m.end(); ++it)
		{
			std::cout << std::endl << "=============" << std::endl;

			std::cout << "Index:       " << it->second.index() << std::endl;
			std::cout << "Name:        " << it->second.name() << std::endl;
			std::cout << "DisplayName: " << it->second.displayName() << std::endl;
			std::cout << "Status:      " << (it->second.isUp() ? "Up" : "Down") << std::endl;

			NetworkInterface::MACAddress mac(it->second.macAddress());
			if (!mac.empty() && (it->second.type() != NetworkInterface::NI_TYPE_SOFTWARE_LOOPBACK))
				std::cout << "MAC Address: (" << it->second.type() << ") " << mac << std::endl;

			typedef NetworkInterface::AddressList List;
			const List& ipList = it->second.addressList();
			List::const_iterator ipIt = ipList.begin();
			List::const_iterator ipEnd = ipList.end();
			for (int counter = 0; ipIt != ipEnd; ++ipIt, ++counter)
			{
				std::cout << std::endl << "----------" << std::endl;
				std::cout << "Address " << counter << std::endl;
				std::cout << "----------" << std::endl;
				std::cout << "Address:     " << ipIt->get<NetworkInterface::IP_ADDRESS>() << std::endl;
				IPAddress addr = ipIt->get<NetworkInterface::SUBNET_MASK>();
				if (!addr.isWildcard()) std::cout << "Subnet:      " << addr << " (/" << addr.prefixLength() << ")" << std::endl;
				addr = ipIt->get<NetworkInterface::BROADCAST_ADDRESS>();
				if (!addr.isWildcard()) std::cout << "Broadcast:   " << addr << std::endl;
			}

			std::cout << "=============" << std::endl << std::endl;
		}
	}
	catch (Poco::NotImplementedException e)
	{
	#if POCO_OS != POCO_OS_ANDROID
		throw;
	#endif
	}
}


void NetworkInterfaceTest::testList()
{
	try
	{
		NetworkInterface::List list = NetworkInterface::list(false, false);
		assert (!list.empty());
		for (NetworkInterface::List::const_iterator it = list.begin(); it != list.end(); ++it)
		{
			std::cout << std::endl << "==============" << std::endl;

			std::cout << "Index:       " << it->index() << std::endl;
			std::cout << "Name:        " << it->name() << std::endl;
			std::cout << "DisplayName: " << it->displayName() << std::endl;
			std::cout << "Status:      " << (it->isUp() ? "Up" : "Down") << std::endl;

			NetworkInterface::MACAddress mac(it->macAddress());
			if (!mac.empty() && (it->type() != NetworkInterface::NI_TYPE_SOFTWARE_LOOPBACK))
				std::cout << "MAC Address: (" << it->type() << ") " << mac << std::endl;

			typedef NetworkInterface::AddressList AddrList;
			const AddrList& ipList = it->addressList();
			AddrList::const_iterator ipIt = ipList.begin();
			AddrList::const_iterator ipEnd = ipList.end();
			for (int counter = 0; ipIt != ipEnd; ++ipIt, ++counter)
			{
				std::cout << "IP Address:  " << ipIt->get<NetworkInterface::IP_ADDRESS>() << std::endl;
				IPAddress addr = ipIt->get<NetworkInterface::SUBNET_MASK>();
				if (!addr.isWildcard()) std::cout << "Subnet:      " << ipIt->get<NetworkInterface::SUBNET_MASK>() << " (/" << ipIt->get<NetworkInterface::SUBNET_MASK>().prefixLength() << ")" << std::endl;
				addr = ipIt->get<NetworkInterface::BROADCAST_ADDRESS>();
				if (!addr.isWildcard()) std::cout << "Broadcast:   " << ipIt->get<NetworkInterface::BROADCAST_ADDRESS>() << std::endl;
			}

			std::cout << "==============" << std::endl << std::endl;
		}
	}
	catch (Poco::NotImplementedException e)
	{
	#if POCO_OS != POCO_OS_ANDROID
		throw;
	#endif
	}
}


void NetworkInterfaceTest::testForName()
{
	try
	{
		NetworkInterface::Map map = NetworkInterface::map();
		for (NetworkInterface::Map::const_iterator it = map.begin(); it != map.end(); ++it)
		{
			NetworkInterface ifc = NetworkInterface::forName(it->second.name());
			assert (ifc.name() == it->second.name());
		}
	}
	catch (Poco::NotImplementedException e)
	{
	#if POCO_OS != POCO_OS_ANDROID
		throw;
	#endif
	}
}


void NetworkInterfaceTest::testForAddress()
{
	try
	{
		NetworkInterface::Map map = NetworkInterface::map();
		for (NetworkInterface::Map::const_iterator it = map.begin(); it != map.end(); ++it)
		{
			// not all interfaces have IP configured
			if (it->second.addressList().empty()) continue;

			if (it->second.supportsIPv4())
			{
				NetworkInterface ifc = NetworkInterface::forAddress(it->second.firstAddress(IPAddress::IPv4));
				assert (ifc.firstAddress(IPAddress::IPv4) == it->second.firstAddress(IPAddress::IPv4));

				IPAddress addr(IPAddress::IPv4);
				assert (addr.isWildcard());
				it->second.firstAddress(addr, IPAddress::IPv4);
				assert (!addr.isWildcard());
			}
			else
			{
				try
				{
					it->second.firstAddress(IPAddress::IPv4);
					fail ("must throw");
				}
				catch (NotFoundException&) { }

				IPAddress addr(IPAddress::IPv4);
				assert (addr.isWildcard());
				it->second.firstAddress(addr, IPAddress::IPv4);
				assert (addr.isWildcard());
			}
		}
	}
	catch (Poco::NotImplementedException e)
	{
	#if POCO_OS != POCO_OS_ANDROID
		throw;
	#endif
	}
}


void NetworkInterfaceTest::testForIndex()
{
	try
	{
		NetworkInterface::Map map = NetworkInterface::map();
		for (NetworkInterface::Map::const_iterator it = map.begin(); it != map.end(); ++it)
		{
			NetworkInterface ifc = NetworkInterface::forIndex(it->second.index());
			assert (ifc.index() == it->second.index());
		}
	}
	catch (Poco::NotImplementedException e)
	{
	#if POCO_OS != POCO_OS_ANDROID
		throw;
	#endif
	}
}


void NetworkInterfaceTest::testMapIpOnly()
{
	try
	{
		NetworkInterface::Map m = NetworkInterface::map(true, false);
		assert (!m.empty());

		std::cout << std::endl;
		for (NetworkInterface::Map::const_iterator it = m.begin(); it != m.end(); ++it)
		{
			assert(it->second.supportsIPv4() || it->second.supportsIPv6());
			std::cout << "Interface: (" << it->second.index() << ")" << std::endl;
			std::cout << "Address:    " << it->second.address() << std::endl;
			NetworkInterface::MACAddress mac(it->second.macAddress());
			if (!mac.empty() && (it->second.type() != NetworkInterface::NI_TYPE_SOFTWARE_LOOPBACK))
				std::cout << "MAC Address:" << mac << std::endl;
		}
	}
	catch (Poco::NotImplementedException e)
	{
	#if POCO_OS != POCO_OS_ANDROID
		throw;
	#endif
	}
}


void NetworkInterfaceTest::testMapUpOnly()
{
	try
	{
		NetworkInterface::Map m = NetworkInterface::map(false, true);
		assert (!m.empty());
		for (NetworkInterface::Map::const_iterator it = m.begin(); it != m.end(); ++it)
		{
			assert(it->second.isUp());
		}
	}
	catch (Poco::NotImplementedException e)
	{
	#if POCO_OS != POCO_OS_ANDROID
		throw;
	#endif
	}
}


void NetworkInterfaceTest::testListMapConformance()
{
	try
	{
		NetworkInterface::Map m = NetworkInterface::map(false, false);
		assert (!m.empty());
		NetworkInterface::List l = NetworkInterface::list(false, false);
		assert (!l.empty());

		int counter = 0;
		NetworkInterface::Map::const_iterator mapIt = m.begin();
		NetworkInterface::List::const_iterator listIt = l.begin();
		for (; mapIt != m.end(); ++mapIt)
		{
			NetworkInterface::MACAddress mac(mapIt->second.macAddress());

			typedef NetworkInterface::AddressList List;
			const List& ipList = mapIt->second.addressList();
			if (ipList.size() > 0)
			{
				List::const_iterator ipIt = ipList.begin();
				List::const_iterator ipEnd = ipList.end();
				for(; ipIt != ipEnd; ++ipIt, ++counter, ++listIt)
				{
					if(listIt == l.end()) fail("wrong number of list items");
					NetworkInterface::MACAddress lmac = listIt->macAddress();
					assert (lmac == mac);
				}
			}
			else
			{
				++listIt;
				++counter;
			}
		}

		assert (counter == l.size());
	}
	catch (Poco::NotImplementedException e)
	{
	#if POCO_OS != POCO_OS_ANDROID
		throw;
	#endif
	}
}


void NetworkInterfaceTest::setUp()
{
}


void NetworkInterfaceTest::tearDown()
{
}


CppUnit::Test* NetworkInterfaceTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NetworkInterfaceTest");

	CppUnit_addTest(pSuite, NetworkInterfaceTest, testList);
	CppUnit_addTest(pSuite, NetworkInterfaceTest, testMap);
	CppUnit_addTest(pSuite, NetworkInterfaceTest, testForName);
	CppUnit_addTest(pSuite, NetworkInterfaceTest, testForAddress);
	CppUnit_addTest(pSuite, NetworkInterfaceTest, testForIndex);
	CppUnit_addTest(pSuite, NetworkInterfaceTest, testMapIpOnly);
	CppUnit_addTest(pSuite, NetworkInterfaceTest, testMapUpOnly);
	CppUnit_addTest(pSuite, NetworkInterfaceTest, testListMapConformance);

	return pSuite;
}


#endif // POCO_NET_HAS_INTERFACE
