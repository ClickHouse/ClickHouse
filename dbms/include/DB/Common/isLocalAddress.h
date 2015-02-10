#pragma once

#include <DB/Core/Types.h>
#include <Poco/Util/Application.h>
#include <Poco/Net/NetworkInterface.h>
#include <Poco/Net/SocketAddress.h>

namespace DB
{

inline bool isLocalAddress(const Poco::Net::SocketAddress & address)
{
	const UInt16 clickhouse_port = Poco::Util::Application::instance().config().getInt("tcp_port", 0);
	static auto interfaces = Poco::Net::NetworkInterface::list();

	if (clickhouse_port == address.port())
	{
		return interfaces.end() != std::find_if(interfaces.begin(), interfaces.end(),
			[&] (const Poco::Net::NetworkInterface & interface) {
				return interface.address() == address.host();
			});
	}

	return false;
}

}
