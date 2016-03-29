#include <DB/Core/Types.h>
#include <Poco/Util/Application.h>
#include <Poco/Net/NetworkInterface.h>
#include <Poco/Net/SocketAddress.h>

#include <DB/Common/isLocalAddress.h>


namespace DB
{

bool isLocalAddress(const Poco::Net::SocketAddress & address)
{
	const UInt16 clickhouse_port = Poco::Util::Application::instance().config().getInt("tcp_port", 0);
	static auto interfaces = Poco::Net::NetworkInterface::list();

	if (clickhouse_port == address.port())
	{
		return interfaces.end() != std::find_if(interfaces.begin(), interfaces.end(),
			[&] (const Poco::Net::NetworkInterface & interface)
			{
				/** Сравниваем адреса без учёта scope.
				  * Теоретически, это может быть неверно - зависит от настройки route
				  *  - через какой интерфейс мы на самом деле будем обращаться к заданному адресу.
				  */
				return interface.address().length() == address.host().length()
					&& 0 == memcmp(interface.address().addr(), address.host().addr(), address.host().length());
			});
	}

	return false;
}

}
