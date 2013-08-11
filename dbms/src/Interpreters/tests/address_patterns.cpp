#include <iostream>
#include <DB/Interpreters/Users.h>


using namespace DB;


namespace test
{
	/// Проверяет соответствие адреса одному из адресов хоста.
	class HostExactPattern : public IAddressPattern
	{
	private:
		String host;

	public:
		HostExactPattern(const String & host_) : host(host_) {}

		bool contains(const Poco::Net::IPAddress & addr) const
		{
			Poco::Net::IPAddress addr_v6 = toIPv6(addr);

			addrinfo * ai = NULL;

			addrinfo hints;
			memset(&hints, 0, sizeof(hints));
			hints.ai_family = AF_INET6;
			hints.ai_flags |= AI_V4MAPPED | AI_ALL;
			
			int ret = getaddrinfo(host.c_str(), NULL, NULL, &ai);
			if (0 != ret)
				throw Exception("Cannot getaddrinfo: " + std::string(gai_strerror(ret)), ErrorCodes::DNS_ERROR);

			try
			{
				for (; ai != NULL; ai = ai->ai_next)
				{
					if (ai->ai_addrlen && ai->ai_addr)
					{
						if (ai->ai_family == AF_INET6)
						{
							char buf[1024];
							std::cerr << inet_ntop(ai->ai_family, &reinterpret_cast<sockaddr_in6*>(ai->ai_addr)->sin6_addr, buf, sizeof(buf)) << std::endl;

							std::cerr << Poco::Net::IPAddress(
								&reinterpret_cast<sockaddr_in6*>(ai->ai_addr)->sin6_addr, sizeof(in6_addr),
								reinterpret_cast<sockaddr_in6*>(ai->ai_addr)->sin6_scope_id).toString() << std::endl;

							if (addr_v6 == Poco::Net::IPAddress(
								&reinterpret_cast<sockaddr_in6*>(ai->ai_addr)->sin6_addr, sizeof(in6_addr),
								reinterpret_cast<sockaddr_in6*>(ai->ai_addr)->sin6_scope_id))
							{
								return true;
							}
						}
						else if (ai->ai_family == AF_INET)
						{
							char buf[1024];
							std::cerr << inet_ntop(ai->ai_family, &reinterpret_cast<sockaddr_in*>(ai->ai_addr)->sin_addr, buf, sizeof(buf)) << std::endl;

							std::cerr << Poco::Net::IPAddress(
								&reinterpret_cast<sockaddr_in*>(ai->ai_addr)->sin_addr, sizeof(in_addr)).toString() << std::endl;

							if (addr_v6 == toIPv6(Poco::Net::IPAddress(
								&reinterpret_cast<sockaddr_in*>(ai->ai_addr)->sin_addr, sizeof(in_addr))))
							{
								return true;
							}
						}
					}
				}
			}
			catch (...)
			{
				freeaddrinfo(ai);
				throw;
			}
			freeaddrinfo(ai);

			return false;
		}
	};

	/// Проверяет соответствие PTR-записи для адреса регекспу (и дополнительно проверяет, что PTR-запись резолвится обратно в адрес клиента).
	class HostRegexpPattern : public IAddressPattern
	{
	private:
		Poco::RegularExpression host_regexp;

	public:
		HostRegexpPattern(const String & host_regexp_) : host_regexp(host_regexp_) {}

		bool contains(const Poco::Net::IPAddress & addr) const
		{
			Poco::Net::SocketAddress sock_addr(addr, 0);

			/// Резолвим вручную, потому что в Poco нет такой функциональности.
			char domain[1024];
			int ret = getnameinfo(sock_addr.addr(), sock_addr.length(), domain, sizeof(domain), NULL, 0, NI_NAMEREQD);
			if (0 != ret)
				throw Exception("Cannot getnameinfo: " + std::string(gai_strerror(ret)), ErrorCodes::DNS_ERROR);

			String domain_str = domain;
			Poco::RegularExpression::Match match;

			std::cerr << domain_str << ", " << host_regexp.match(domain_str, match) << std::endl;

			if (host_regexp.match(domain_str, match) && HostExactPattern(domain_str).contains(addr))
				return true;

			return false;
		}
	};
};


int main(int argc, char ** argv)
{
	try
	{
		std::cerr << test::HostRegexpPattern(argv[1]).contains(Poco::Net::IPAddress(argv[2])) << std::endl;
	}
	catch (const Exception & e)
	{
		std::cerr << "Exception " << e.what() << ": " << e.displayText() << "\n" << e.getStackTrace().toString();
		return 1;
	}
	
	return 0;
}
