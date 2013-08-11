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
			Poco::Net::HostEntry entry = Poco::Net::DNS::hostByName(host);

			for (size_t i = 0, size = entry.addresses().size(); i < size; ++i)
			{
				std::cerr << i << ", " << entry.addresses()[i].toString() << std::endl;
				if (toIPv6(entry.addresses()[i]) == addr_v6)
					return true;
			}

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
			int gai_errno = getnameinfo(sock_addr.addr(), sock_addr.length(), domain, sizeof(domain), NULL, 0, NI_NAMEREQD);
			if (0 != gai_errno)
				throw Exception("Cannot getnameinfo: " + std::string(gai_strerror(gai_errno)), ErrorCodes::CANNOT_GETNAMEINFO);

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
