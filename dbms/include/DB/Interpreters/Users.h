#pragma once

#include <string.h>

#include <Poco/RegularExpression.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/DNS.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/IO/ReadHelpers.h>


namespace DB
{


/// Позволяет проверить соответствие адреса шаблону.
class IAddressPattern
{
public:
	virtual bool contains(const Poco::Net::IPAddress & addr) const = 0;
	virtual ~IAddressPattern() {}

	static Poco::Net::IPAddress toIPv6(const Poco::Net::IPAddress addr)
	{
		if (addr.family() == Poco::Net::IPAddress::IPv6)
			return addr;

		return Poco::Net::IPAddress("::FFFF:" + addr.toString());
	}
};


/// IP-адрес или маска подсети. Например, 213.180.204.3 или 10.0.0.1/8 или 2a02:6b8::3 или 2a02:6b8::3/64.
class IPAddressPattern : public IAddressPattern
{
private:
	/// Адрес маски. Всегда переводится в IPv6.
	Poco::Net::IPAddress mask_address;
	/// Количество бит в маске.
	UInt8 prefix_bits;

public:
	explicit IPAddressPattern(const String & str)
	{
		const char * pos = strchr(str.c_str(), '/');

		if (NULL == pos)
		{
			construct(Poco::Net::IPAddress(str));
		}
		else
		{
			String addr(str, 0, pos - str.c_str());
			UInt8 prefix_bits_ = parse<UInt8>(pos + 1);

			construct(Poco::Net::IPAddress(addr), prefix_bits_);
		}
	}
	
	bool contains(const Poco::Net::IPAddress & addr) const
	{
		return prefixBitsEquals(reinterpret_cast<const char *>(toIPv6(addr).addr()), reinterpret_cast<const char *>(mask_address.addr()), prefix_bits);
	}

private:
	void construct(const Poco::Net::IPAddress & mask_address_)
	{
		mask_address = toIPv6(mask_address_);
		prefix_bits = 128;
	}

	void construct(const Poco::Net::IPAddress & mask_address_, UInt8 prefix_bits_)
	{
		mask_address = toIPv6(mask_address_);
		prefix_bits = mask_address_.family() == Poco::Net::IPAddress::IPv4
			? prefix_bits_ + 96
			: prefix_bits_;
	}

	static bool prefixBitsEquals(const char * lhs, const char * rhs, UInt8 prefix_bits)
	{
		UInt8 prefix_bytes = prefix_bits / 8;
		UInt8 remaining_bits = prefix_bits % 8;

		return 0 == memcmp(lhs, rhs, prefix_bytes)
			&& (remaining_bits % 8 == 0
				 || (lhs[prefix_bytes] >> (8 - remaining_bits)) == (rhs[prefix_bytes] >> (8 - remaining_bits)));
	}
};


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
			if (toIPv6(entry.addresses()[i]) == addr_v6)
				return true;

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
		Poco::RegularExpression::Match match;
		Poco::Net::HostEntry entry = Poco::Net::DNS::hostByAddress(addr);

		for (size_t i = 0, size = entry.aliases().size(); i < size; ++i)
			if (host_regexp.match(entry.aliases()[i], match) && HostExactPattern(entry.aliases()[i]).contains(addr))
				return true;

		return false;
	}
};



class AddressPatterns
{
private:
	typedef std::vector<SharedPtr<IAddressPattern> > Container;
	Container patterns;

public:
	bool contains(const Poco::Net::IPAddress & addr) const
	{
		for (size_t i = 0, size = patterns.size(); i < size; ++i)
			if (patterns[i]->contains(addr))
				return true;

		return false;
	}

	void addFromConfig(const String & config_elem)
	{
		Poco::Util::AbstractConfiguration & config = Poco::Util::Application::instance().config();

		Poco::Util::AbstractConfiguration::Keys config_keys;
		config.keys(config_elem, config_keys);

		for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = config_keys.begin(); it != config_keys.end(); ++it)
		{
			SharedPtr<IAddressPattern> pattern;
			String value = config.getString(config_elem + "." + *it);

			if (0 == it->compare(0, strlen("ip"), "ip"))
				pattern = new IPAddressPattern(value);
			else if (0 == it->compare(0, strlen("host_regexp"), "host_regexp"))
				pattern = new HostRegexpPattern(value);
			else if (0 == it->compare(0, strlen("host"), "host"))
				pattern = new HostExactPattern(value);
			else
				throw Exception("Unknown address pattern type: " + *it, ErrorCodes::UNKNOWN_ADDRESS_PATTERN_TYPE);
			
			patterns.push_back(pattern);
		}
	}
};


/** Пользователь и ACL.
  */
struct User
{
	String name;

	/// Требуемый пароль. Хранится в открытом виде.
	String password;

	String profile;
	String quota;

	AddressPatterns addresses;

	User(const String & name_, const String & config_elem)
		: name(name_)
	{
		Poco::Util::AbstractConfiguration & config = Poco::Util::Application::instance().config();

		password 	= config.getString(config_elem + ".password");
		profile 	= config.getString(config_elem + ".profile");
		quota 		= config.getString(config_elem + ".quota");

		addresses.addFromConfig(config_elem + ".networks");
	}

	/// Для вставки в контейнер.
	User() {}
};


/// Известные пользователи.
class Users
{
private:
	typedef std::map<String, User> Container;
	Container cont;
	
public:
	void initFromConfig()
	{
		Poco::Util::AbstractConfiguration & config = Poco::Util::Application::instance().config();

		Poco::Util::AbstractConfiguration::Keys config_keys;
		config.keys("users", config_keys);

		for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = config_keys.begin(); it != config_keys.end(); ++it)
			cont[*it] = User(*it, "users." + *it);
	}

	const User & get(const String & name, const String & password, const Poco::Net::IPAddress & address) const
	{
		Container::const_iterator it = cont.find(name);

		if (cont.end() == it)
			throw Exception("Unknown user " + name, ErrorCodes::UNKNOWN_USER);

		if (!it->second.addresses.contains(address))
			throw Exception("User " + name + " is not allowed to connect from address " + address.toString(), ErrorCodes::IP_ADDRESS_NOT_ALLOWED);

		if (password != it->second.password)
		{
			if (password.empty())
				throw Exception("Password required for user " + name, ErrorCodes::REQUIRED_PASSWORD);
			else
				throw Exception("Wrong password for user " + name, ErrorCodes::WRONG_PASSWORD);
		}

		return it->second;
	}
};


}
