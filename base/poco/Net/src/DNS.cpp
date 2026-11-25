//
// DNS.cpp
//
// Library: Net
// Package: NetCore
// Module:  DNS
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/DNS.h"
#include "Poco/Net/NetException.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Environment.h"
#include "Poco/NumberFormatter.h"
#include "Poco/NumberParser.h"
#include "Poco/RWLock.h"
#include "Poco/String.h"
#include "Poco/TextIterator.h"
#include "Poco/TextConverter.h"
#include "Poco/UTF8Encoding.h"
#include "Poco/UTF32Encoding.h"
#include "Poco/Unicode.h"
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <exception>
#include <cstring>
#include <fstream>
#include <memory>
#include <new>
#include <optional>
#include <sstream>
#include <vector>

/// On macOS, host names are resolved by the system (see the comment below), on other platforms by c-ares.
#if !defined(POCO_NET_DNS_USE_SYSTEM_RESOLVER)
#if defined(__APPLE__)
#define POCO_NET_DNS_USE_SYSTEM_RESOLVER 1
#else
#define POCO_NET_DNS_USE_SYSTEM_RESOLVER 0
#endif
#endif

#if !POCO_NET_DNS_USE_SYSTEM_RESOLVER
#include <ares.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#if !defined(__EMSCRIPTEN__)
#include <ifaddrs.h>
#endif
#endif


#if defined(POCO_HAVE_LIBRESOLV)
#include <resolv.h>
#endif


using Poco::Environment;
using Poco::NumberFormatter;
using Poco::IOException;


namespace Poco {
namespace Net {


typedef Poco::UInt32 punycode_uint;


enum
{
	punycode_success = 0,
	punycode_overflow = -1,
	punycode_big_output = -2,
	punycode_bad_input = -3
};


static int punycode_encode(size_t input_length, const punycode_uint input[], size_t* output_length, char output[]);
static int punycode_decode(size_t input_length, const char input[], size_t* output_length, punycode_uint output[]);


#if defined(POCO_HAVE_LIBRESOLV)
static Poco::RWLock resolverLock;
#endif


#if !POCO_NET_DNS_USE_SYSTEM_RESOLVER
namespace
{


/// Host names are resolved with c-ares rather than with `getaddrinfo` and `getnameinfo` of the libc,
/// so that resolution does not depend on the plugins of the Name Service Switch that the libc loads
/// at run time (they are not available under QEMU-user or in a minimal chroot, and some of them
/// answer queries from unexpected sources), and so that every lookup is bounded by the timeouts and
/// attempts of `/etc/resolv.conf`. The sources are `/etc/hosts` and the DNS servers of
/// `/etc/resolv.conf`, in the order given by the `hosts` line of `/etc/nsswitch.conf`.
///
/// A channel is created for every lookup: the bundled c-ares is built without thread support, so a
/// channel cannot be shared between threads, and a new channel reads the current configuration.
/// The results are cached by `DNSResolver`, so the cost of creating a channel does not matter.
///
/// macOS is the exception: host names are resolved with `getaddrinfo` and `getnameinfo` there. Its libc
/// has no plugins to load, and the configuration of DNS is not in `/etc/resolv.conf` but in the system
/// configuration daemon, which is queried through `mDNSResponder` (scoped and domain-specific resolvers,
/// `/etc/resolver`, VPN). c-ares reads only a part of it through a private API, and a process that sends
/// DNS queries itself can be refused the access to the DNS servers of the local network, so with c-ares
/// no name was resolved on the macOS machines of the CI.
class AresChannel
{
public:
	/// `lookups` overrides the sources of the configuration: "f" for the hosts file, "b" for DNS.
	explicit AresChannel(const char* lookups = nullptr)
	{
		struct ares_options options;
		std::memset(&options, 0, sizeof(options));
		options.sock_state_cb = &AresChannel::socketStateCallback;
		options.sock_state_cb_data = this;
		int optmask = ARES_OPT_SOCK_STATE_CB;
		if (lookups)
		{
			options.lookups = const_cast<char*>(lookups);
			optmask |= ARES_OPT_LOOKUPS;
		}
		int rc = ares_init_options(&_channel, &options, optmask);
		if (rc == ARES_ENOMEM)
			throw std::bad_alloc();
		if (rc != ARES_SUCCESS)
			throw DNSException("Cannot initialize the DNS resolver", ares_strerror(rc), EAI_FAIL);
	}

	~AresChannel()
	{
		ares_destroy(_channel);
	}

	AresChannel(const AresChannel&) = delete;
	AresChannel& operator=(const AresChannel&) = delete;

	ares_channel_t* get()
	{
		return _channel;
	}

	/// Runs the event loop until all the queries of the channel complete.
	void wait()
	{
		rethrowCallbackException();
		std::vector<ares_fd_events_t> events;
		while (ares_queue_active_queries(_channel) > 0)
		{
			struct timeval tv;
			struct timeval* timeout = ares_timeout(_channel, nullptr, &tv);
			if (!timeout && _fds.empty())
				throw DNSException("The DNS resolver has active queries, but neither sockets nor timeouts to wait for", "", EAI_FAIL);

			int timeout_ms = timeout ? static_cast<int>(tv.tv_sec * 1000 + (tv.tv_usec + 999) / 1000) : -1;
			int rc = ::poll(_fds.data(), static_cast<nfds_t>(_fds.size()), timeout_ms);
			if (rc < 0)
			{
				if (errno == EINTR)
					continue;
				throw NetException("Cannot poll the sockets of the DNS resolver", errno);
			}

			events.clear();
			for (const auto& fd : _fds)
			{
				unsigned int flags = 0;
				if (fd.revents & (POLLIN | POLLERR | POLLHUP))
					flags |= ARES_FD_EVENT_READ;
				if (fd.revents & POLLOUT)
					flags |= ARES_FD_EVENT_WRITE;
				if (flags)
					events.push_back(ares_fd_events_t{fd.fd, flags});
			}

			/// With no events, this processes the timeouts.
			const ares_status_t status = ares_process_fds(_channel, events.data(), events.size(), ARES_PROCESS_FLAG_NONE);
			rethrowCallbackException();

			/// The only error that c-ares reports here is a failed allocation (other errors are handled by its retries
			/// and reported to the callback of the query). It can leave a query without a timeout, so the loop must
			/// not continue: it would wait in `poll` forever if the DNS server did not answer.
			if (status == ARES_ENOMEM)
				throw std::bad_alloc();
			if (status != ARES_SUCCESS)
				throw DNSException(std::string("Cannot process the events of the DNS resolver: ") + ares_strerror(status), "", EAI_FAIL);
		}
	}

private:
	/// An exception must not unwind through the frames of c-ares: it would skip its cleanup and leak the socket.
	/// It is stored and rethrown after the c-ares function returns, and the destructor releases the channel.
	static void socketStateCallback(void* data, ares_socket_t socket, int readable, int writable)
	{
		auto* self = static_cast<AresChannel*>(data);
		try
		{
			auto& fds = self->_fds;
			auto it = std::find_if(fds.begin(), fds.end(), [socket](const pollfd& fd) { return fd.fd == socket; });

			if (!readable && !writable)
			{
				if (it != fds.end())
					fds.erase(it);
				return;
			}

			if (it == fds.end())
			{
				fds.push_back(pollfd{socket, 0, 0});
				it = std::prev(fds.end());
			}
			it->events = static_cast<short>((readable ? POLLIN : 0) | (writable ? POLLOUT : 0));
		}
		catch (...)
		{
			if (!self->_callbackException)
				self->_callbackException = std::current_exception();
		}
	}

	void rethrowCallbackException()
	{
		if (_callbackException)
			std::rethrow_exception(_callbackException);
	}

	ares_channel_t* _channel = nullptr;
	std::vector<pollfd> _fds;
	std::exception_ptr _callbackException;
};


/// Throws the same exceptions that the `getaddrinfo` errors produce, with the `EAI_*` code.
[[noreturn]] void throwAresError(int status, const std::string& arg)
{
	switch (status)
	{
	case ARES_ENOTFOUND:
		throw HostNotFoundException(arg, EAI_NONAME);
	case ARES_ENODATA:
#if defined(EAI_NODATA)
		throw NoAddressFoundException(arg, EAI_NODATA);
#else
		throw NoAddressFoundException(arg, EAI_NONAME);
#endif
	case ARES_ENOMEM:
		throw std::bad_alloc();
	case ARES_ETIMEOUT:
	case ARES_ECONNREFUSED:
	case ARES_ESERVFAIL:
	case ARES_EREFUSED:
		throw DNSException(std::string("Temporary DNS error while resolving: ") + ares_strerror(status), arg, EAI_AGAIN);
	default:
		throw DNSException(std::string("Non recoverable DNS error while resolving: ") + ares_strerror(status), arg, EAI_FAIL);
	}
}


/// Whether the host has IPv4 and IPv6 addresses other than the loopback ones, for `AI_ADDRCONFIG`.
/// This is the check of the glibc: the addresses of the interfaces that are down count too.
void configuredAddressFamilies(bool& seenIPv4, bool& seenIPv6)
{
	seenIPv4 = false;
	seenIPv6 = false;
#if !defined(__EMSCRIPTEN__)
	struct ifaddrs* addresses = nullptr;
	if (getifaddrs(&addresses) != 0)
		throw NetException("Cannot get the addresses of the network interfaces to resolve with AI_ADDRCONFIG", errno);

	for (struct ifaddrs* it = addresses; it; it = it->ifa_next)
	{
		if (!it->ifa_addr)
			continue;
		if (it->ifa_addr->sa_family == AF_INET)
		{
			if (reinterpret_cast<const struct sockaddr_in*>(it->ifa_addr)->sin_addr.s_addr != htonl(INADDR_LOOPBACK))
				seenIPv4 = true;
		}
		else if (it->ifa_addr->sa_family == AF_INET6)
		{
			if (!IN6_IS_ADDR_LOOPBACK(&reinterpret_cast<const struct sockaddr_in6*>(it->ifa_addr)->sin6_addr))
				seenIPv6 = true;
		}
	}
	freeifaddrs(addresses);
#endif
}


/// Parses an IPv4 or IPv6 address literal, including what `IPAddress::tryParse` rejects: the all-zero IPv6
/// address `::` and a numeric zone index (`fe80::1%2`; it accepts only interface names, such as `fe80::1%eth0`).
bool tryParseAddressLiteral(const std::string& hostname, IPAddress& address)
{
	if (IPAddress::tryParse(hostname, address))
		return true;
#if defined(POCO_HAVE_IPv6)
	const auto percent = hostname.find('%');
	const std::string addressPart = hostname.substr(0, percent);
	struct in6_addr address6;
	if (inet_pton(AF_INET6, addressPart.c_str(), &address6) != 1)
		return false;

	Poco::UInt32 scope = 0;
	if (percent != std::string::npos)
	{
		const std::string zone = hostname.substr(percent + 1);
		unsigned index = 0;
		if (zone.empty()
			|| !std::all_of(zone.begin(), zone.end(), [](unsigned char c) { return c >= '0' && c <= '9'; })
			|| !NumberParser::tryParseUnsigned(zone, index))
			return false;
		scope = index;
	}
	address = IPAddress(&address6, sizeof(address6), scope);
	return true;
#else
	return false;
#endif
}


/// The lines of the hosts file that contain a name.
struct HostsFileEntry
{
	/// The first name of the first line that contains the name.
	std::string canonicalName;
	HostEntry::AddressList addresses;
};


/// Looks up a name in the hosts file as the glibc does: only the lines that contain the name count.
/// c-ares merges the lines that share a name or an address into one entry, so with
/// "127.0.0.1 localhost", "::1 localhost" and "127.0.0.1 myhost", it resolves `myhost` to `::1` too,
/// with `localhost` as the canonical name. The file is the one that c-ares reads.
/// A missing file contains no names; any other error to read it is thrown, so that the addresses
/// of a name are never taken from the merged entries of c-ares unchecked.
std::optional<HostsFileEntry> lookupHostsFile(const std::string& hostname)
{
	const char* path = "/etc/hosts";
	/// The file is owned from the moment it is opened: reading it into a string can throw.
	struct FileCloser
	{
		void operator()(std::FILE* f) const { std::fclose(f); }
	};
	std::unique_ptr<std::FILE, FileCloser> file(std::fopen(path, "re"));
	if (!file)
	{
		if (errno == ENOENT)
			return std::nullopt;
		throw DNSException(std::string("Cannot open the hosts file ") + path + ": " + std::strerror(errno), hostname, EAI_SYSTEM);
	}

	std::string content;
	char buffer[4096];
	size_t size = 0;
	while ((size = std::fread(buffer, 1, sizeof(buffer), file.get())) > 0)
		content.append(buffer, size);
	const bool failed = std::ferror(file.get());
	const int error = errno;
	file.reset();
	if (failed)
		throw DNSException(std::string("Cannot read the hosts file ") + path + ": " + std::strerror(error), hostname, EAI_SYSTEM);

	std::optional<HostsFileEntry> result;
	std::istringstream lines(content);
	std::string line;
	while (std::getline(lines, line))
	{
		line.erase(std::find(line.begin(), line.end(), '#'), line.end());
		std::istringstream tokens(line);
		std::string addressString;
		if (!(tokens >> addressString))
			continue;

		IPAddress address;
		if (!IPAddress::tryParse(addressString, address))
			continue;

		std::string firstName;
		std::string name;
		bool matches = false;
		while (tokens >> name)
		{
			if (firstName.empty())
				firstName = name;
			if (Poco::icompare(name, hostname) == 0)
				matches = true;
		}
		if (!matches)
			continue;

		if (!result)
			result.emplace(HostsFileEntry{firstName, {}});
		if (std::find(result->addresses.begin(), result->addresses.end(), address) == result->addresses.end())
			result->addresses.push_back(address);
	}
	return result;
}


/// Whether an address returned by c-ares is one of the addresses of the hosts file,
/// also as an IPv4-mapped IPv6 address (`DNS_HINT_AI_V4MAPPED`).
bool isListedAddress(const IPAddress& address, const HostEntry::AddressList& listed)
{
	for (const auto& candidate : listed)
	{
		if (address == candidate)
			return true;
#if defined(POCO_HAVE_IPv6)
		if (address.family() == IPAddress::IPv6 && candidate.family() == IPAddress::IPv4 && address.isIPv4Mapped()
			&& std::memcmp(static_cast<const char*>(address.addr()) + 12, candidate.addr(), 4) == 0)
			return true;
#endif
	}
	return false;
}


/// The localhost names of RFC 6761, which c-ares never sends to DNS and always resolves to the loopback addresses.
bool isLocalhostName(const std::string& hostname)
{
	static const std::string suffix = ".localhost";
	if (Poco::icompare(hostname, "localhost") == 0)
		return true;
	return hostname.size() >= suffix.size() && Poco::icompare(hostname, hostname.size() - suffix.size(), suffix.size(), suffix) == 0;
}


/// The loopback addresses of an address family (of both families for `AF_UNSPEC`), which a localhost name resolves to.
HostEntry::AddressList loopbackAddresses(int family)
{
	HostEntry::AddressList addresses;
	if (family != AF_INET6)
		addresses.push_back(IPAddress("127.0.0.1"));
#if defined(POCO_HAVE_IPv6)
	if (family != AF_INET)
		addresses.push_back(IPAddress("::1"));
#endif
	return addresses;
}


/// Reads the values of one `hosts` entry of a configuration file into the sources ("f" for the hosts file,
/// "b" for DNS), as c-ares does. Unknown values (other NSS modules, actions such as `[NOTFOUND=return]`) are skipped.
void parseLookups(const std::string& values, const char* separators, std::string& lookups)
{
	std::string result;
	size_t pos = 0;
	while (pos < values.size())
	{
		const size_t end = std::min(values.find_first_of(separators, pos), values.size());
		const std::string value = Poco::trim(values.substr(pos, end - pos));
		pos = end + 1;

		char source = 0;
		for (const char* name : {"dns", "bind", "bind4", "bind6", "resolv", "resolve"})
			if (Poco::icompare(value, name) == 0)
				source = 'b';
		for (const char* name : {"files", "file", "local", "local4", "local6"})
			if (Poco::icompare(value, name) == 0)
				source = 'f';
		if (source && result.find(source) == std::string::npos)
			result += source;
	}
	if (!result.empty())
		lookups = result;
}


/// Calls `callback` for each trimmed line of a configuration file that does not begin with a comment.
template <typename Callback>
void forEachConfigLine(const char* path, Callback&& callback)
{
	std::ifstream file(path);
	std::string line;
	while (std::getline(file, line))
	{
		line = Poco::trim(line);
		if (!line.empty() && line[0] != '#')
			callback(line);
	}
}


/// The sources of host names and their order, as c-ares determines them when it initializes a channel:
/// `lookup` of `/etc/resolv.conf`, then the `hosts` entries of `/etc/nsswitch.conf`, `/etc/netsvc.conf`
/// and `/etc/svc.conf` (the last one wins), "fb" by default. c-ares does not expose the result, and
/// `hostByName` needs it to know which source an answer comes from.
std::string lookupOrder()
{
	std::string lookups = "fb";

	forEachConfigLine("/etc/resolv.conf", [&](const std::string& line)
	{
		if (line[0] == ';')
			return;
		const size_t end = line.find_first_of(" \t");
		if (end == std::string::npos)
			return;
		const std::string option = line.substr(0, end);
		if (option != "lookup" && option != "hostresorder")
			return;
		std::string value = line.substr(end);
		value.erase(std::min(value.find_first_of("#;"), value.size()));
		parseLookups(value, " \t", lookups);
	});

	forEachConfigLine("/etc/nsswitch.conf", [&](const std::string& line)
	{
		const size_t colon = line.find(':');
		if (colon != std::string::npos && Poco::trim(line.substr(0, colon)) == "hosts")
			parseLookups(line.substr(colon + 1), " \t", lookups);
	});

	for (const char* path : {"/etc/netsvc.conf", "/etc/svc.conf"})
	{
		forEachConfigLine(path, [&](const std::string& line)
		{
			const size_t equals = line.find('=');
			if (equals != std::string::npos && Poco::trim(line.substr(0, equals)) == "hosts")
				parseLookups(line.substr(equals + 1), ",", lookups);
		});
	}

	return lookups;
}


struct AddrInfoResult
{
	int status = ARES_SUCCESS;
	struct ares_addrinfo* result = nullptr;

	AddrInfoResult() = default;
	AddrInfoResult(const AddrInfoResult&) = delete;
	AddrInfoResult& operator=(const AddrInfoResult&) = delete;

	~AddrInfoResult()
	{
		if (result)
			ares_freeaddrinfo(result);
	}

	static void callback(void* arg, int status, int /*timeouts*/, struct ares_addrinfo* result)
	{
		auto* self = static_cast<AddrInfoResult*>(arg);
		self->status = status;
		self->result = result;
	}
};


struct NameInfoResult
{
	int status = ARES_SUCCESS;
	std::string host;
	/// See `AresChannel::socketStateCallback`.
	std::exception_ptr exception;

	static void callback(void* arg, int status, int /*timeouts*/, char* node, char* /*service*/)
	{
		auto* self = static_cast<NameInfoResult*>(arg);
		self->status = status;
		try
		{
			if (status == ARES_SUCCESS && node)
				self->host = node;
		}
		catch (...)
		{
			self->exception = std::current_exception();
		}
	}
};


} // namespace
#endif


#if POCO_NET_DNS_USE_SYSTEM_RESOLVER


HostEntry DNS::hostByName(const std::string& hostname, unsigned hintFlags)
{
	struct addrinfo* pAI = nullptr;
	struct addrinfo hints;
	std::memset(&hints, 0, sizeof(hints));
	hints.ai_flags = hintFlags;
	int rc = getaddrinfo(hostname.c_str(), nullptr, &hints, &pAI);
	if (rc != 0)
	{
		aierror(rc, hostname); // throws an appropriate exception
		throw DNSException("Cannot resolve the host name", hostname, rc);
	}

	struct AddrInfoDeleter
	{
		void operator()(struct addrinfo* info) const { freeaddrinfo(info); }
	};
	std::unique_ptr<struct addrinfo, AddrInfoDeleter> holder(pAI);
	return HostEntry(pAI);
}


HostEntry DNS::hostByAddress(const IPAddress& address, unsigned hintFlags)
{
	SocketAddress sa(address, 0);
	char fqname[1025];
	int rc = getnameinfo(sa.addr(), sa.length(), fqname, sizeof(fqname), nullptr, 0, NI_NAMEREQD);
	if (rc != 0)
	{
		aierror(rc, address.toString()); // throws an appropriate exception
		throw DNSException("Cannot resolve the address", address.toString(), rc);
	}
	return hostByName(fqname, hintFlags);
}


#else


HostEntry DNS::hostByName(const std::string& hostname, unsigned hintFlags)
{
	int family = AF_UNSPEC;
	if (hintFlags & DNS_HINT_AI_ADDRCONFIG)
	{
		/// As in the glibc: if the host has addresses of only one family, resolve only to this family.
		bool seenIPv4 = false;
		bool seenIPv6 = false;
		configuredAddressFamilies(seenIPv4, seenIPv6);
		if (seenIPv4 != seenIPv6)
			family = seenIPv4 ? AF_INET : AF_INET6;
	}

	IPAddress literal;
	if (tryParseAddressLiteral(hostname, literal))
	{
		if (family != AF_UNSPEC && literal.af() != family)
		{
#if defined(EAI_ADDRFAMILY)
			throw DNSException("Address family for hostname not supported", hostname, EAI_ADDRFAMILY);
#else
			throw DNSException("Address family for hostname not supported", hostname, EAI_FAMILY);
#endif
		}
		return HostEntry(hostname, HostEntry::AddressList{literal});
	}

	if (hintFlags & DNS_HINT_AI_NUMERICHOST)
		throw HostNotFoundException(hostname, EAI_NONAME);

	struct ares_addrinfo_hints hints;
	std::memset(&hints, 0, sizeof(hints));
	hints.ai_family = family;
	hints.ai_flags = ARES_AI_CANONNAME;
	if (hintFlags & DNS_HINT_AI_V4MAPPED)
		hints.ai_flags |= ARES_AI_V4MAPPED;
	if (hintFlags & DNS_HINT_AI_ALL)
		hints.ai_flags |= ARES_AI_ALL;

	/// The sources are queried one by one rather than by one c-ares lookup, because the answer of the hosts file
	/// has to be corrected (see `lookupHostsFile`), and only it: a name can have other addresses in DNS.
	/// As in c-ares, the next source is tried when a name is not found, and other errors are thrown.
	const bool localhost = isLocalhostName(hostname);
	int status = ARES_ENOTFOUND;
	for (const char source : lookupOrder())
	{
		std::optional<HostsFileEntry> hostsFileEntry;
		if (source == 'f')
		{
			/// The same read gives both whether the hosts file has the name and its addresses.
			/// c-ares answers from the hosts file only for the names that it contains, and for the localhost names.
			hostsFileEntry = lookupHostsFile(hostname);

			/// An entry without an address of the requested family (`AI_ADDRCONFIG`) is as good as no entry.
			/// It must not be passed to c-ares: for such an entry it reports the initial status of its lookup,
			/// `ARES_ECONNREFUSED`, which would be thrown instead of trying the next source.
			const bool familyMiss = hostsFileEntry && family != AF_UNSPEC
				&& std::none_of(hostsFileEntry->addresses.begin(), hostsFileEntry->addresses.end(),
					[family](const IPAddress& address) { return address.af() == family; });
			if (familyMiss)
			{
				hostsFileEntry.reset();
				status = ARES_ENODATA;
			}

			if (!hostsFileEntry)
			{
				if (!localhost)
					continue;

				/// A localhost name without a usable entry resolves to the loopback addresses, and only to them.
				HostEntry::AddressList loopback = loopbackAddresses(family);

				/// The name is in the hosts file, but without an address of the requested family. c-ares must not
				/// be asked about it: it looks for an address of this family in its merged entry, and returns
				/// the address of an unrelated name that shares an address of the other family with this one.
				if (familyMiss)
					return HostEntry(hostname, loopback);

				/// The name is not in the hosts file, and c-ares synthesizes the loopback addresses.
				/// Its answer is checked against them in the same way as against the addresses of an entry.
				hostsFileEntry.emplace(HostsFileEntry{hostname, std::move(loopback)});
			}
		}
		else if (source != 'b' || localhost)
			continue;

		const char lookups[] = {source, '\0'};
		AddrInfoResult response;
		{
			AresChannel channel(lookups);
			ares_getaddrinfo(channel.get(), hostname.c_str(), nullptr, &hints, &AddrInfoResult::callback, &response);
			channel.wait();
		}

		status = response.status;
		if (status == ARES_SUCCESS && (!response.result || !response.result->nodes))
			status = ARES_ENODATA;
		if (status == ARES_ENOTFOUND || status == ARES_ENODATA)
			continue;
		if (status != ARES_SUCCESS)
			throwAresError(status, hostname);

		HostEntry::AddressList addresses;
		for (const struct ares_addrinfo_node* node = response.result->nodes; node; node = node->ai_next)
		{
			if (!node->ai_addr)
				continue;
			if (node->ai_addr->sa_family == AF_INET)
			{
				addresses.push_back(IPAddress(&reinterpret_cast<const struct sockaddr_in*>(node->ai_addr)->sin_addr, sizeof(in_addr)));
			}
#if defined(POCO_HAVE_IPv6)
			else if (node->ai_addr->sa_family == AF_INET6)
			{
				const auto* address6 = reinterpret_cast<const struct sockaddr_in6*>(node->ai_addr);
				addresses.push_back(IPAddress(&address6->sin6_addr, sizeof(in6_addr), address6->sin6_scope_id));
			}
#endif
		}

		/// A name from the hosts file keeps only the addresses of its own lines, and a localhost name that is not
		/// there only the loopback addresses, in the order of c-ares (RFC 6724). Only an answer of DNS is not checked.
		if (hostsFileEntry)
		{
			const auto& listed = hostsFileEntry->addresses;
			std::erase_if(addresses, [&listed](const IPAddress& address) { return !isListedAddress(address, listed); });
			if (addresses.empty())
			{
				status = ARES_ENODATA;
				continue;
			}
			return HostEntry(hostsFileEntry->canonicalName, addresses);
		}

		if (addresses.empty())
		{
			status = ARES_ENODATA;
			continue;
		}

		/// The canonical name is the target of the last CNAME.
		std::string name = response.result->name ? response.result->name : hostname;
		for (const struct ares_addrinfo_cname* cname = response.result->cnames; cname; cname = cname->next)
		{
			if (cname->name)
				name = cname->name;
		}

		return HostEntry(name, addresses);
	}

	throwAresError(status, hostname);
}


HostEntry DNS::hostByAddress(const IPAddress& address, unsigned hintFlags)
{
	SocketAddress sa(address, 0);

	NameInfoResult response;
	{
		AresChannel channel;
		ares_getnameinfo(channel.get(), sa.addr(), static_cast<ares_socklen_t>(sa.length()),
			ARES_NI_LOOKUPHOST | ARES_NI_NAMEREQD, &NameInfoResult::callback, &response);
		channel.wait();
	}

	if (response.exception)
		std::rethrow_exception(response.exception);

	if (response.status != ARES_SUCCESS)
		throwAresError(response.status, address.toString());
	if (response.host.empty())
		throwAresError(ARES_ENOTFOUND, address.toString());

	return hostByName(response.host, hintFlags);
}


#endif


HostEntry DNS::resolve(const std::string& address)
{
	IPAddress ip;
	if (IPAddress::tryParse(address, ip))
	{
		return hostByAddress(ip);
	}
	else if (isIDN(address))
	{
		std::string encoded = encodeIDN(address);
		return hostByName(encoded);
	}
	else
	{
		return hostByName(address);
	}
}


IPAddress DNS::resolveOne(const std::string& address)
{
	const HostEntry& entry = resolve(address);
	if (!entry.addresses().empty())
		return entry.addresses()[0];
	else
		throw NoAddressFoundException(address);
}


HostEntry DNS::thisHost()
{
	return hostByName(hostName());
}


void DNS::reload()
{
#if defined(POCO_HAVE_LIBRESOLV)
	Poco::ScopedWriteRWLock writeLock(resolverLock);
	res_init();
#endif
}


std::string DNS::hostName()
{
	char buffer[256];
	int rc = gethostname(buffer, sizeof(buffer));
	if (rc == 0)
		return std::string(buffer);
	else
		throw NetException("Cannot get host name");
}


bool DNS::isIDN(const std::string& hostname)
{
	for (std::string::const_iterator it = hostname.begin(); it != hostname.end(); ++it)
	{
		if (static_cast<unsigned char>(*it) >= 0x80) return true;
	}
	return false;
}


bool DNS::isEncodedIDN(const std::string& hostname)
{
	return hostname.compare(0, 4, "xn--") == 0 || hostname.find(".xn--") != std::string::npos;
}


std::string DNS::encodeIDN(const std::string& idn)
{
	std::string encoded;
	std::string::const_iterator it = idn.begin();
	std::string::const_iterator end = idn.end();
	while (it != end)
	{
		std::string label;
		bool mustEncode = false;
		while (it != end && *it != '.')
		{
			if (static_cast<unsigned char>(*it) >= 0x80) mustEncode = true;
			label += *it++;
		}
		if (mustEncode)
			encoded += encodeIDNLabel(label);
		else
			encoded += label;
		if (it != end) encoded += *it++;
	}
	return encoded;
}


std::string DNS::decodeIDN(const std::string& encodedIDN)
{
	std::string decoded;
	std::string::const_iterator it = encodedIDN.begin();
	std::string::const_iterator end = encodedIDN.end();
	while (it != end)
	{
		std::string label;
		while (it != end && *it != '.')
		{
			label += *it++;
		}
		decoded += decodeIDNLabel(label);
		if (it != end) decoded += *it++;
	}
	return decoded;
}


std::string DNS::encodeIDNLabel(const std::string& label)
{
	std::string encoded = "xn--";
	std::vector<Poco::UInt32> uniLabel;
	Poco::UTF8Encoding utf8;
	Poco::TextIterator it(label, utf8);
	Poco::TextIterator end(label);
	while (it != end)
	{
		int ch = *it;
		if (ch < 0) throw DNSException("Invalid UTF-8 character in IDN label", label);
		if (Poco::Unicode::isUpper(ch))
		{
			ch = Poco::Unicode::toLower(ch);
		}
		uniLabel.push_back(static_cast<Poco::UInt32>(ch));
		++it;
	}
	char buffer[64];
	std::size_t size = 64;
	int rc = punycode_encode(uniLabel.size(), &uniLabel[0], &size, buffer);
	if (rc == punycode_success)
		encoded.append(buffer, size);
	else
		throw DNSException("Failed to encode IDN label", label);
	return encoded;
}


std::string DNS::decodeIDNLabel(const std::string& encodedIDN)
{
	std::string decoded;
	if (encodedIDN.compare(0, 4, "xn--") == 0)
	{
		std::size_t size = 64;
		punycode_uint buffer[64];
		int rc = punycode_decode(encodedIDN.size() - 4, encodedIDN.data() + 4, &size, buffer);
		if (rc == punycode_success)
		{
			Poco::UTF32Encoding utf32;
			Poco::UTF8Encoding utf8;
			Poco::TextConverter converter(utf32, utf8);
			converter.convert(buffer, size*sizeof(punycode_uint), decoded);
		}
		else throw DNSException("Failed to decode IDN label: ", encodedIDN);
	}
	else
	{
		decoded = encodedIDN;
	}
	return decoded;
}


int DNS::lastError()
{
	return h_errno;
}


void DNS::error(int code, const std::string& arg)
{
	switch (code)
	{
	case POCO_ESYSNOTREADY:
		throw NetException("Net subsystem not ready");
	case POCO_ENOTINIT:
		throw NetException("Net subsystem not initialized");
	case POCO_HOST_NOT_FOUND:
		throw HostNotFoundException(arg);
	case POCO_TRY_AGAIN:
		throw DNSException("Temporary DNS error while resolving", arg);
	case POCO_NO_RECOVERY:
		throw DNSException("Non recoverable DNS error while resolving", arg);
	case POCO_NO_DATA:
		throw NoAddressFoundException(arg);
	default:
		throw IOException(NumberFormatter::format(code));
	}
}


void DNS::aierror(int code, const std::string& arg)
{
#if defined(POCO_HAVE_IPv6) || defined(POCO_HAVE_ADDRINFO)
	switch (code)
	{
	case EAI_AGAIN:
		throw DNSException("Temporary DNS error while resolving", arg);
	case EAI_FAIL:
		throw DNSException("Non recoverable DNS error while resolving", arg);
#if defined(EAI_NODATA) // deprecated in favor of EAI_NONAME on FreeBSD
	case EAI_NODATA:
		throw NoAddressFoundException(arg);
#endif
	case EAI_NONAME:
		throw HostNotFoundException(arg);
#if defined(EAI_SYSTEM)
	case EAI_SYSTEM:
		error(lastError(), arg);
		break;
#endif
	default:
		throw DNSException("EAI", gai_strerror(code));
	}
#endif // POCO_HAVE_IPv6 || defined(POCO_HAVE_ADDRINFO)
}


/*
  Code copied from http://www.nicemice.net/idn/punycode-spec.gz on
  2018-02-17 with SHA-1 a966a8017f6be579d74a50a226accc7607c40133
  labeled punycode-spec 1.0.3 (2006-Mar-23-Thu).

  Modified for POCO C++ Libraries by Guenter Obiltschnig.

  License on the original code:

  punycode-spec 1.0.3 (2006-Mar-23-Thu)
  http://www.nicemice.net/idn/
  Adam M. Costello
  http://www.nicemice.net/amc/

  B. Disclaimer and license

    Regarding this entire document or any portion of it (including
    the pseudocode and C code), the author makes no guarantees and
    is not responsible for any damage resulting from its use.  The
    author grants irrevocable permission to anyone to use, modify,
    and distribute it in any way that does not diminish the rights
    of anyone else to use, modify, and distribute it, provided that
    redistributed derivative works do not contain misleading author or
    version information.  Derivative works need not be licensed under
    similar terms.

  C. Punycode sample implementation

  punycode-sample.c 2.0.0 (2004-Mar-21-Sun)
  http://www.nicemice.net/idn/
  Adam M. Costello
  http://www.nicemice.net/amc/

  This is ANSI C code (C89) implementing Punycode 1.0.x.
*/


/*** Bootstring parameters for Punycode ***/

enum
{
	base = 36,
	tmin = 1,
	tmax = 26,
	skew = 38,
	damp = 700,
	initial_bias = 72,
	initial_n = 0x80,
	delimiter = 0x2D
};

/* basic(cp) tests whether cp is a basic code point: */
#define basic(cp) ((punycode_uint)(cp) < 0x80)

/* delim(cp) tests whether cp is a delimiter: */
#define delim(cp) ((cp) == delimiter)

/* encode_digit(d,flag) returns the basic code point whose value      */
/* (when used for representing integers) is d, which needs to be in   */
/* the range 0 to base-1.  The lowercase form is used unless flag is  */
/* nonzero, in which case the uppercase form is used.  The behavior   */
/* is undefined if flag is nonzero and digit d has no uppercase form. */

static char encode_digit(punycode_uint d, int flag)
{
	return static_cast<char>(d + 22 + 75 * (d < 26) - ((flag != 0) << 5));
	/*  0..25 map to ASCII a..z or A..Z */
	/* 26..35 map to ASCII 0..9         */
}

/* decode_digit(cp) returns the numeric value of a basic code */
/* point (for use in representing integers) in the range 0 to */
/* base-1, or base if cp does not represent a value.          */

static unsigned decode_digit(int cp)
{
	return (unsigned) (cp - 48 < 10 ? cp - 22 :  cp - 65 < 26 ? cp - 65 :
		cp - 97 < 26 ? cp - 97 :  base);
}

/*** Platform-specific constants ***/

/* maxint is the maximum value of a punycode_uint variable: */
static const punycode_uint maxint = -1;
/* Because maxint is unsigned, -1 becomes the maximum value. */

/*** Bias adaptation function ***/

static punycode_uint adapt(punycode_uint delta, punycode_uint numpoints, int firsttime);

static punycode_uint adapt(punycode_uint delta, punycode_uint numpoints, int firsttime)
{
	punycode_uint k;

	delta = firsttime ? delta / damp : delta >> 1;
	/* delta >> 1 is a faster way of doing delta / 2 */
	delta += delta / numpoints;

	for (k = 0;  delta > ((base - tmin) * tmax) / 2;  k += base)
	{
		delta /= base - tmin;
	}

	return k + (base - tmin + 1) * delta / (delta + skew);
}

/*** Main encode function ***/

int punycode_encode(size_t input_length_orig, const punycode_uint input[], size_t *output_length, char output[])
{
	punycode_uint input_length, n, delta, h, b, bias, j, m, q, k, t;
	size_t out, max_out;

	/* The Punycode spec assumes that the input length is the same type */
	/* of integer as a code point, so we need to convert the size_t to  */
	/* a punycode_uint, which could overflow.                           */

	if (input_length_orig > maxint) return punycode_overflow;
	input_length = (punycode_uint) input_length_orig;

	/* Initialize the state: */

	n = initial_n;
	delta = 0;
	out = 0;
	max_out = *output_length;
	bias = initial_bias;

	/* Handle the basic code points: */

	for (j = 0;  j < input_length;  ++j)
	{
		if (basic(input[j]))
		{
			if (max_out - out < 2) return punycode_big_output;
			output[out++] = (char) input[j];
		}
		/* else if (input[j] < n) return punycode_bad_input; */
		/* (not needed for Punycode with unsigned code points) */
	}

	h = b = (punycode_uint) out;
	/* cannot overflow because out <= input_length <= maxint */

	/* h is the number of code points that have been handled, b is the  */
	/* number of basic code points, and out is the number of ASCII code */
	/* points that have been output.                                    */

	if (b > 0) output[out++] = delimiter;

	/* Main encoding loop: */

	while (h < input_length)
	{
		/* All non-basic code points < n have been     */
		/* handled already.  Find the next larger one: */

		for (m = maxint, j = 0;  j < input_length;  ++j)
		{
			/* if (basic(input[j])) continue; */
			/* (not needed for Punycode) */
			if (input[j] >= n && input[j] < m) m = input[j];
		}

		/* Increase delta enough to advance the decoder's    */
		/* <n,i> state to <m,0>, but guard against overflow: */

		if (m - n > (maxint - delta) / (h + 1)) return punycode_overflow;
		delta += (m - n) * (h + 1);
		n = m;

		for (j = 0;  j < input_length;  ++j)
		{
			/* Punycode does not need to check whether input[j] is basic: */
			if (input[j] < n /* || basic(input[j]) */ )
			{
				if (++delta == 0) return punycode_overflow;
			}

			if (input[j] == n)
			{
				/* Represent delta as a generalized variable-length integer: */

				for (q = delta, k = base;  ;  k += base)
				{
					if (out >= max_out) return punycode_big_output;
					t = k <= bias /* + tmin */ ? tmin :     /* +tmin not needed */
						k >= bias + tmax ? tmax : k - bias;
					if (q < t) break;
					output[out++] = encode_digit(t + (q - t) % (base - t), 0);
					q = (q - t) / (base - t);
				}

				output[out++] = encode_digit(q, 0);
				bias = adapt(delta, h + 1, h == b);
				delta = 0;
				++h;
			}
		}

		++delta, ++n;
	}

	*output_length = out;
	return punycode_success;
}

/*** Main decode function ***/

int punycode_decode(size_t input_length, const char input[], size_t *output_length, punycode_uint output[])
{
	punycode_uint n, out, i, max_out, bias, oldi, w, k, digit, t;
	size_t b, j, in;

	/* Initialize the state: */

	n = initial_n;
	out = i = 0;
	max_out = *output_length > maxint ? maxint : (punycode_uint) *output_length;
	bias = initial_bias;

	/* Handle the basic code points:  Let b be the number of input code */
	/* points before the last delimiter, or 0 if there is none, then    */
	/* copy the first b code points to the output.                      */

	for (b = j = 0;  j < input_length;  ++j)
	{
		if (delim(input[j])) b = j;
	}
	if (b > max_out) return punycode_big_output;

	for (j = 0;  j < b;  ++j)
	{
		if (!basic(input[j])) return punycode_bad_input;
		output[out++] = input[j];
	}

	/* Main decoding loop:  Start just after the last delimiter if any  */
	/* basic code points were copied; start at the beginning otherwise. */

	for (in = b > 0 ? b + 1 : 0;  in < input_length;  ++out)
	{
		/* in is the index of the next ASCII code point to be consumed, */
		/* and out is the number of code points in the output array.    */

		/* Decode a generalized variable-length integer into delta,  */
		/* which gets added to i.  The overflow checking is easier   */
		/* if we increase i as we go, then subtract off its starting */
		/* value at the end to obtain delta.                         */

		for (oldi = i, w = 1, k = base;  ;  k += base)
		{
			if (in >= input_length) return punycode_bad_input;
			digit = decode_digit(input[in++]);
			if (digit >= base) return punycode_bad_input;
			if (digit > (maxint - i) / w) return punycode_overflow;
			i += digit * w;
			t = k <= bias /* + tmin */ ? tmin :     /* +tmin not needed */
				k >= bias + tmax ? tmax : k - bias;
			if (digit < t) break;
			if (w > maxint / (base - t)) return punycode_overflow;
			w *= (base - t);
		}

		bias = adapt(i - oldi, out + 1, oldi == 0);

		/* i was supposed to wrap around from out+1 to 0,   */
		/* incrementing n each time, so we'll fix that now: */

		if (i / (out + 1) > maxint - n) return punycode_overflow;
		n += i / (out + 1);
		i %= (out + 1);

		/* Insert n at position i of the output: */

		/* not needed for Punycode: */
		/* if (basic(n)) return punycode_bad_input; */
		if (out >= max_out) return punycode_big_output;

		std::memmove(output + i + 1, output + i, (out - i) * sizeof *output);
		output[i++] = n;
	}

	*output_length = (size_t) out;
	/* cannot overflow because out <= old value of *output_length */
	return punycode_success;
}


} } // namespace Poco::Net
