//
// DNS.cpp
//
// $Id: //poco/1.4/Net/src/DNS.cpp#10 $
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
#include "Poco/RWLock.h"
#include "Poco/Timespan.h"
#include "Poco/Mutex.h"
#include <cstring>
#include <list>


#if defined(POCO_HAVE_LIBRESOLV)
#include <resolv.h>
#endif


/// set default DNS timeout to 60 seconds
const Poco::Timespan Poco::Net::DNS::DEFAULT_DNS_TIMEOUT = Poco::Timespan(60, 0);

/** getaddrinfo иногда работает бесконечно долго.
 *  Этот код использует getaddrinfo_a c некоторым таймаутом.
 *
 *  При выполнении в один поток производительность ниже на 30%
 *  При выполнении запросов в 4 потока производительность отличается
 *  иногда в лучшую иногда в худшую сторону на ~10-20%
 */
class GetAddrinfo
{
public:
	static GetAddrinfo & instance()
	{
		static GetAddrinfo impl;
		return impl;
	}

	int getaddrinfo(const char * name,
					 const char * service,
					 const struct addrinfo * hints,
					 struct addrinfo ** pai,
					 const Poco::Timespan * timeout_);

	size_t requestsNum()
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		return requests.size();
	}

private:
	GetAddrinfo() {}

	GetAddrinfo(const GetAddrinfo &) = delete;
	const GetAddrinfo & operator=(const GetAddrinfo &) = delete;

	void releaseUnused()
	{
		for (auto it = requests.rbegin(); it != requests.rend();)
		{
			/// don't delete if structure is used by other thread or by internal cycle of getaddrinfo
			if (it->unused && gai_error(&(*it)) != EAI_INPROGRESS)
			{
				free(const_cast<char *>(it->ar_name));
				it->ar_name = nullptr;
				free(const_cast<char *>(it->ar_service));
				it->ar_service = nullptr;
				free(const_cast<addrinfo *>(it->ar_request));
				it->ar_request = nullptr;
				freeaddrinfo(it->ar_result);
				it->ar_result = nullptr;

				auto it_to_delete = --(it.base());

				it = decltype(it)(requests.erase(it_to_delete));
			}
			else
				break;
		}
	}

	void addOne(const char * name,
				const char * service,
				const struct addrinfo * hints)
	{
		requests.emplace_back();

		auto & request = requests.back();

		request.ar_name = name ? strdup(name) : nullptr;
		request.ar_service = service ? strdup(service) : nullptr;

		addrinfo * my_hints = nullptr;
		if (hints)
		{
			/// only ai_flags are used in Poco
			my_hints = (addrinfo *)calloc(1, sizeof(addrinfo));
			my_hints->ai_flags = hints->ai_flags;
		}
		request.ar_request = my_hints;

		request.ar_result = nullptr;
	}

private:
	struct gaicb_ext : public gaicb
	{
		gaicb_ext()
		{
			memset(this, 0, sizeof(gaicb_ext));
			unused = false;
		}

		~gaicb_ext()
		{
			if (gai_error(this) != EAI_INPROGRESS)
			{
				free(const_cast<char *>(ar_name));
				free(const_cast<char *>(ar_service));
				free(const_cast<addrinfo *>(ar_request));

				freeaddrinfo(ar_result);
			}
		}

		bool unused;
	};

	std::list<gaicb_ext> requests;

	Poco::FastMutex mutex;
};

int GetAddrinfo::getaddrinfo(const char * name,
		 const char * service,
		 const struct addrinfo * hints,
		 struct addrinfo ** pai,
		 const Poco::Timespan * timeout_)
{
	if (timeout_)
	{
		timespec timeout;
		timeout.tv_sec = timeout_->totalSeconds();
		timeout.tv_nsec = timeout_->microseconds() * 1000;

		gaicb_ext * request_ext_ptr = nullptr;
		{
			Poco::ScopedLock<Poco::FastMutex> lock(mutex);
			addOne(name, service, hints);
			request_ext_ptr = &requests.back();
		}
		gaicb * request_ptr = request_ext_ptr;

		int code = getaddrinfo_a(GAI_NOWAIT, &request_ptr, 1, nullptr);

		if (!code)
		{
			gai_suspend(&request_ptr, 1, &timeout);

			*pai = request_ext_ptr->ar_result;
			/// prevent deleting result in dctor
			request_ext_ptr->ar_result = nullptr;

			code = gai_error(request_ext_ptr);
		}

		request_ext_ptr->unused = true;

		{
			Poco::ScopedLock<Poco::FastMutex> lock(mutex);
			releaseUnused();
		}

		return code;
	}
	else
	{
		return ::getaddrinfo(name, service, hints, pai);
	}
}


using Poco::Environment;
using Poco::NumberFormatter;
using Poco::IOException;


namespace Poco {
namespace Net {


#if defined(POCO_HAVE_LIBRESOLV)
static Poco::RWLock resolverLock;
#endif


HostEntry DNS::hostByName(const std::string& hostname, const Poco::Timespan * timeout_, unsigned
#ifdef POCO_HAVE_ADDRINFO
						  hintFlags
#endif
						 )
{
#if defined(POCO_HAVE_LIBRESOLV)
	Poco::ScopedReadRWLock readLock(resolverLock);
#endif

#if defined(POCO_HAVE_ADDRINFO)
	struct addrinfo* pAI;
	struct addrinfo hints;
	std::memset(&hints, 0, sizeof(hints));
	hints.ai_flags = hintFlags;
	int rc = GetAddrinfo::instance().getaddrinfo(hostname.c_str(), NULL, &hints, &pAI, timeout_);
	if (rc == 0)
	{
		HostEntry result(pAI);
		freeaddrinfo(pAI);
		return result;
	}
	else
	{
		aierror(rc, hostname);
	}
#elif defined(POCO_VXWORKS)
	int addr = hostGetByName(const_cast<char*>(hostname.c_str()));
	if (addr != ERROR)
	{
		return HostEntry(hostname, IPAddress(&addr, sizeof(addr)));
	}
#else
	struct hostent* he = gethostbyname(hostname.c_str());
	if (he)
	{
		return HostEntry(he);
	}
#endif
	error(lastError(), hostname); // will throw an appropriate exception
	throw NetException(); // to silence compiler
}


HostEntry DNS::hostByAddress(const IPAddress& address, const Poco::Timespan * timeout_, unsigned
#ifdef POCO_HAVE_ADDRINFO
							 hintFlags
#endif
							)
{
#if defined(POCO_HAVE_LIBRESOLV)
	Poco::ScopedReadRWLock readLock(resolverLock);
#endif

#if defined(POCO_HAVE_ADDRINFO)
	SocketAddress sa(address, 0);
	static char fqname[1024];
	int rc = getnameinfo(sa.addr(), sa.length(), fqname, sizeof(fqname), NULL, 0, NI_NAMEREQD);
	if (rc == 0)
	{
		struct addrinfo* pAI;
		struct addrinfo hints;
		std::memset(&hints, 0, sizeof(hints));
		hints.ai_flags = hintFlags;
		int rc = GetAddrinfo::instance().getaddrinfo(fqname, NULL, &hints, &pAI, timeout_);
		if (rc == 0)
		{
			HostEntry result(pAI);
			freeaddrinfo(pAI);
			return result;
		}
		else
		{
			aierror(rc, address.toString());
		}
	}
	else
	{
		aierror(rc, address.toString());
	}
#elif defined(POCO_VXWORKS)
	char name[MAXHOSTNAMELEN + 1];
	if (hostGetByAddr(*reinterpret_cast<const int*>(address.addr()), name) == OK)
	{
		return HostEntry(std::string(name), address);
	}
#else
	struct hostent* he = gethostbyaddr(reinterpret_cast<const char*>(address.addr()), address.length(), address.af());
	if (he)
	{
		return HostEntry(he);
	}
#endif
	int err = lastError();
	error(err, address.toString()); // will throw an appropriate exception
	throw NetException(); // to silence compiler
}


HostEntry DNS::resolve(const std::string& address)
{
	IPAddress ip;
	if (IPAddress::tryParse(address, ip))
		return hostByAddress(ip);
	else
		return hostByName(address);
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


void DNS::flushCache()
{
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


int DNS::lastError()
{
#if defined(_WIN32)
	return GetLastError();
#elif defined(POCO_VXWORKS)
	return errno;
#else
	return h_errno;
#endif
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
#if !defined(_WIN32) // EAI_NODATA and EAI_NONAME have the same value
#if defined(EAI_NODATA) // deprecated in favor of EAI_NONAME on FreeBSD
	case EAI_NODATA:
		throw NoAddressFoundException(arg);
#endif
#endif
	case EAI_NONAME:
		throw HostNotFoundException(arg);
#if defined(EAI_SYSTEM)
	case EAI_SYSTEM:
		error(lastError(), arg);
		break;
#endif
#if defined(_WIN32)
	case WSANO_DATA: // may happen on XP
		throw HostNotFoundException(arg);
#endif
	default:
		throw DNSException("EAI", NumberFormatter::format(code));
	}
#endif // POCO_HAVE_IPv6 || defined(POCO_HAVE_ADDRINFO)
}


} } // namespace Poco::Net
