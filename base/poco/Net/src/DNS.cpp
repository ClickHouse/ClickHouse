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
#include "Poco/RWLock.h"
#include "Poco/TextIterator.h"
#include "Poco/TextConverter.h"
#include "Poco/UTF8Encoding.h"
#include "Poco/UTF32Encoding.h"
#include "Poco/Unicode.h"
#include <cstring>


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


HostEntry DNS::hostByName(const std::string& hostname, unsigned
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
	int rc = getaddrinfo(hostname.c_str(), NULL, &hints, &pAI);
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


HostEntry DNS::hostByAddress(const IPAddress& address, unsigned
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
		rc = getaddrinfo(fqname, NULL, &hints, &pAI);
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
	return d + 22 + 75 * (d < 26) - ((flag != 0) << 5);
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
