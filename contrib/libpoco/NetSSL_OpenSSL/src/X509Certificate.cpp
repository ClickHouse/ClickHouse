//
// X509Certificate.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/src/X509Certificate.cpp#4 $
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  X509Certificate
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/X509Certificate.h"
#include "Poco/Net/SSLException.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/Net/DNS.h"
#include "Poco/TemporaryFile.h"
#include "Poco/FileStream.h"
#include "Poco/StreamCopier.h"
#include "Poco/String.h"
#include "Poco/RegularExpression.h"
#include "Poco/DateTimeParser.h"
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <openssl/err.h>


namespace Poco {
namespace Net {


X509Certificate::X509Certificate(std::istream& istr):
	Poco::Crypto::X509Certificate(istr)
{	
}


X509Certificate::X509Certificate(const std::string& path):
	Poco::Crypto::X509Certificate(path)
{
}


X509Certificate::X509Certificate(X509* pCert):
	Poco::Crypto::X509Certificate(pCert)
{
}


X509Certificate::X509Certificate(X509* pCert, bool shared):
	Poco::Crypto::X509Certificate(pCert, shared)
{
}


X509Certificate::X509Certificate(const Poco::Crypto::X509Certificate& cert):
	Poco::Crypto::X509Certificate(cert)
{
}


X509Certificate& X509Certificate::operator = (const Poco::Crypto::X509Certificate& cert)
{
	X509Certificate tmp(cert);
	swap(tmp);
	return *this;
}


X509Certificate::~X509Certificate()
{
}


bool X509Certificate::verify(const std::string& hostName) const
{
	return verify(*this, hostName);
}


bool X509Certificate::verify(const Poco::Crypto::X509Certificate& certificate, const std::string& hostName)
{		
	std::string commonName;
	std::set<std::string> dnsNames;
	certificate.extractNames(commonName, dnsNames);
	if (!commonName.empty()) dnsNames.insert(commonName);
	bool ok = (dnsNames.find(hostName) != dnsNames.end());
	if (!ok)
	{
		for (std::set<std::string>::const_iterator it = dnsNames.begin(); !ok && it != dnsNames.end(); ++it)
		{
			try
			{
				// two cases: name contains wildcards or not
				if (containsWildcards(*it))
				{
					// a compare by IPAddress is not possible with wildcards
					// only allow compare by name
					ok = matchWildcard(*it, hostName);
				}
				else
				{
					// it depends on hostName whether we compare by IP or by alias
					IPAddress ip;
					if (IPAddress::tryParse(hostName, ip))
					{
						// compare by IP
						const HostEntry& heData = DNS::resolve(*it);
						const HostEntry::AddressList& addr = heData.addresses();
						HostEntry::AddressList::const_iterator it = addr.begin();
						HostEntry::AddressList::const_iterator itEnd = addr.end();
						for (; it != itEnd && !ok; ++it)
						{
							ok = (*it == ip);
						}
					}
					else
					{
						ok = Poco::icompare(*it, hostName) == 0;
					}
				}
			}
			catch (NoAddressFoundException&)
			{
			}
			catch (HostNotFoundException&)
			{
			}
		}
	}
	return ok;
}


bool X509Certificate::containsWildcards(const std::string& commonName)
{
	return (commonName.find('*') != std::string::npos || commonName.find('?') != std::string::npos);
}


bool X509Certificate::matchWildcard(const std::string& wildcard, const std::string& hostName)
{
	// fix wildcards
	std::string wildcardExpr("^");
	wildcardExpr += Poco::replace(wildcard, ".", "\\.");
	Poco::replaceInPlace(wildcardExpr, "*", ".*");
	Poco::replaceInPlace(wildcardExpr, "..*", ".*");
	Poco::replaceInPlace(wildcardExpr, "?", ".?");
	Poco::replaceInPlace(wildcardExpr, "..?", ".?");
	wildcardExpr += "$";

	Poco::RegularExpression expr(wildcardExpr, Poco::RegularExpression::RE_CASELESS);
	return expr.match(hostName);
}


} } // namespace Poco::Net
