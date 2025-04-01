//
// X509Certificate.cpp
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


#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <openssl/err.h>

#include "Poco/Net/X509Certificate.h"
#include "Poco/String.h"
#include "Poco/RegularExpression.h"
#include "Poco/Net/IPAddress.h"


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


bool X509Certificate::verify(const std::string& hostName) const
{
	return verify(*this, hostName);
}


bool X509Certificate::verify(const Poco::Crypto::X509Certificate& certificate, const std::string& hostName)
{
	if (X509_check_host(const_cast<X509*>(certificate.certificate()), hostName.c_str(), hostName.length(), 0, nullptr) == 1)
	{
		return true;
	}
	else
	{
		IPAddress ip;
		if (IPAddress::tryParse(hostName, ip))
		{
		    return (X509_check_ip_asc(const_cast<X509*>(certificate.certificate()), hostName.c_str(), 0) == 1);
		}
	}
	return false;
#endif
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
