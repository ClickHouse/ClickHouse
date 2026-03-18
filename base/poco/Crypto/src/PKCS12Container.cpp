//
// PKCS12Container.cpp
//
//
// Library: Crypto
// Package: Certificate
// Module:  PKCS12Container
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Crypto/PKCS12Container.h"
#include "Poco/NumberFormatter.h"
#include "Poco/StreamCopier.h"
#include <sstream>
#include <openssl/err.h>


namespace Poco {
namespace Crypto {


PKCS12Container::PKCS12Container(std::istream& istr, const std::string& password): _pKey(0)
{
	std::ostringstream ostr;
	Poco::StreamCopier::copyStream(istr, ostr);
	const std::string& cont = ostr.str();

	BIO *pBIO = BIO_new_mem_buf(const_cast<char*>(cont.data()), static_cast<int>(cont.size()));
	if (pBIO)
	{
		PKCS12* pPKCS12 = 0;
		d2i_PKCS12_bio(pBIO, &pPKCS12);
		BIO_free(pBIO);
		if (!pPKCS12) throw OpenSSLException("PKCS12Container(istream&, const string&)");
		load(pPKCS12, password);
	}
	else
	{
		throw Poco::NullPointerException("PKCS12Container(istream&, const string&)");
	}
}


PKCS12Container::PKCS12Container(const std::string& path, const std::string& password): _pKey(0)
{
	FILE* pFile = fopen(path.c_str(), "rb");
	if (pFile)
	{
		PKCS12* pPKCS12 = d2i_PKCS12_fp(pFile, NULL);
		fclose (pFile);
		if (!pPKCS12) throw OpenSSLException("PKCS12Container(const string&, const string&)");
		load(pPKCS12, password);
	}
	else
	{
		throw Poco::OpenFileException("PKCS12Container: " + path);
	}
}


PKCS12Container::PKCS12Container(const PKCS12Container& other):
	_pKey(EVPPKey::duplicate(other._pKey, &_pKey)),
	_pX509Cert(new X509Certificate(*other._pX509Cert)),
	_caCertList(other._caCertList),
	_caCertNames(other._caCertNames),
	_pkcsFriendlyName(other._pkcsFriendlyName)
{
}


PKCS12Container& PKCS12Container::operator = (const PKCS12Container& other)
{
	if (&other != this)
	{
		if (_pKey) EVP_PKEY_free(_pKey);
		_pKey = EVPPKey::duplicate(other._pKey, &_pKey);
		_pX509Cert.reset(new X509Certificate(*other._pX509Cert));
		_caCertList = other._caCertList;
		_caCertNames = other._caCertNames;
		_pkcsFriendlyName = other._pkcsFriendlyName;
	}
	return *this;
}


#ifdef POCO_ENABLE_CPP11


PKCS12Container::PKCS12Container(PKCS12Container&& other):
	_pKey(other._pKey),
	_pX509Cert(std::move(other._pX509Cert)),
	_caCertList(std::move(other._caCertList)),
	_caCertNames(std::move(other._caCertNames)),
	_pkcsFriendlyName(std::move(other._pkcsFriendlyName))
{
	other._pKey = 0;
}


PKCS12Container& PKCS12Container::operator = (PKCS12Container&& other)
{
	if (&other != this)
	{
		if (_pKey) EVP_PKEY_free(_pKey);
		_pKey = other._pKey; other._pKey = 0;
		_pX509Cert = std::move(other._pX509Cert);
		_caCertList = std::move(other._caCertList);
		_caCertNames = std::move(other._caCertNames);
		_pkcsFriendlyName = std::move(other._pkcsFriendlyName);
	}
	return *this;
}


#endif // POCO_ENABLE_CPP11


PKCS12Container::~PKCS12Container()
{
	if (_pKey) EVP_PKEY_free(_pKey);
}


std::string PKCS12Container::extractFriendlyName(X509* pCert)
{
	std::string friendlyName;
	if(!pCert) throw NullPointerException("PKCS12Container::extractFriendlyName(X509)");

	// This is how PKCS12_add_cert() sets friendlyName (via PKCS12_add_friendlyname())
	int namelen = 0;
	char *name = (char *)X509_alias_get0(pCert, &namelen);

	friendlyName = std::string(name, namelen);
	return friendlyName;
}


void PKCS12Container::load(PKCS12* pPKCS12, const std::string& password)
{
	if (pPKCS12)
	{
		X509* pCert = 0;
		STACK_OF(X509)* pCA = 0;
		if (PKCS12_parse(pPKCS12, password.c_str(), &_pKey, &pCert, &pCA))
		{
			if (pCert)
			{
				_pX509Cert.reset(new X509Certificate(pCert, true));
				_pkcsFriendlyName = extractFriendlyName(pCert);
			}
			else _pX509Cert.reset();

			_caCertList.clear();
			_caCertNames.clear();
			if (pCA)
			{
				int certCount = sk_X509_num(pCA);
				for (int i = 0; i < certCount; ++i)
				{
					X509* pX509 = sk_X509_value(pCA, i);
					if (pX509)
					{
						_caCertList.push_back(X509Certificate(pX509, true));
						_caCertNames.push_back(extractFriendlyName(pX509));
					}
					else throw OpenSSLException("PKCS12Container::load()");
				}
			}
		}
		else
		{
			throw OpenSSLException();
		}
		PKCS12_free(pPKCS12);
		sk_X509_pop_free(pCA, X509_free);
		if (pCert) X509_free(pCert);
		poco_assert_dbg (_caCertList.size() == _caCertNames.size());
	}
	else
	{
		throw NullPointerException("PKCS12Container: struct PKCS12");
	}
}


} } // namespace Poco::Crypto
