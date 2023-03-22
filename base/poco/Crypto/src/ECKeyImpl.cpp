//
// ECKeyImpl.cpp
//
//
// Library: Crypto
// Package: EC
// Module:  ECKeyImpl
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Crypto/ECKeyImpl.h"
#include "Poco/Crypto/X509Certificate.h"
#include "Poco/Crypto/PKCS12Container.h"
#include "Poco/FileStream.h"
#include "Poco/Format.h"
#include "Poco/StreamCopier.h"
#include <sstream>
#include <openssl/evp.h>
#if OPENSSL_VERSION_NUMBER >= 0x00908000L
#include <openssl/bn.h>
#endif


namespace Poco {
namespace Crypto {


ECKeyImpl::ECKeyImpl(const EVPPKey& key):
	KeyPairImpl("ec", KT_EC_IMPL),
	_pEC(EVP_PKEY_get1_EC_KEY(const_cast<EVP_PKEY*>((const EVP_PKEY*)key)))
{
	checkEC("ECKeyImpl(const EVPPKey&)", "EVP_PKEY_get1_EC_KEY()");
}


ECKeyImpl::ECKeyImpl(const X509Certificate& cert):
	KeyPairImpl("ec", KT_EC_IMPL),
	_pEC(0)
{
	const X509* pCert = cert.certificate();
	if (pCert)
	{
		EVP_PKEY* pKey = X509_get_pubkey(const_cast<X509*>(pCert));
		if (pKey)
		{
			_pEC = EVP_PKEY_get1_EC_KEY(pKey);
			EVP_PKEY_free(pKey);
			checkEC("ECKeyImpl(const const X509Certificate&)", "EVP_PKEY_get1_EC_KEY()");
			return;
		}
	}
	throw OpenSSLException("ECKeyImpl(const X509Certificate&)");
}


ECKeyImpl::ECKeyImpl(const PKCS12Container& cont):
	KeyPairImpl("ec", KT_EC_IMPL),
	_pEC(EVP_PKEY_get1_EC_KEY(cont.getKey()))
{
	checkEC("ECKeyImpl(const PKCS12Container&)", "EVP_PKEY_get1_EC_KEY()");
}


ECKeyImpl::ECKeyImpl(int curve):
	KeyPairImpl("ec", KT_EC_IMPL),
	_pEC(EC_KEY_new_by_curve_name(curve))
{
	poco_check_ptr(_pEC);
	EC_KEY_set_asn1_flag(_pEC, OPENSSL_EC_NAMED_CURVE);
	if (!(EC_KEY_generate_key(_pEC)))
		throw OpenSSLException("ECKeyImpl(int curve): EC_KEY_generate_key()");
	checkEC("ECKeyImpl(int curve)", "EC_KEY_generate_key()");
}


ECKeyImpl::ECKeyImpl(const std::string& publicKeyFile, 
	const std::string& privateKeyFile, 
	const std::string& privateKeyPassphrase): KeyPairImpl("ec", KT_EC_IMPL), _pEC(0)
{
	if (EVPPKey::loadKey(&_pEC, PEM_read_PrivateKey, EVP_PKEY_get1_EC_KEY, privateKeyFile, privateKeyPassphrase))
	{
		checkEC(Poco::format("ECKeyImpl(%s, %s, %s)",
			publicKeyFile, privateKeyFile, privateKeyPassphrase.empty() ? privateKeyPassphrase : std::string("***")),
			"PEM_read_PrivateKey() or EVP_PKEY_get1_EC_KEY()");
		return; // private key is enough
	}

	// no private key, this must be public key only, otherwise throw
	if (!EVPPKey::loadKey(&_pEC, PEM_read_PUBKEY, EVP_PKEY_get1_EC_KEY, publicKeyFile))
	{
		throw OpenSSLException("ECKeyImpl(const string&, const string&, const string&");
	}
	checkEC(Poco::format("ECKeyImpl(%s, %s, %s)",
		publicKeyFile, privateKeyFile, privateKeyPassphrase.empty() ? privateKeyPassphrase : std::string("***")),
		"PEM_read_PUBKEY() or EVP_PKEY_get1_EC_KEY()");
}


ECKeyImpl::ECKeyImpl(std::istream* pPublicKeyStream,
	std::istream* pPrivateKeyStream,
	const std::string& privateKeyPassphrase): KeyPairImpl("ec", KT_EC_IMPL), _pEC(0)
{
	if (EVPPKey::loadKey(&_pEC, PEM_read_bio_PrivateKey, EVP_PKEY_get1_EC_KEY, pPrivateKeyStream, privateKeyPassphrase))
	{
		checkEC(Poco::format("ECKeyImpl(stream, stream, %s)",
			privateKeyPassphrase.empty() ? privateKeyPassphrase : std::string("***")),
			"PEM_read_bio_PrivateKey() or EVP_PKEY_get1_EC_KEY()");
		return; // private key is enough
	}

	// no private key, this must be public key only, otherwise throw
	if (!EVPPKey::loadKey(&_pEC, PEM_read_bio_PUBKEY, EVP_PKEY_get1_EC_KEY, pPublicKeyStream))
	{
		throw OpenSSLException("ECKeyImpl(istream*, istream*, const string&");
	}
	checkEC(Poco::format("ECKeyImpl(stream, stream, %s)",
		privateKeyPassphrase.empty() ? privateKeyPassphrase : std::string("***")),
		"PEM_read_bio_PUBKEY() or EVP_PKEY_get1_EC_KEY()");
}


ECKeyImpl::~ECKeyImpl()
{
	freeEC();
}


void ECKeyImpl::checkEC(const std::string& method, const std::string& func) const
{
	if (!_pEC) throw OpenSSLException(Poco::format("%s: %s", method, func));
	if (!EC_KEY_check_key(_pEC))
		throw OpenSSLException(Poco::format("%s: EC_KEY_check_key()", method));
}


void ECKeyImpl::freeEC()
{
	if (_pEC)
	{
		EC_KEY_free(_pEC);
		_pEC = 0;
	}
}


int ECKeyImpl::size() const
{
	int sz = -1;
	EVP_PKEY* pKey = EVP_PKEY_new();
	if (pKey && EVP_PKEY_set1_EC_KEY(pKey, _pEC))
	{
		sz = EVP_PKEY_bits(pKey);
		EVP_PKEY_free(pKey);
		return sz;
	}
	throw OpenSSLException("ECKeyImpl::size()");
}


int ECKeyImpl::groupId() const
{
	if (_pEC)
	{
		const EC_GROUP* ecGroup = EC_KEY_get0_group(_pEC);
		if (ecGroup)
		{
			return EC_GROUP_get_curve_name(ecGroup);
		}
		else
		{
			throw OpenSSLException("ECKeyImpl::groupName()");
		}
	}
	throw NullPointerException("ECKeyImpl::groupName() => _pEC");
}


std::string ECKeyImpl::getCurveName(int nid)
{
	std::string curveName;
	size_t len = EC_get_builtin_curves(NULL, 0);
	EC_builtin_curve* pCurves =
			(EC_builtin_curve*) OPENSSL_malloc(sizeof(EC_builtin_curve) * len);
	if (!pCurves) return curveName;

	if (!EC_get_builtin_curves(pCurves, len))
	{
		OPENSSL_free(pCurves);
		return curveName;
	}

	if (-1 == nid) nid = pCurves[0].nid;
	const int bufLen = 128;
	char buf[bufLen];
	std::memset(buf, 0, bufLen);
	OBJ_obj2txt(buf, bufLen, OBJ_nid2obj(nid), 0);
	curveName = buf;
	OPENSSL_free(pCurves);
	return curveName;
}


int ECKeyImpl::getCurveNID(std::string& name)
{
	std::string curveName;
	size_t len = EC_get_builtin_curves(NULL, 0);
	EC_builtin_curve* pCurves =
		(EC_builtin_curve*)OPENSSL_malloc(static_cast<int>(sizeof(EC_builtin_curve) * len));
	if (!pCurves) return -1;

	if (!EC_get_builtin_curves(pCurves, len))
	{
		OPENSSL_free(pCurves);
		return -1;
	}

	int nid = -1;
	const int bufLen = 128;
	char buf[bufLen];
	if (name.empty())
	{
		std::memset(buf, 0, bufLen);
		OBJ_obj2txt(buf, bufLen, OBJ_nid2obj(nid), 0);
		name = buf;
		nid = pCurves[0].nid;
	}
	else
	{
		for (int i = 0; i < len; ++i)
		{
			std::memset(buf, 0, bufLen);
			OBJ_obj2txt(buf, bufLen, OBJ_nid2obj(pCurves[i].nid), 0);
			if (strncmp(name.c_str(), buf, name.size() > bufLen ? bufLen : name.size()) == 0)
			{
				nid = pCurves[i].nid;
				break;
			}
		}
	}

	OPENSSL_free(pCurves);
	return nid;
}


bool ECKeyImpl::hasCurve(const std::string& name)
{
	std::string tmp(name);
	return (-1 != getCurveNID(tmp));
}


} } // namespace Poco::Crypto
