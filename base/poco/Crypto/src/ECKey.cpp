//
// ECKey.cpp
//
//
// Library: Crypto
// Package: EC
// Module:  ECKey
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Crypto/ECKey.h"
#include <openssl/rsa.h>


namespace Poco {
namespace Crypto {


ECKey::ECKey(const EVPPKey& key):
	KeyPair(new ECKeyImpl(key)),
	_pImpl(KeyPair::impl().cast<ECKeyImpl>())
{
}


ECKey::ECKey(const X509Certificate& cert):
	KeyPair(new ECKeyImpl(cert)),
	_pImpl(KeyPair::impl().cast<ECKeyImpl>())
{
}


ECKey::ECKey(const PKCS12Container& cont):
	KeyPair(new ECKeyImpl(cont)),
	_pImpl(KeyPair::impl().cast<ECKeyImpl>())
{
}


ECKey::ECKey(const std::string& eccGroup):
	KeyPair(new ECKeyImpl(OBJ_txt2nid(eccGroup.c_str()))),
	_pImpl(KeyPair::impl().cast<ECKeyImpl>())
{
}


ECKey::ECKey(const std::string& publicKeyFile,
	const std::string& privateKeyFile,
	const std::string& privateKeyPassphrase):
		KeyPair(new ECKeyImpl(publicKeyFile, privateKeyFile, privateKeyPassphrase)),
		_pImpl(KeyPair::impl().cast<ECKeyImpl>())
{
}


ECKey::ECKey(std::istream* pPublicKeyStream,
	std::istream* pPrivateKeyStream,
	const std::string& privateKeyPassphrase):
		KeyPair(new ECKeyImpl(pPublicKeyStream, pPrivateKeyStream, privateKeyPassphrase)),
		_pImpl(KeyPair::impl().cast<ECKeyImpl>())
{
}


ECKey::~ECKey()
{
}


} } // namespace Poco::Crypto
