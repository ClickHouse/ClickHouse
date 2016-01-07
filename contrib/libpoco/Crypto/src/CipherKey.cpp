//
// CipherKey.cpp
//
// $Id: //poco/1.4/Crypto/src/CipherKey.cpp#1 $
//
// Library: Crypto
// Package: Cipher
// Module:  CipherKey
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Crypto/CipherKey.h"


namespace Poco {
namespace Crypto {


CipherKey::CipherKey(const std::string& name, const std::string& passphrase,  const std::string& salt, int iterationCount):
	_pImpl(new CipherKeyImpl(name, passphrase, salt, iterationCount))
{
}


CipherKey::CipherKey(const std::string& name, const ByteVec& key, const ByteVec& iv):
	_pImpl(new CipherKeyImpl(name, key, iv))
{
}


CipherKey::CipherKey(const std::string& name):
	_pImpl(new CipherKeyImpl(name))
{
}


CipherKey::~CipherKey()
{
}


} } // namespace Poco::Crypto
