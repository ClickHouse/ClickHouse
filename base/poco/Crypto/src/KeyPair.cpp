//
// KeyPair.cpp
//
//
// Library: Crypto
// Package: CryptoCore
// Module:  KeyPair
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Crypto/KeyPair.h"
#include <openssl/rsa.h>


namespace Poco {
namespace Crypto {


KeyPair::KeyPair(KeyPairImpl::Ptr pKeyPairImpl): _pImpl(pKeyPairImpl)
{
}


KeyPair::~KeyPair()
{
}


} } // namespace Poco::Crypto
