//
// DigestEngine.cpp
//
// $Id: //poco/1.4/Crypto/src/DigestEngine.cpp#1 $
//
// Library: Crypto
// Package: Digest
// Module:  DigestEngine
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Crypto/DigestEngine.h"
#include "Poco/Exception.h"


namespace Poco {
namespace Crypto {


DigestEngine::DigestEngine(const std::string& name):
	_name(name)
{
	const EVP_MD* md = EVP_get_digestbyname(_name.c_str());
	if (!md) throw Poco::NotFoundException(_name);
	_ctx = EVP_MD_CTX_create();
	EVP_DigestInit_ex(_ctx, md, NULL);	
}

	
DigestEngine::~DigestEngine()
{
	EVP_MD_CTX_destroy(_ctx);
}

int DigestEngine::nid() const
{
	return EVP_MD_nid(_ctx->digest);
}

std::size_t DigestEngine::digestLength() const
{
	return EVP_MD_CTX_size(_ctx);
}


void DigestEngine::reset()
{
	EVP_MD_CTX_cleanup(_ctx);
	const EVP_MD* md = EVP_get_digestbyname(_name.c_str());
	if (!md) throw Poco::NotFoundException(_name);
	EVP_DigestInit_ex(_ctx, md, NULL);
}


const Poco::DigestEngine::Digest& DigestEngine::digest()
{
	_digest.clear();
	unsigned len = EVP_MD_CTX_size(_ctx);
	_digest.resize(len);
	EVP_DigestFinal_ex(_ctx, &_digest[0], &len);
	reset();
	return _digest;
}


void DigestEngine::updateImpl(const void* data, std::size_t length)
{
	EVP_DigestUpdate(_ctx, data, length);
}


} } // namespace Poco::Crypto
