//
// DigestEngine.cpp
//
// Library: Foundation
// Package: Crypt
// Module:  DigestEngine
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DigestEngine.h"
#include "Poco/Exception.h"


namespace Poco
{

DigestEngine::DigestEngine()
{
}


DigestEngine::~DigestEngine()
{
}


std::string DigestEngine::digestToHex(const Digest& bytes)
{
	static const char digits[] = "0123456789abcdef";
	std::string result;
	result.reserve(bytes.size() * 2);
	for (Digest::const_iterator it = bytes.begin(); it != bytes.end(); ++it)
	{
		unsigned char c = *it;
		result += digits[(c >> 4) & 0xF];
		result += digits[c & 0xF];
	}
	return result;
}


DigestEngine::Digest DigestEngine::digestFromHex(const std::string& digest)
{
	if (digest.size() % 2 != 0)
		throw DataFormatException();
	Digest result;
	result.reserve(digest.size() / 2);
	for (std::size_t i = 0; i < digest.size(); ++i)
	{
		int c = 0;
		// first upper 4 bits
		if (digest[i] >= '0' && digest[i] <= '9')
			c = digest[i] - '0';
		else if (digest[i] >= 'a' && digest[i] <= 'f')
			c = digest[i] - 'a' + 10;
		else if (digest[i] >= 'A' && digest[i] <= 'F')
			c = digest[i] - 'A' + 10;
		else
			throw DataFormatException();
		c <<= 4;
		++i;
		if (digest[i] >= '0' && digest[i] <= '9')
			c += digest[i] - '0';
		else if (digest[i] >= 'a' && digest[i] <= 'f')
			c += digest[i] - 'a' + 10;
		else if (digest[i] >= 'A' && digest[i] <= 'F')
			c += digest[i] - 'A' + 10;
		else
			throw DataFormatException();

		result.push_back(static_cast<unsigned char>(c));
	}
	return result;
}


bool DigestEngine::constantTimeEquals(const Digest& d1, const Digest& d2)
{
	if (d1.size() != d2.size()) return false;

	int result = 0;
	Digest::const_iterator it1 = d1.begin();
	Digest::const_iterator it2 = d2.begin();
	Digest::const_iterator end1 = d1.end();
	while (it1 != end1)
	{
		result |= *it1++ ^ *it2++;
	}
	return result == 0;
}


} // namespace Poco

