//
// SHA1Engine.h
//
// Library: Foundation
// Package: Crypt
// Module:  SHA1Engine
//
// Definition of class SHA1Engine.
//
// Secure Hash Standard SHA-1 algorithm
// (FIPS 180-1, see http://www.itl.nist.gov/fipspubs/fip180-1.htm)
//
// Based on the public domain implementation by Peter C. Gutmann
// on 2 Sep 1992, modified by Carl Ellison to be SHA-1.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_SHA1Engine_INCLUDED
#define Foundation_SHA1Engine_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/DigestEngine.h"


namespace Poco {


class Foundation_API SHA1Engine: public DigestEngine
	/// This class implementes the SHA-1 message digest algorithm.
	/// (FIPS 180-1, see http://www.itl.nist.gov/fipspubs/fip180-1.htm)
{
public:
	enum
	{
		BLOCK_SIZE  = 64,
		DIGEST_SIZE = 20
	};

	SHA1Engine();
	~SHA1Engine();
		
	std::size_t digestLength() const;
	void reset();
	const DigestEngine::Digest& digest();

protected:
	void updateImpl(const void* data, std::size_t length);

private:
	void transform();
	static void byteReverse(UInt32* buffer, int byteCount);

	typedef UInt8 BYTE;

	struct Context
	{
		UInt32 digest[5]; // Message digest
		UInt32 countLo;   // 64-bit bit count
		UInt32 countHi;
		UInt32 data[16];  // SHA data buffer
		UInt32 slop;      // # of bytes saved in data[]
	};

	Context _context;
	DigestEngine::Digest _digest;

	SHA1Engine(const SHA1Engine&);
	SHA1Engine& operator = (const SHA1Engine&);
};


} // namespace Poco


#endif // Foundation_SHA1Engine_INCLUDED
