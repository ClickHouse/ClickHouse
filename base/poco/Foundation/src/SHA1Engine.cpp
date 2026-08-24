//
// SHA1Engine.cpp
//
// Library: Foundation
// Package: Crypt
// Module:  SHA1Engine
//
// Based on the public domain implementation by Peter C. Gutmann
// on 2 Sep 1992, modified by Carl Ellison to be SHA-1.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SHA1Engine.h"
#include <cstring>


#ifdef POCO_ARCH_LITTLE_ENDIAN
	#define SHA1_BYTE_REVERSE(x, y) byteReverse(x, y)
#else
	#define SHA1_BYTE_REVERSE(x, y)
#endif


namespace Poco {


SHA1Engine::SHA1Engine()
{
	_digest.reserve(16);
	reset();
}


SHA1Engine::~SHA1Engine()
{
	reset();
}


inline void SHA1Engine::byteReverse(UInt32* buffer, int byteCount)
{
#ifdef POCO_ARCH_LITTLE_ENDIAN
	byteCount /= sizeof(UInt32);
	for(int count = 0; count < byteCount; count++)
	{
		UInt32 value = (buffer[ count ] << 16) | (buffer[ count ] >> 16);
		buffer[count] = ((value & 0xFF00FF00L) >> 8) | ((value & 0x00FF00FFL) << 8);
	}
#endif // POCO_ARCH_LITTLE_ENDIAN
}

	
void SHA1Engine::updateImpl(const void* buffer_, std::size_t count)
{
	const BYTE* buffer = (const BYTE*) buffer_;
	BYTE* db = (BYTE*) &_context.data[0];

	/* Update bitcount */
	if ((_context.countLo + ((UInt32) count << 3)) < _context.countLo)
		_context.countHi++; /* Carry from low to high bitCount */
	_context.countLo += ((UInt32) count << 3);
	_context.countHi += ((UInt32 ) count >> 29);

	/* Process data in BLOCK_SIZE chunks */
	while (count-- > 0)
	{
		db[_context.slop++] = *(buffer++);
		if (_context.slop == BLOCK_SIZE)
		{ 
			/* transform this one block */
			SHA1_BYTE_REVERSE(_context.data, BLOCK_SIZE);
			transform();
			_context.slop = 0 ;	/* no slop left */
		}
	}
}


std::size_t SHA1Engine::digestLength() const
{
	return DIGEST_SIZE;
}


void SHA1Engine::reset()
{
	_context.digest[0] = 0x67452301L;
	_context.digest[1] = 0xEFCDAB89L;
	_context.digest[2] = 0x98BADCFEL;
	_context.digest[3] = 0x10325476L;
	_context.digest[4] = 0xC3D2E1F0L;
	_context.countLo   = 0;
	_context.countHi   = 0;
	_context.slop      = 0;
	std::memset(_context.data, 0, sizeof(_context.data));
}


const DigestEngine::Digest& SHA1Engine::digest()
{
	int count;
	UInt32 lowBitcount  = _context.countLo;
	UInt32 highBitcount = _context.countHi;

	/* Compute number of bytes mod 64 */
	count = (int) ((_context.countLo >> 3) & 0x3F);

	/* Set the first char of padding to 0x80.  This is safe since there is
		always at least one byte free */
	((BYTE*) _context.data)[count++] = 0x80;

	/* Pad out to 56 mod 64 */
	if (count > 56)
	{
		/* Two lots of padding:  Pad the first block to 64 bytes */
		std::memset((BYTE*) &_context.data + count, 0, 64 - count);
		SHA1_BYTE_REVERSE(_context.data, BLOCK_SIZE);
		transform();

		/* Now fill the next block with 56 bytes */
		std::memset(&_context.data, 0, 56);
	}
	else
	{
		/* Pad block to 56 bytes */
		std::memset((BYTE*) &_context.data + count, 0, 56 - count);
	}
	SHA1_BYTE_REVERSE(_context.data, BLOCK_SIZE);

	/* Append length in bits and transform */
	_context.data[14] = highBitcount;
	_context.data[15] = lowBitcount;

	transform();
	SHA1_BYTE_REVERSE(_context.data, DIGEST_SIZE);

	unsigned char hash[DIGEST_SIZE];
	for (count = 0; count < DIGEST_SIZE; count++)
		hash[count] = (BYTE) ((_context.digest[count>>2]) >> (8*(3-(count & 0x3)))) & 0xff;
	_digest.clear();
	_digest.insert(_digest.begin(), hash, hash + DIGEST_SIZE);
	reset();
	return _digest;
}


/* The SHA f()-functions */
#define f1(x,y,z)   ( ( x & y ) | ( ~x & z ) )              /* Rounds  0-19 */
#define f2(x,y,z)   ( x ^ y ^ z )                           /* Rounds 20-39 */
#define f3(x,y,z)   ( ( x & y ) | ( x & z ) | ( y & z ) )   /* Rounds 40-59 */
#define f4(x,y,z)   ( x ^ y ^ z )                           /* Rounds 60-79 */


/* The SHA Mysterious Constants */
#define K1  0x5A827999L     /* Rounds  0-19 */
#define K2  0x6ED9EBA1L     /* Rounds 20-39 */
#define K3  0x8F1BBCDCL     /* Rounds 40-59 */
#define K4  0xCA62C1D6L     /* Rounds 60-79 */


/* 32-bit rotate - kludged with shifts */
typedef UInt32 UL;	/* to save space */


#define S(n,X)  ( ( ((UL)X) << n ) | ( ((UL)X) >> ( 32 - n ) ) )


/* The initial expanding function */
#define expand(count)   W[ count ] = S(1,(W[ count - 3 ] ^ W[ count - 8 ] ^ W[ count - 14 ] ^ W[ count - 16 ]))	/* to make this SHA-1 */


/* The four SHA sub-rounds */
#define subRound1(count)    \
{ \
    temp = S( 5, A ) + f1( B, C, D ) + E + W[ count ] + K1; \
    E = D; \
    D = C; \
    C = S( 30, B ); \
    B = A; \
    A = temp; \
}

#define subRound2(count)    \
{ \
    temp = S( 5, A ) + f2( B, C, D ) + E + W[ count ] + K2; \
    E = D; \
    D = C; \
    C = S( 30, B ); \
    B = A; \
    A = temp; \
}

#define subRound3(count)    \
{ \
    temp = S( 5, A ) + f3( B, C, D ) + E + W[ count ] + K3; \
    E = D; \
    D = C; \
    C = S( 30, B ); \
    B = A; \
    A = temp; \
}

#define subRound4(count)    \
{ \
    temp = S( 5, A ) + f4( B, C, D ) + E + W[ count ] + K4; \
    E = D; \
    D = C; \
    C = S( 30, B ); \
    B = A; \
    A = temp; \
}


void SHA1Engine::transform()
{
	UInt32 W[80];
	UInt32 temp;
	UInt32 A, B, C, D, E;
	int i;

	/* Step A.  Copy the data buffer into the local work buffer */
	for( i = 0; i < 16; i++ )
	W[ i ] = _context.data[ i ];

	/* Step B.  Expand the 16 words into 64 temporary data words */
	expand( 16 ); expand( 17 ); expand( 18 ); expand( 19 ); expand( 20 );
	expand( 21 ); expand( 22 ); expand( 23 ); expand( 24 ); expand( 25 );
	expand( 26 ); expand( 27 ); expand( 28 ); expand( 29 ); expand( 30 );
	expand( 31 ); expand( 32 ); expand( 33 ); expand( 34 ); expand( 35 );
	expand( 36 ); expand( 37 ); expand( 38 ); expand( 39 ); expand( 40 );
	expand( 41 ); expand( 42 ); expand( 43 ); expand( 44 ); expand( 45 );
	expand( 46 ); expand( 47 ); expand( 48 ); expand( 49 ); expand( 50 );
	expand( 51 ); expand( 52 ); expand( 53 ); expand( 54 ); expand( 55 );
	expand( 56 ); expand( 57 ); expand( 58 ); expand( 59 ); expand( 60 );
	expand( 61 ); expand( 62 ); expand( 63 ); expand( 64 ); expand( 65 );
	expand( 66 ); expand( 67 ); expand( 68 ); expand( 69 ); expand( 70 );
	expand( 71 ); expand( 72 ); expand( 73 ); expand( 74 ); expand( 75 );
	expand( 76 ); expand( 77 ); expand( 78 ); expand( 79 );

	/* Step C.  Set up first buffer */
	A = _context.digest[ 0 ];
	B = _context.digest[ 1 ];
	C = _context.digest[ 2 ];
	D = _context.digest[ 3 ];
	E = _context.digest[ 4 ];

	/* Step D.  Serious mangling, divided into four sub-rounds */
	subRound1( 0 ); subRound1( 1 ); subRound1( 2 ); subRound1( 3 );
	subRound1( 4 ); subRound1( 5 ); subRound1( 6 ); subRound1( 7 );
	subRound1( 8 ); subRound1( 9 ); subRound1( 10 ); subRound1( 11 );
	subRound1( 12 ); subRound1( 13 ); subRound1( 14 ); subRound1( 15 );
	subRound1( 16 ); subRound1( 17 ); subRound1( 18 ); subRound1( 19 );
	subRound2( 20 ); subRound2( 21 ); subRound2( 22 ); subRound2( 23 );
	subRound2( 24 ); subRound2( 25 ); subRound2( 26 ); subRound2( 27 );
	subRound2( 28 ); subRound2( 29 ); subRound2( 30 ); subRound2( 31 );
	subRound2( 32 ); subRound2( 33 ); subRound2( 34 ); subRound2( 35 );
	subRound2( 36 ); subRound2( 37 ); subRound2( 38 ); subRound2( 39 );
	subRound3( 40 ); subRound3( 41 ); subRound3( 42 ); subRound3( 43 );
	subRound3( 44 ); subRound3( 45 ); subRound3( 46 ); subRound3( 47 );
	subRound3( 48 ); subRound3( 49 ); subRound3( 50 ); subRound3( 51 );
	subRound3( 52 ); subRound3( 53 ); subRound3( 54 ); subRound3( 55 );
	subRound3( 56 ); subRound3( 57 ); subRound3( 58 ); subRound3( 59 );
	subRound4( 60 ); subRound4( 61 ); subRound4( 62 ); subRound4( 63 );
	subRound4( 64 ); subRound4( 65 ); subRound4( 66 ); subRound4( 67 );
	subRound4( 68 ); subRound4( 69 ); subRound4( 70 ); subRound4( 71 );
	subRound4( 72 ); subRound4( 73 ); subRound4( 74 ); subRound4( 75 );
	subRound4( 76 ); subRound4( 77 ); subRound4( 78 ); subRound4( 79 );

	/* Step E.  Build message digest */
	_context.digest[ 0 ] += A;
	_context.digest[ 1 ] += B;
	_context.digest[ 2 ] += C;
	_context.digest[ 3 ] += D;
	_context.digest[ 4 ] += E;
}


} // namespace Poco
