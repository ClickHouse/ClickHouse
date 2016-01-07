//
// Random.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Random.h#1 $
//
// Library: Foundation
// Package: Crypt
// Module:  Random
//
// Definition of class Random.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//
//
// Based on the FreeBSD random number generator.
// src/lib/libc/stdlib/random.c,v 1.25 
//
// Copyright (c) 1983, 1993
// The Regents of the University of California.  All rights reserved.
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 4. Neither the name of the University nor the names of its contributors
//    may be used to endorse or promote products derived from this software
//    without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//


#ifndef Foundation_Random_INCLUDED
#define Foundation_Random_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


class Foundation_API Random
	/// A better random number generator.
	/// Random implements a pseudo random number generator
	/// (PRNG). The PRNG is a nonlinear additive
	/// feedback random number generator using 256 bytes
	/// of state information and a period of up to 2^69.
{
public:
	enum Type
	{
		RND_STATE_0   =   8,  /// linear congruential
		RND_STATE_32  =  32,  /// x**7 + x**3 + 1
		RND_STATE_64  =  64,  /// x**15 + x + 1
		RND_STATE_128 = 128,  /// x**31 + x**3 + 1
		RND_STATE_256 = 256   /// x**63 + x + 1
	};

	Random(int stateSize = 256);
		/// Creates and initializes the PRNG.
		/// Specify either a state buffer size
		/// (8 to 256 bytes) or one of the Type values.

	~Random();
		/// Destroys the PRNG.

	void seed(UInt32 seed);
		/// Seeds the pseudo random generator with the given seed.

	void seed();
		/// Seeds the pseudo random generator with a random seed
		/// obtained from a RandomInputStream.

	UInt32 next();
		/// Returns the next 31-bit pseudo random number.

	UInt32 next(UInt32 n);
		/// Returns the next 31-bit pseudo random number modulo n.
	
	char nextChar();
		/// Returns the next pseudo random character.
	
	bool nextBool();
		/// Returns the next boolean pseudo random value.
		
	float nextFloat();
		/// Returns the next float pseudo random number between 0.0 and 1.0.
		
	double nextDouble();
		/// Returns the next double pseudo random number between 0.0 and 1.0.

protected:
	void initState(UInt32 seed, char* arg_state, Int32 n);
	static UInt32 goodRand(Int32 x);

private:
	enum
	{
		MAX_TYPES = 5,
		NSHUFF    = 50
	};

	UInt32* _fptr;
	UInt32* _rptr;
	UInt32* _state;
	int     _randType;
	int     _randDeg;
	int     _randSep;
	UInt32* _endPtr;
	char*  _pBuffer;
};


//
// inlines
//
inline UInt32 Random::next(UInt32 n)
{
	return next() % n;
}


inline char Random::nextChar()
{
	return char((next() >> 3) & 0xFF);
}


inline bool Random::nextBool()
{
	return (next() & 0x1000) != 0;
}

	
inline float Random::nextFloat()
{
	return float(next()) / 0x7FFFFFFF;
}

	
inline double Random::nextDouble()
{
	return double(next()) / 0x7FFFFFFF;
}


} // namespace Poco


#endif // Foundation_Random_INCLUDED
