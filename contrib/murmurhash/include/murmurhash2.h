//-----------------------------------------------------------------------------
// MurmurHash2 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

#ifndef _MURMURHASH2_H_
#define _MURMURHASH2_H_

//-----------------------------------------------------------------------------
// Platform-specific functions and macros

// Microsoft Visual Studio

#if defined(_MSC_VER) && (_MSC_VER < 1600)

typedef unsigned char uint8_t;
typedef unsigned int uint32_t;
typedef unsigned __int64 uint64_t;

// Other compilers

#else	// defined(_MSC_VER)

#include <stdint.h>

#endif // !defined(_MSC_VER)

uint32_t MurmurHash2        (const void * key, int len, uint32_t seed);
uint64_t MurmurHash64A      (const void * key, int len, uint64_t seed);
uint64_t MurmurHash64B      (const void * key, int len, uint64_t seed);
uint32_t MurmurHash2A       (const void * key, int len, uint32_t seed);
uint32_t MurmurHashNeutral2 (const void * key, int len, uint32_t seed);
uint32_t MurmurHashAligned2 (const void * key, int len, uint32_t seed);

#endif // _MURMURHASH2_H_

