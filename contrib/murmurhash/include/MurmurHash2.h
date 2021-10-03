//-----------------------------------------------------------------------------
// MurmurHash2 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

#ifndef MURMURHASH2_H
#define MURMURHASH2_H

#include <stddef.h>

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

//-----------------------------------------------------------------------------

#ifdef __cplusplus
extern "C" {
#endif

uint32_t MurmurHash2        ( const void * key, size_t len, uint32_t seed );
uint64_t MurmurHash64A      ( const void * key, size_t len, uint64_t seed );
uint64_t MurmurHash64B      ( const void * key, size_t len, uint64_t seed );
uint32_t MurmurHash2A       ( const void * key, size_t len, uint32_t seed );
uint32_t MurmurHashNeutral2 ( const void * key, size_t len, uint32_t seed );
uint32_t MurmurHashAligned2 ( const void * key, size_t len, uint32_t seed );

#ifdef __cplusplus
}
#endif

//-----------------------------------------------------------------------------

#endif // _MURMURHASH2_H_

