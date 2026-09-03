//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

#ifndef MURMURHASH3_H
#define MURMURHASH3_H

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

void MurmurHash3_x86_32  ( const void * key, size_t len, uint32_t seed, void * out );

void MurmurHash3_x86_128 ( const void * key, size_t len, uint32_t seed, void * out );

void MurmurHash3_x64_128 ( const void * key, size_t len, uint32_t seed, void * out );

#ifdef __cplusplus
}
#endif

//-----------------------------------------------------------------------------

#endif // _MURMURHASH3_H_
