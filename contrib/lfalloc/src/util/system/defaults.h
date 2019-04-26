#pragma once

#include "platform.h"

#if defined _unix_
#define LOCSLASH_C '/'
#define LOCSLASH_S "/"
#else
#define LOCSLASH_C '\\'
#define LOCSLASH_S "\\"
#endif // _unix_

#if defined(__INTEL_COMPILER) && defined(__cplusplus)
#include <new>
#endif

// low and high parts of integers
#if !defined(_win_)
#include <sys/param.h>
#endif

#if defined(BSD) || defined(_android_)

#if defined(BSD)
#include <machine/endian.h>
#endif

#if defined(_android_)
#include <endian.h>
#endif

#if (BYTE_ORDER == LITTLE_ENDIAN)
#define _little_endian_
#elif (BYTE_ORDER == BIG_ENDIAN)
#define _big_endian_
#else
#error unknown endian not supported
#endif

#elif (defined(_sun_) && !defined(__i386__)) || defined(_hpux_) || defined(WHATEVER_THAT_HAS_BIG_ENDIAN)
#define _big_endian_
#else
#define _little_endian_
#endif

// alignment
#if (defined(_sun_) && !defined(__i386__)) || defined(_hpux_) || defined(__alpha__) || defined(__ia64__) || defined(WHATEVER_THAT_NEEDS_ALIGNING_QUADS)
#define _must_align8_
#endif

#if (defined(_sun_) && !defined(__i386__)) || defined(_hpux_) || defined(__alpha__) || defined(__ia64__) || defined(WHATEVER_THAT_NEEDS_ALIGNING_LONGS)
#define _must_align4_
#endif

#if (defined(_sun_) && !defined(__i386__)) || defined(_hpux_) || defined(__alpha__) || defined(__ia64__) || defined(WHATEVER_THAT_NEEDS_ALIGNING_SHORTS)
#define _must_align2_
#endif

#if defined(__GNUC__)
#define alias_hack __attribute__((__may_alias__))
#endif

#ifndef alias_hack
#define alias_hack
#endif

#include "types.h"

#if defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L)
#define PRAGMA(x) _Pragma(#x)
#define RCSID(idstr) PRAGMA(comment(exestr, idstr))
#else
#define RCSID(idstr) static const char rcsid[] = idstr
#endif

#include "compiler.h"

#ifdef _win_
#include <malloc.h>
#elif defined(_sun_)
#include <alloca.h>
#endif

#ifdef NDEBUG
#define Y_IF_DEBUG(X)
#else
#define Y_IF_DEBUG(X) X
#endif

/**
 * @def Y_ARRAY_SIZE
 *
 * This macro is needed to get number of elements in a statically allocated fixed size array. The
 * expression is a compile-time constant and therefore can be used in compile time computations.
 *
 * @code
 * enum ENumbers {
 *     EN_ONE,
 *     EN_TWO,
 *     EN_SIZE
 * }
 *
 * const char* NAMES[] = {
 *     "one",
 *     "two"
 * }
 *
 * static_assert(Y_ARRAY_SIZE(NAMES) == EN_SIZE, "you should define `NAME` for each enumeration");
 * @endcode
 *
 * This macro also catches type errors. If you see a compiler error like "warning: division by zero
 * is undefined" when using `Y_ARRAY_SIZE` then you are probably giving it a pointer.
 *
 * Since all of our code is expected to work on a 64 bit platform where pointers are 8 bytes we may
 * falsefully accept pointers to types of sizes that are divisors of 8 (1, 2, 4 and 8).
 */
#if defined(__cplusplus)
namespace NArraySizePrivate {
    template <class T>
    struct TArraySize;

    template <class T, size_t N>
    struct TArraySize<T[N]> {
        enum {
            Result = N
        };
    };

    template <class T, size_t N>
    struct TArraySize<T (&)[N]> {
        enum {
            Result = N
        };
    };
}

#define Y_ARRAY_SIZE(arr) ((size_t)::NArraySizePrivate::TArraySize<decltype(arr)>::Result)
#else
#undef Y_ARRAY_SIZE
#define Y_ARRAY_SIZE(arr) \
    ((sizeof(arr) / sizeof((arr)[0])) / static_cast<size_t>(!(sizeof(arr) % sizeof((arr)[0]))))
#endif

#undef Y_ARRAY_BEGIN
#define Y_ARRAY_BEGIN(arr) (arr)

#undef Y_ARRAY_END
#define Y_ARRAY_END(arr) ((arr) + Y_ARRAY_SIZE(arr))

/**
 * Concatenates two symbols, even if one of them is itself a macro.
 */
#define Y_CAT(X, Y) Y_CAT_I(X, Y)
#define Y_CAT_I(X, Y) Y_CAT_II(X, Y)
#define Y_CAT_II(X, Y) X##Y

#define Y_STRINGIZE(X) UTIL_PRIVATE_STRINGIZE_AUX(X)
#define UTIL_PRIVATE_STRINGIZE_AUX(X) #X

#if defined(__COUNTER__)
#define Y_GENERATE_UNIQUE_ID(N) Y_CAT(N, __COUNTER__)
#endif

#if !defined(Y_GENERATE_UNIQUE_ID)
#define Y_GENERATE_UNIQUE_ID(N) Y_CAT(N, __LINE__)
#endif

#define NPOS ((size_t)-1)
