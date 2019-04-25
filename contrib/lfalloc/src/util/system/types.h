#pragma once

// DO_NOT_STYLE

#include "platform.h"

#include <inttypes.h>

typedef int8_t i8;
typedef int16_t i16;
typedef uint8_t ui8;
typedef uint16_t ui16;

typedef int yssize_t;
#define PRIYSZT "d"

#if defined(_darwin_) && defined(_32_)
typedef unsigned long ui32;
typedef long i32;
#else
typedef uint32_t ui32;
typedef int32_t i32;
#endif

#if defined(_darwin_) && defined(_64_)
typedef unsigned long ui64;
typedef long i64;
#else
typedef uint64_t ui64;
typedef int64_t i64;
#endif

#define LL(number) INT64_C(number)
#define ULL(number) UINT64_C(number)

// Macro for size_t and ptrdiff_t types
#if defined(_32_)
#   if defined(_darwin_)
#       define PRISZT "lu"
#       undef PRIi32
#       define PRIi32 "li"
#       undef SCNi32
#       define SCNi32 "li"
#       undef PRId32
#       define PRId32 "li"
#       undef SCNd32
#       define SCNd32 "li"
#       undef PRIu32
#       define PRIu32 "lu"
#       undef SCNu32
#       define SCNu32 "lu"
#       undef PRIx32
#       define PRIx32 "lx"
#       undef SCNx32
#       define SCNx32 "lx"
#   elif !defined(_cygwin_)
#       define PRISZT PRIu32
#   else
#       define PRISZT "u"
#   endif
#   define SCNSZT SCNu32
#   define PRIPDT PRIi32
#   define SCNPDT SCNi32
#   define PRITMT PRIi32
#   define SCNTMT SCNi32
#elif defined(_64_)
#   if defined(_darwin_)
#       define PRISZT "lu"
#       undef PRIu64
#       define PRIu64 PRISZT
#       undef PRIx64
#       define PRIx64 "lx"
#       undef PRIX64
#       define PRIX64 "lX"
#       undef PRId64
#       define PRId64 "ld"
#       undef PRIi64
#       define PRIi64 "li"
#       undef SCNi64
#       define SCNi64 "li"
#       undef SCNu64
#       define SCNu64 "lu"
#       undef SCNx64
#       define SCNx64 "lx"
#   else
#       define PRISZT PRIu64
#   endif
#   define SCNSZT SCNu64
#   define PRIPDT PRIi64
#   define SCNPDT SCNi64
#   define PRITMT PRIi64
#   define SCNTMT SCNi64
#else
#   error "Unsupported platform"
#endif

// SUPERLONG
#if !defined(DONT_USE_SUPERLONG) && !defined(SUPERLONG_MAX)
#define SUPERLONG_MAX ~LL(0)
typedef i64 SUPERLONG;
#endif

// UNICODE
// UCS-2, native byteorder
typedef ui16 wchar16;
// internal symbol type: UTF-16LE
typedef wchar16 TChar;
typedef ui32 wchar32;

#if defined(_MSC_VER)
#include <basetsd.h>
typedef SSIZE_T ssize_t;
#define HAVE_SSIZE_T 1
#include <wchar.h>
#endif

#include <sys/types.h>
