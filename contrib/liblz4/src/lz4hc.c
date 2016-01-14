/*
   LZ4 HC - High Compression Mode of LZ4
   Copyright (C) 2011-2014, Yann Collet.
   BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

       * Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
       * Redistributions in binary form must reproduce the above
   copyright notice, this list of conditions and the following disclaimer
   in the documentation and/or other materials provided with the
   distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

   You can contact the author at :
   - LZ4 homepage : http://fastcompression.blogspot.com/p/lz4.html
   - LZ4 source repository : http://code.google.com/p/lz4/
*/



/**************************************
   Tuning Parameter
**************************************/
#define LZ4HC_DEFAULT_COMPRESSIONLEVEL 8


/**************************************
   Memory routines
**************************************/
#include <stdlib.h>   /* calloc, free */
#define ALLOCATOR(s)  calloc(1,s)
#define FREEMEM       free
#include <string.h>   /* memset, memcpy */
#define MEM_INIT      memset


/**************************************
   CPU Feature Detection
**************************************/
/* 32 or 64 bits ? */
#if (defined(__x86_64__) || defined(_M_X64) || defined(_WIN64) \
  || defined(__powerpc64__) || defined(__ppc64__) || defined(__PPC64__) \
  || defined(__64BIT__) || defined(_LP64) || defined(__LP64__) \
  || defined(__ia64) || defined(__itanium__) || defined(_M_IA64) )   /* Detects 64 bits mode */
#  define LZ4_ARCH64 1
#else
#  define LZ4_ARCH64 0
#endif

/*
 * Little Endian or Big Endian ?
 * Overwrite the #define below if you know your architecture endianess
 */
#if defined (__GLIBC__)
#  include <endian.h>
#  if (__BYTE_ORDER == __BIG_ENDIAN)
#     define LZ4_BIG_ENDIAN 1
#  endif
#elif (defined(__BIG_ENDIAN__) || defined(__BIG_ENDIAN) || defined(_BIG_ENDIAN)) && !(defined(__LITTLE_ENDIAN__) || defined(__LITTLE_ENDIAN) || defined(_LITTLE_ENDIAN))
#  define LZ4_BIG_ENDIAN 1
#elif defined(__sparc) || defined(__sparc__) \
   || defined(__powerpc__) || defined(__ppc__) || defined(__PPC__) \
   || defined(__hpux)  || defined(__hppa) \
   || defined(_MIPSEB) || defined(__s390__)
#  define LZ4_BIG_ENDIAN 1
#else
/* Little Endian assumed. PDP Endian and other very rare endian format are unsupported. */
#endif

/*
 * Unaligned memory access is automatically enabled for "common" CPU, such as x86.
 * For others CPU, the compiler will be more cautious, and insert extra code to ensure aligned access is respected
 * If you know your target CPU supports unaligned memory access, you want to force this option manually to improve performance
 */
#if defined(__ARM_FEATURE_UNALIGNED)
#  define LZ4_FORCE_UNALIGNED_ACCESS 1
#endif

/* Define this parameter if your target system or compiler does not support hardware bit count */
#if defined(_MSC_VER) && defined(_WIN32_WCE)            /* Visual Studio for Windows CE does not support Hardware bit count */
#  define LZ4_FORCE_SW_BITCOUNT
#endif


/**************************************
 Compiler Options
**************************************/
#if defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L)   /* C99 */
/* "restrict" is a known keyword */
#else
#  define restrict /* Disable restrict */
#endif

#ifdef _MSC_VER    /* Visual Studio */
#  define FORCE_INLINE static __forceinline
#  include <intrin.h>                    /* For Visual 2005 */
#  if LZ4_ARCH64   /* 64-bits */
#    pragma intrinsic(_BitScanForward64) /* For Visual 2005 */
#    pragma intrinsic(_BitScanReverse64) /* For Visual 2005 */
#  else            /* 32-bits */
#    pragma intrinsic(_BitScanForward)   /* For Visual 2005 */
#    pragma intrinsic(_BitScanReverse)   /* For Visual 2005 */
#  endif
#  pragma warning(disable : 4127)        /* disable: C4127: conditional expression is constant */
#  pragma warning(disable : 4701)        /* disable: C4701: potentially uninitialized local variable used */
#else
#  ifdef __GNUC__
#    define FORCE_INLINE static inline __attribute__((always_inline))
#  else
#    define FORCE_INLINE static inline
#  endif
#endif

#ifdef _MSC_VER  /* Visual Studio */
#  define lz4_bswap16(x) _byteswap_ushort(x)
#else
#  define lz4_bswap16(x)  ((unsigned short int) ((((x) >> 8) & 0xffu) | (((x) & 0xffu) << 8)))
#endif


/**************************************
   Includes
**************************************/
#include <lz4/lz4hc.h>
#include <lz4/lz4.h>


/**************************************
   Basic Types
**************************************/
#if defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L)   /* C99 */
# include <stdint.h>
  typedef uint8_t  BYTE;
  typedef uint16_t U16;
  typedef uint32_t U32;
  typedef  int32_t S32;
  typedef uint64_t U64;
#else
  typedef unsigned char       BYTE;
  typedef unsigned short      U16;
  typedef unsigned int        U32;
  typedef   signed int        S32;
  typedef unsigned long long  U64;
#endif

#if defined(__GNUC__)  && !defined(LZ4_FORCE_UNALIGNED_ACCESS)
#  define _PACKED __attribute__ ((packed))
#else
#  define _PACKED
#endif

#if !defined(LZ4_FORCE_UNALIGNED_ACCESS) && !defined(__GNUC__)
#  ifdef __IBMC__
#    pragma pack(1)
#  else
#    pragma pack(push, 1)
#  endif
#endif

typedef struct _U16_S { U16 v; } _PACKED U16_S;
typedef struct _U32_S { U32 v; } _PACKED U32_S;
typedef struct _U64_S { U64 v; } _PACKED U64_S;

#if !defined(LZ4_FORCE_UNALIGNED_ACCESS) && !defined(__GNUC__)
#  pragma pack(pop)
#endif

#define A64(x) (((U64_S *)(x))->v)
#define A32(x) (((U32_S *)(x))->v)
#define A16(x) (((U16_S *)(x))->v)


/**************************************
   Constants
**************************************/
#define MINMATCH 4

#define DICTIONARY_LOGSIZE 16
#define MAXD (1<<DICTIONARY_LOGSIZE)
#define MAXD_MASK ((U32)(MAXD - 1))
#define MAX_DISTANCE (MAXD - 1)

#define HASH_LOG (DICTIONARY_LOGSIZE-1)
#define HASHTABLESIZE (1 << HASH_LOG)
#define HASH_MASK (HASHTABLESIZE - 1)

#define ML_BITS  4
#define ML_MASK  (size_t)((1U<<ML_BITS)-1)
#define RUN_BITS (8-ML_BITS)
#define RUN_MASK ((1U<<RUN_BITS)-1)

#define COPYLENGTH 8
#define LASTLITERALS 5
#define MFLIMIT (COPYLENGTH+MINMATCH)
#define MINLENGTH (MFLIMIT+1)
#define OPTIMAL_ML (int)((ML_MASK-1)+MINMATCH)

#define KB *(1U<<10)
#define MB *(1U<<20)
#define GB *(1U<<30)


/**************************************
   Architecture-specific macros
**************************************/
#if LZ4_ARCH64   /* 64-bit */
#  define STEPSIZE 8
#  define LZ4_COPYSTEP(s,d)     A64(d) = A64(s); d+=8; s+=8;
#  define LZ4_COPYPACKET(s,d)   LZ4_COPYSTEP(s,d)
#  define AARCH A64
#  define HTYPE                 U32
#  define INITBASE(b,s)         const BYTE* const b = s
#else            /* 32-bit */
#  define STEPSIZE 4
#  define LZ4_COPYSTEP(s,d)     A32(d) = A32(s); d+=4; s+=4;
#  define LZ4_COPYPACKET(s,d)   LZ4_COPYSTEP(s,d); LZ4_COPYSTEP(s,d);
#  define AARCH A32
#  define HTYPE                 U32
#  define INITBASE(b,s)         const BYTE* const b = s
#endif

#if defined(LZ4_BIG_ENDIAN)
#  define LZ4_READ_LITTLEENDIAN_16(d,s,p) { U16 v = A16(p); v = lz4_bswap16(v); d = (s) - v; }
#  define LZ4_WRITE_LITTLEENDIAN_16(p,i)  { U16 v = (U16)(i); v = lz4_bswap16(v); A16(p) = v; p+=2; }
#else      /* Little Endian */
#  define LZ4_READ_LITTLEENDIAN_16(d,s,p) { d = (s) - A16(p); }
#  define LZ4_WRITE_LITTLEENDIAN_16(p,v)  { A16(p) = v; p+=2; }
#endif


/**************************************
   Local Types
**************************************/
typedef struct
{
    const BYTE* inputBuffer;
    const BYTE* base;
    const BYTE* end;
    HTYPE hashTable[HASHTABLESIZE];
    U16 chainTable[MAXD];
    const BYTE* nextToUpdate;
} LZ4HC_Data_Structure;


/**************************************
   Macros
**************************************/
#define LZ4_WILDCOPY(s,d,e)    do { LZ4_COPYPACKET(s,d) } while (d<e);
#define LZ4_BLINDCOPY(s,d,l)   { BYTE* e=d+l; LZ4_WILDCOPY(s,d,e); d=e; }
#define HASH_FUNCTION(i)       (((i) * 2654435761U) >> ((MINMATCH*8)-HASH_LOG))
#define HASH_VALUE(p)          HASH_FUNCTION(A32(p))
#define HASH_POINTER(p)        (HashTable[HASH_VALUE(p)] + base)
#define DELTANEXT(p)           chainTable[(size_t)(p) & MAXD_MASK]
#define GETNEXT(p)             ((p) - (size_t)DELTANEXT(p))


/**************************************
 Private functions
**************************************/
#if LZ4_ARCH64

FORCE_INLINE int LZ4_NbCommonBytes (register U64 val)
{
#if defined(LZ4_BIG_ENDIAN)
#  if defined(_MSC_VER) && !defined(LZ4_FORCE_SW_BITCOUNT)
    unsigned long r = 0;
    _BitScanReverse64( &r, val );
    return (int)(r>>3);
#  elif defined(__GNUC__) && ((__GNUC__ * 100 + __GNUC_MINOR__) >= 304) && !defined(LZ4_FORCE_SW_BITCOUNT)
    return (__builtin_clzll(val) >> 3);
#  else
    int r;
    if (!(val>>32)) { r=4; } else { r=0; val>>=32; }
    if (!(val>>16)) { r+=2; val>>=8; } else { val>>=24; }
    r += (!val);
    return r;
#  endif
#else
#  if defined(_MSC_VER) && !defined(LZ4_FORCE_SW_BITCOUNT)
    unsigned long r = 0;
    _BitScanForward64( &r, val );
    return (int)(r>>3);
#  elif defined(__GNUC__) && ((__GNUC__ * 100 + __GNUC_MINOR__) >= 304) && !defined(LZ4_FORCE_SW_BITCOUNT)
    return (__builtin_ctzll(val) >> 3);
#  else
    static const int DeBruijnBytePos[64] = { 0, 0, 0, 0, 0, 1, 1, 2, 0, 3, 1, 3, 1, 4, 2, 7, 0, 2, 3, 6, 1, 5, 3, 5, 1, 3, 4, 4, 2, 5, 6, 7, 7, 0, 1, 2, 3, 3, 4, 6, 2, 6, 5, 5, 3, 4, 5, 6, 7, 1, 2, 4, 6, 4, 4, 5, 7, 2, 6, 5, 7, 6, 7, 7 };
    return DeBruijnBytePos[((U64)((val & -val) * 0x0218A392CDABBD3F)) >> 58];
#  endif
#endif
}

#else

FORCE_INLINE int LZ4_NbCommonBytes (register U32 val)
{
#if defined(LZ4_BIG_ENDIAN)
#  if defined(_MSC_VER) && !defined(LZ4_FORCE_SW_BITCOUNT)
    unsigned long r;
    _BitScanReverse( &r, val );
    return (int)(r>>3);
#  elif defined(__GNUC__) && ((__GNUC__ * 100 + __GNUC_MINOR__) >= 304) && !defined(LZ4_FORCE_SW_BITCOUNT)
    return (__builtin_clz(val) >> 3);
#  else
    int r;
    if (!(val>>16)) { r=2; val>>=8; } else { r=0; val>>=24; }
    r += (!val);
    return r;
#  endif
#else
#  if defined(_MSC_VER) && !defined(LZ4_FORCE_SW_BITCOUNT)
    unsigned long r;
    _BitScanForward( &r, val );
    return (int)(r>>3);
#  elif defined(__GNUC__) && ((__GNUC__ * 100 + __GNUC_MINOR__) >= 304) && !defined(LZ4_FORCE_SW_BITCOUNT)
    return (__builtin_ctz(val) >> 3);
#  else
    static const int DeBruijnBytePos[32] = { 0, 0, 3, 0, 3, 1, 3, 0, 3, 2, 2, 1, 3, 2, 0, 1, 3, 3, 1, 2, 2, 2, 2, 0, 3, 1, 2, 0, 1, 0, 1, 1 };
    return DeBruijnBytePos[((U32)((val & -(S32)val) * 0x077CB531U)) >> 27];
#  endif
#endif
}

#endif


int LZ4_sizeofStreamStateHC()
{
    return sizeof(LZ4HC_Data_Structure);
}

FORCE_INLINE void LZ4_initHC (LZ4HC_Data_Structure* hc4, const BYTE* base)
{
    MEM_INIT((void*)hc4->hashTable, 0, sizeof(hc4->hashTable));
    MEM_INIT(hc4->chainTable, 0xFF, sizeof(hc4->chainTable));
    hc4->nextToUpdate = base + 1;
    hc4->base = base;
    hc4->inputBuffer = base;
    hc4->end = base;
}

int LZ4_resetStreamStateHC(void* state, const char* inputBuffer)
{
    if ((((size_t)state) & (sizeof(void*)-1)) != 0) return 1;   /* Error : pointer is not aligned for pointer (32 or 64 bits) */
    LZ4_initHC((LZ4HC_Data_Structure*)state, (const BYTE*)inputBuffer);
    return 0;
}


void* LZ4_createHC (const char* inputBuffer)
{
    void* hc4 = ALLOCATOR(sizeof(LZ4HC_Data_Structure));
    LZ4_initHC ((LZ4HC_Data_Structure*)hc4, (const BYTE*)inputBuffer);
    return hc4;
}


int LZ4_freeHC (void* LZ4HC_Data)
{
    FREEMEM(LZ4HC_Data);
    return (0);
}


/* Update chains up to ip (excluded) */
FORCE_INLINE void LZ4HC_Insert (LZ4HC_Data_Structure* hc4, const BYTE* ip)
{
    U16*   chainTable = hc4->chainTable;
    HTYPE* HashTable  = hc4->hashTable;
    INITBASE(base,hc4->base);

    while(hc4->nextToUpdate < ip)
    {
        const BYTE* const p = hc4->nextToUpdate;
        size_t delta = (p) - HASH_POINTER(p);
        if (delta>MAX_DISTANCE) delta = MAX_DISTANCE;
        DELTANEXT(p) = (U16)delta;
        HashTable[HASH_VALUE(p)] = (HTYPE)((p) - base);
        hc4->nextToUpdate++;
    }
}


char* LZ4_slideInputBufferHC(void* LZ4HC_Data)
{
    LZ4HC_Data_Structure* hc4 = (LZ4HC_Data_Structure*)LZ4HC_Data;
    U32 distance = (U32)(hc4->end - hc4->inputBuffer) - 64 KB;
    distance = (distance >> 16) << 16;   /* Must be a multiple of 64 KB */
    LZ4HC_Insert(hc4, hc4->end - MINMATCH);
    memcpy((void*)(hc4->end - 64 KB - distance), (const void*)(hc4->end - 64 KB), 64 KB);
    hc4->nextToUpdate -= distance;
    hc4->base -= distance;
    if ((U32)(hc4->inputBuffer - hc4->base) > 1 GB + 64 KB)   /* Avoid overflow */
    {
        int i;
        hc4->base += 1 GB;
        for (i=0; i<HASHTABLESIZE; i++) hc4->hashTable[i] -= 1 GB;
    }
    hc4->end -= distance;
    return (char*)(hc4->end);
}


FORCE_INLINE size_t LZ4HC_CommonLength (const BYTE* p1, const BYTE* p2, const BYTE* const matchlimit)
{
    const BYTE* p1t = p1;

    while (p1t<matchlimit-(STEPSIZE-1))
    {
        size_t diff = AARCH(p2) ^ AARCH(p1t);
        if (!diff) { p1t+=STEPSIZE; p2+=STEPSIZE; continue; }
        p1t += LZ4_NbCommonBytes(diff);
        return (p1t - p1);
    }
    if (LZ4_ARCH64) if ((p1t<(matchlimit-3)) && (A32(p2) == A32(p1t))) { p1t+=4; p2+=4; }
    if ((p1t<(matchlimit-1)) && (A16(p2) == A16(p1t))) { p1t+=2; p2+=2; }
    if ((p1t<matchlimit) && (*p2 == *p1t)) p1t++;
    return (p1t - p1);
}


FORCE_INLINE int LZ4HC_InsertAndFindBestMatch (LZ4HC_Data_Structure* hc4, const BYTE* ip, const BYTE* const matchlimit, const BYTE** matchpos, const int maxNbAttempts)
{
    U16* const chainTable = hc4->chainTable;
    HTYPE* const HashTable = hc4->hashTable;
    const BYTE* ref;
    INITBASE(base,hc4->base);
    int nbAttempts=maxNbAttempts;
    size_t repl=0, ml=0;
    U16 delta=0;  /* useless assignment, to remove an uninitialization warning */

    /* HC4 match finder */
    LZ4HC_Insert(hc4, ip);
    ref = HASH_POINTER(ip);

#define REPEAT_OPTIMIZATION
#ifdef REPEAT_OPTIMIZATION
    /* Detect repetitive sequences of length <= 4 */
    if ((U32)(ip-ref) <= 4)        /* potential repetition */
    {
        if (A32(ref) == A32(ip))   /* confirmed */
        {
            delta = (U16)(ip-ref);
            repl = ml  = LZ4HC_CommonLength(ip+MINMATCH, ref+MINMATCH, matchlimit) + MINMATCH;
            *matchpos = ref;
        }
        ref = GETNEXT(ref);
    }
#endif

    while (((U32)(ip-ref) <= MAX_DISTANCE) && (nbAttempts))
    {
        nbAttempts--;
        if (*(ref+ml) == *(ip+ml))
        if (A32(ref) == A32(ip))
        {
            size_t mlt = LZ4HC_CommonLength(ip+MINMATCH, ref+MINMATCH, matchlimit) + MINMATCH;
            if (mlt > ml) { ml = mlt; *matchpos = ref; }
        }
        ref = GETNEXT(ref);
    }

#ifdef REPEAT_OPTIMIZATION
    /* Complete table */
    if (repl)
    {
        const BYTE* ptr = ip;
        const BYTE* end;

        end = ip + repl - (MINMATCH-1);
        while(ptr < end-delta)
        {
            DELTANEXT(ptr) = delta;    /* Pre-Load */
            ptr++;
        }
        do
        {
            DELTANEXT(ptr) = delta;
            HashTable[HASH_VALUE(ptr)] = (HTYPE)((ptr) - base);     /* Head of chain */
            ptr++;
        } while(ptr < end);
        hc4->nextToUpdate = end;
    }
#endif

    return (int)ml;
}


FORCE_INLINE int LZ4HC_InsertAndGetWiderMatch (LZ4HC_Data_Structure* hc4, const BYTE* ip, const BYTE* startLimit, const BYTE* matchlimit, int longest, const BYTE** matchpos, const BYTE** startpos, const int maxNbAttempts)
{
    U16* const  chainTable = hc4->chainTable;
    HTYPE* const HashTable = hc4->hashTable;
    INITBASE(base,hc4->base);
    const BYTE*  ref;
    int nbAttempts = maxNbAttempts;
    int delta = (int)(ip-startLimit);

    /* First Match */
    LZ4HC_Insert(hc4, ip);
    ref = HASH_POINTER(ip);

    while (((U32)(ip-ref) <= MAX_DISTANCE) && (nbAttempts))
    {
        nbAttempts--;
        if (*(startLimit + longest) == *(ref - delta + longest))
        if (A32(ref) == A32(ip))
        {
#if 1
            const BYTE* reft = ref+MINMATCH;
            const BYTE* ipt = ip+MINMATCH;
            const BYTE* startt = ip;

            while (ipt<matchlimit-(STEPSIZE-1))
            {
                size_t diff = AARCH(reft) ^ AARCH(ipt);
                if (!diff) { ipt+=STEPSIZE; reft+=STEPSIZE; continue; }
                ipt += LZ4_NbCommonBytes(diff);
                goto _endCount;
            }
            if (LZ4_ARCH64) if ((ipt<(matchlimit-3)) && (A32(reft) == A32(ipt))) { ipt+=4; reft+=4; }
            if ((ipt<(matchlimit-1)) && (A16(reft) == A16(ipt))) { ipt+=2; reft+=2; }
            if ((ipt<matchlimit) && (*reft == *ipt)) ipt++;
_endCount:
            reft = ref;
#else
            /* Easier for code maintenance, but unfortunately slower too */
            const BYTE* startt = ip;
            const BYTE* reft = ref;
            const BYTE* ipt = ip + MINMATCH + LZ4HC_CommonLength(ip+MINMATCH, ref+MINMATCH, matchlimit);
#endif

            while ((startt>startLimit) && (reft > hc4->inputBuffer) && (startt[-1] == reft[-1])) {startt--; reft--;}

            if ((ipt-startt) > longest)
            {
                longest = (int)(ipt-startt);
                *matchpos = reft;
                *startpos = startt;
            }
        }
        ref = GETNEXT(ref);
    }

    return longest;
}


typedef enum { noLimit = 0, limitedOutput = 1 } limitedOutput_directive;

FORCE_INLINE int LZ4HC_encodeSequence (
                       const BYTE** ip,
                       BYTE** op,
                       const BYTE** anchor,
                       int matchLength,
                       const BYTE* ref,
                       limitedOutput_directive limitedOutputBuffer,
                       BYTE* oend)
{
    int length;
    BYTE* token;

    /* Encode Literal length */
    length = (int)(*ip - *anchor);
    token = (*op)++;
    if ((limitedOutputBuffer) && ((*op + length + (2 + 1 + LASTLITERALS) + (length>>8)) > oend)) return 1;   /* Check output limit */
    if (length>=(int)RUN_MASK) { int len; *token=(RUN_MASK<<ML_BITS); len = length-RUN_MASK; for(; len > 254 ; len-=255) *(*op)++ = 255;  *(*op)++ = (BYTE)len; }
    else *token = (BYTE)(length<<ML_BITS);

    /* Copy Literals */
    LZ4_BLINDCOPY(*anchor, *op, length);

    /* Encode Offset */
    LZ4_WRITE_LITTLEENDIAN_16(*op,(U16)(*ip-ref));

    /* Encode MatchLength */
    length = (int)(matchLength-MINMATCH);
    if ((limitedOutputBuffer) && (*op + (1 + LASTLITERALS) + (length>>8) > oend)) return 1;   /* Check output limit */
    if (length>=(int)ML_MASK) { *token+=ML_MASK; length-=ML_MASK; for(; length > 509 ; length-=510) { *(*op)++ = 255; *(*op)++ = 255; } if (length > 254) { length-=255; *(*op)++ = 255; } *(*op)++ = (BYTE)length; }
    else *token += (BYTE)(length);

    /* Prepare next loop */
    *ip += matchLength;
    *anchor = *ip;

    return 0;
}


#define MAX_COMPRESSION_LEVEL 16
static int LZ4HC_compress_generic (
                 void* ctxvoid,
                 const char* source,
                 char* dest,
                 int inputSize,
                 int maxOutputSize,
                 int compressionLevel,
                 limitedOutput_directive limit
                )
{
    LZ4HC_Data_Structure* ctx = (LZ4HC_Data_Structure*) ctxvoid;
    const BYTE* ip = (const BYTE*) source;
    const BYTE* anchor = ip;
    const BYTE* const iend = ip + inputSize;
    const BYTE* const mflimit = iend - MFLIMIT;
    const BYTE* const matchlimit = (iend - LASTLITERALS);

    BYTE* op = (BYTE*) dest;
    BYTE* const oend = op + maxOutputSize;

    const int maxNbAttempts = compressionLevel > MAX_COMPRESSION_LEVEL ? 1 << MAX_COMPRESSION_LEVEL : compressionLevel ? 1<<(compressionLevel-1) : 1<<LZ4HC_DEFAULT_COMPRESSIONLEVEL;
    int   ml, ml2, ml3, ml0;
    const BYTE* ref=NULL;
    const BYTE* start2=NULL;
    const BYTE* ref2=NULL;
    const BYTE* start3=NULL;
    const BYTE* ref3=NULL;
    const BYTE* start0;
    const BYTE* ref0;


    /* Ensure blocks follow each other */
    if (ip != ctx->end) return 0;
    ctx->end += inputSize;

    ip++;

    /* Main Loop */
    while (ip < mflimit)
    {
        ml = LZ4HC_InsertAndFindBestMatch (ctx, ip, matchlimit, (&ref), maxNbAttempts);
        if (!ml) { ip++; continue; }

        /* saved, in case we would skip too much */
        start0 = ip;
        ref0 = ref;
        ml0 = ml;

_Search2:
        if (ip+ml < mflimit)
            ml2 = LZ4HC_InsertAndGetWiderMatch(ctx, ip + ml - 2, ip + 1, matchlimit, ml, &ref2, &start2, maxNbAttempts);
        else ml2 = ml;

        if (ml2 == ml)  /* No better match */
        {
            if (LZ4HC_encodeSequence(&ip, &op, &anchor, ml, ref, limit, oend)) return 0;
            continue;
        }

        if (start0 < ip)
        {
            if (start2 < ip + ml0)   /* empirical */
            {
                ip = start0;
                ref = ref0;
                ml = ml0;
            }
        }

        /* Here, start0==ip */
        if ((start2 - ip) < 3)   /* First Match too small : removed */
        {
            ml = ml2;
            ip = start2;
            ref =ref2;
            goto _Search2;
        }

_Search3:
        /*
         * Currently we have :
         * ml2 > ml1, and
         * ip1+3 <= ip2 (usually < ip1+ml1)
         */
        if ((start2 - ip) < OPTIMAL_ML)
        {
            int correction;
            int new_ml = ml;
            if (new_ml > OPTIMAL_ML) new_ml = OPTIMAL_ML;
            if (ip+new_ml > start2 + ml2 - MINMATCH) new_ml = (int)(start2 - ip) + ml2 - MINMATCH;
            correction = new_ml - (int)(start2 - ip);
            if (correction > 0)
            {
                start2 += correction;
                ref2 += correction;
                ml2 -= correction;
            }
        }
        /* Now, we have start2 = ip+new_ml, with new_ml = min(ml, OPTIMAL_ML=18) */

        if (start2 + ml2 < mflimit)
            ml3 = LZ4HC_InsertAndGetWiderMatch(ctx, start2 + ml2 - 3, start2, matchlimit, ml2, &ref3, &start3, maxNbAttempts);
        else ml3 = ml2;

        if (ml3 == ml2) /* No better match : 2 sequences to encode */
        {
            /* ip & ref are known; Now for ml */
            if (start2 < ip+ml)  ml = (int)(start2 - ip);
            /* Now, encode 2 sequences */
            if (LZ4HC_encodeSequence(&ip, &op, &anchor, ml, ref, limit, oend)) return 0;
            ip = start2;
            if (LZ4HC_encodeSequence(&ip, &op, &anchor, ml2, ref2, limit, oend)) return 0;
            continue;
        }

        if (start3 < ip+ml+3) /* Not enough space for match 2 : remove it */
        {
            if (start3 >= (ip+ml)) /* can write Seq1 immediately ==> Seq2 is removed, so Seq3 becomes Seq1 */
            {
                if (start2 < ip+ml)
                {
                    int correction = (int)(ip+ml - start2);
                    start2 += correction;
                    ref2 += correction;
                    ml2 -= correction;
                    if (ml2 < MINMATCH)
                    {
                        start2 = start3;
                        ref2 = ref3;
                        ml2 = ml3;
                    }
                }

                if (LZ4HC_encodeSequence(&ip, &op, &anchor, ml, ref, limit, oend)) return 0;
                ip  = start3;
                ref = ref3;
                ml  = ml3;

                start0 = start2;
                ref0 = ref2;
                ml0 = ml2;
                goto _Search2;
            }

            start2 = start3;
            ref2 = ref3;
            ml2 = ml3;
            goto _Search3;
        }

        /*
         * OK, now we have 3 ascending matches; let's write at least the first one
         * ip & ref are known; Now for ml
         */
        if (start2 < ip+ml)
        {
            if ((start2 - ip) < (int)ML_MASK)
            {
                int correction;
                if (ml > OPTIMAL_ML) ml = OPTIMAL_ML;
                if (ip + ml > start2 + ml2 - MINMATCH) ml = (int)(start2 - ip) + ml2 - MINMATCH;
                correction = ml - (int)(start2 - ip);
                if (correction > 0)
                {
                    start2 += correction;
                    ref2 += correction;
                    ml2 -= correction;
                }
            }
            else
            {
                ml = (int)(start2 - ip);
            }
        }
        if (LZ4HC_encodeSequence(&ip, &op, &anchor, ml, ref, limit, oend)) return 0;

        ip = start2;
        ref = ref2;
        ml = ml2;

        start2 = start3;
        ref2 = ref3;
        ml2 = ml3;

        goto _Search3;

    }

    /* Encode Last Literals */
    {
        int lastRun = (int)(iend - anchor);
        if ((limit) && (((char*)op - dest) + lastRun + 1 + ((lastRun+255-RUN_MASK)/255) > (U32)maxOutputSize)) return 0;  /* Check output limit */
        if (lastRun>=(int)RUN_MASK) { *op++=(RUN_MASK<<ML_BITS); lastRun-=RUN_MASK; for(; lastRun > 254 ; lastRun-=255) *op++ = 255; *op++ = (BYTE) lastRun; }
        else *op++ = (BYTE)(lastRun<<ML_BITS);
        memcpy(op, anchor, iend - anchor);
        op += iend-anchor;
    }

    /* End */
    return (int) (((char*)op)-dest);
}


int LZ4_compressHC2(const char* source, char* dest, int inputSize, int compressionLevel)
{
    void* ctx = LZ4_createHC(source);
    int result;
    if (ctx==NULL) return 0;

    result = LZ4HC_compress_generic (ctx, source, dest, inputSize, 0, compressionLevel, noLimit);

    LZ4_freeHC(ctx);
    return result;
}

int LZ4_compressHC(const char* source, char* dest, int inputSize) { return LZ4_compressHC2(source, dest, inputSize, 0); }

int LZ4_compressHC2_limitedOutput(const char* source, char* dest, int inputSize, int maxOutputSize, int compressionLevel)
{
    void* ctx = LZ4_createHC(source);
    int result;
    if (ctx==NULL) return 0;

    result = LZ4HC_compress_generic (ctx, source, dest, inputSize, maxOutputSize, compressionLevel, limitedOutput);

    LZ4_freeHC(ctx);
    return result;
}

int LZ4_compressHC_limitedOutput(const char* source, char* dest, int inputSize, int maxOutputSize)
{
    return LZ4_compressHC2_limitedOutput(source, dest, inputSize, maxOutputSize, 0);
}


/*****************************
   Using external allocation
*****************************/
int LZ4_sizeofStateHC() { return sizeof(LZ4HC_Data_Structure); }


int LZ4_compressHC2_withStateHC (void* state, const char* source, char* dest, int inputSize, int compressionLevel)
{
    if (((size_t)(state)&(sizeof(void*)-1)) != 0) return 0;   /* Error : state is not aligned for pointers (32 or 64 bits) */
    LZ4_initHC ((LZ4HC_Data_Structure*)state, (const BYTE*)source);
    return LZ4HC_compress_generic (state, source, dest, inputSize, 0, compressionLevel, noLimit);
}

int LZ4_compressHC_withStateHC (void* state, const char* source, char* dest, int inputSize)
{ return LZ4_compressHC2_withStateHC (state, source, dest, inputSize, 0); }


int LZ4_compressHC2_limitedOutput_withStateHC (void* state, const char* source, char* dest, int inputSize, int maxOutputSize, int compressionLevel)
{
    if (((size_t)(state)&(sizeof(void*)-1)) != 0) return 0;   /* Error : state is not aligned for pointers (32 or 64 bits) */
    LZ4_initHC ((LZ4HC_Data_Structure*)state, (const BYTE*)source);
    return LZ4HC_compress_generic (state, source, dest, inputSize, maxOutputSize, compressionLevel, limitedOutput);
}

int LZ4_compressHC_limitedOutput_withStateHC (void* state, const char* source, char* dest, int inputSize, int maxOutputSize)
{ return LZ4_compressHC2_limitedOutput_withStateHC (state, source, dest, inputSize, maxOutputSize, 0); }


/****************************
   Stream functions
****************************/

int LZ4_compressHC_continue (void* LZ4HC_Data, const char* source, char* dest, int inputSize)
{
    return LZ4HC_compress_generic (LZ4HC_Data, source, dest, inputSize, 0, 0, noLimit);
}

int LZ4_compressHC2_continue (void* LZ4HC_Data, const char* source, char* dest, int inputSize, int compressionLevel)
{
    return LZ4HC_compress_generic (LZ4HC_Data, source, dest, inputSize, 0, compressionLevel, noLimit);
}

int LZ4_compressHC_limitedOutput_continue (void* LZ4HC_Data, const char* source, char* dest, int inputSize, int maxOutputSize)
{
    return LZ4HC_compress_generic (LZ4HC_Data, source, dest, inputSize, maxOutputSize, 0, limitedOutput);
}

int LZ4_compressHC2_limitedOutput_continue (void* LZ4HC_Data, const char* source, char* dest, int inputSize, int maxOutputSize, int compressionLevel)
{
    return LZ4HC_compress_generic (LZ4HC_Data, source, dest, inputSize, maxOutputSize, compressionLevel, limitedOutput);
}
