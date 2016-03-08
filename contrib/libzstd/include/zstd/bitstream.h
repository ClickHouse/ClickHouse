/* ******************************************************************
   bitstream
   Part of FSE library
   header file (to include)
   Copyright (C) 2013-2016, Yann Collet.

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
   - Source repository : https://github.com/Cyan4973/FiniteStateEntropy
****************************************************************** */
#ifndef BITSTREAM_H_MODULE
#define BITSTREAM_H_MODULE

#if defined (__cplusplus)
extern "C" {
#endif


/*
*  This API consists of small unitary functions, which highly benefit from being inlined.
*  Since link-time-optimization is not available for all compilers,
*  these functions are defined into a .h to be included.
*/

/*-****************************************
*  Dependencies
******************************************/
#include "mem.h"            /* unaligned access routines */
#include "error_private.h"  /* error codes and messages */


/*-******************************************
*  bitStream encoding API (write forward)
********************************************/
/*!
* bitStream can mix input from multiple sources.
* A critical property of these streams is that they encode and decode in **reverse** direction.
* So the first bit sequence you add will be the last to be read, like a LIFO stack.
*/
typedef struct
{
    size_t bitContainer;
    int    bitPos;
    char*  startPtr;
    char*  ptr;
    char*  endPtr;
} BIT_CStream_t;

MEM_STATIC size_t BIT_initCStream(BIT_CStream_t* bitC, void* dstBuffer, size_t dstCapacity);
MEM_STATIC void   BIT_addBits(BIT_CStream_t* bitC, size_t value, unsigned nbBits);
MEM_STATIC void   BIT_flushBits(BIT_CStream_t* bitC);
MEM_STATIC size_t BIT_closeCStream(BIT_CStream_t* bitC);

/*!
* Start by initCStream, providing the size of buffer to write into.
* bitStream will never write outside of this buffer.
* @dstCapacity must be >= sizeof(size_t), otherwise @return will be an error code.
*
* bits are first added to a local register.
* Local register is size_t, hence 64-bits on 64-bits systems, or 32-bits on 32-bits systems.
* Writing data into memory is an explicit operation, performed by the flushBits function.
* Hence keep track how many bits are potentially stored into local register to avoid register overflow.
* After a flushBits, a maximum of 7 bits might still be stored into local register.
*
* Avoid storing elements of more than 24 bits if you want compatibility with 32-bits bitstream readers.
*
* Last operation is to close the bitStream.
* The function returns the final size of CStream in bytes.
* If data couldn't fit into @dstBuffer, it will return a 0 ( == not storable)
*/


/*-********************************************
*  bitStream decoding API (read backward)
**********************************************/
typedef struct
{
    size_t   bitContainer;
    unsigned bitsConsumed;
    const char* ptr;
    const char* start;
} BIT_DStream_t;

typedef enum { BIT_DStream_unfinished = 0,
               BIT_DStream_endOfBuffer = 1,
               BIT_DStream_completed = 2,
               BIT_DStream_overflow = 3 } BIT_DStream_status;  /* result of BIT_reloadDStream() */
               /* 1,2,4,8 would be better for bitmap combinations, but slows down performance a bit ... :( */

MEM_STATIC size_t   BIT_initDStream(BIT_DStream_t* bitD, const void* srcBuffer, size_t srcSize);
MEM_STATIC size_t   BIT_readBits(BIT_DStream_t* bitD, unsigned nbBits);
MEM_STATIC BIT_DStream_status BIT_reloadDStream(BIT_DStream_t* bitD);
MEM_STATIC unsigned BIT_endOfDStream(const BIT_DStream_t* bitD);


/*!
* Start by invoking BIT_initDStream().
* A chunk of the bitStream is then stored into a local register.
* Local register size is 64-bits on 64-bits systems, 32-bits on 32-bits systems (size_t).
* You can then retrieve bitFields stored into the local register, **in reverse order**.
* Local register is explicitly reloaded from memory by the BIT_reloadDStream() method.
* A reload guarantee a minimum of ((8*sizeof(size_t))-7) bits when its result is BIT_DStream_unfinished.
* Otherwise, it can be less than that, so proceed accordingly.
* Checking if DStream has reached its end can be performed with BIT_endOfDStream()
*/


/*-****************************************
*  unsafe API
******************************************/
MEM_STATIC void BIT_addBitsFast(BIT_CStream_t* bitC, size_t value, unsigned nbBits);
/* faster, but works only if value is "clean", meaning all high bits above nbBits are 0 */

MEM_STATIC void BIT_flushBitsFast(BIT_CStream_t* bitC);
/* unsafe version; does not check buffer overflow */

MEM_STATIC size_t BIT_readBitsFast(BIT_DStream_t* bitD, unsigned nbBits);
/* faster, but works only if nbBits >= 1 */



/*-**************************************************************
*  Helper functions
****************************************************************/
MEM_STATIC unsigned BIT_highbit32 (register U32 val)
{
#   if defined(_MSC_VER)   /* Visual */
    unsigned long r=0;
    _BitScanReverse ( &r, val );
    return (unsigned) r;
#   elif defined(__GNUC__) && (__GNUC__ >= 3)   /* Use GCC Intrinsic */
    return 31 - __builtin_clz (val);
#   else   /* Software version */
    static const unsigned DeBruijnClz[32] = { 0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30, 8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31 };
    U32 v = val;
    unsigned r;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    r = DeBruijnClz[ (U32) (v * 0x07C4ACDDU) >> 27];
    return r;
#   endif
}


/*-**************************************************************
*  bitStream encoding
****************************************************************/
MEM_STATIC size_t BIT_initCStream(BIT_CStream_t* bitC, void* startPtr, size_t maxSize)
{
    bitC->bitContainer = 0;
    bitC->bitPos = 0;
    bitC->startPtr = (char*)startPtr;
    bitC->ptr = bitC->startPtr;
    bitC->endPtr = bitC->startPtr + maxSize - sizeof(bitC->ptr);
    if (maxSize < sizeof(bitC->ptr)) return ERROR(dstSize_tooSmall);
    return 0;
}

MEM_STATIC void BIT_addBits(BIT_CStream_t* bitC, size_t value, unsigned nbBits)
{
    static const unsigned mask[] = { 0, 1, 3, 7, 0xF, 0x1F, 0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF, 0x1FFF, 0x3FFF, 0x7FFF, 0xFFFF, 0x1FFFF, 0x3FFFF, 0x7FFFF, 0xFFFFF, 0x1FFFFF, 0x3FFFFF, 0x7FFFFF,  0xFFFFFF, 0x1FFFFFF };   /* up to 25 bits */
    bitC->bitContainer |= (value & mask[nbBits]) << bitC->bitPos;
    bitC->bitPos += nbBits;
}

/*! BIT_addBitsFast
 *  works only if `value` is _clean_, meaning all high bits above nbBits are 0 */
MEM_STATIC void BIT_addBitsFast(BIT_CStream_t* bitC, size_t value, unsigned nbBits)
{
    bitC->bitContainer |= value << bitC->bitPos;
    bitC->bitPos += nbBits;
}

/*! BIT_flushBitsFast
 *  unsafe version; does not check buffer overflow */
MEM_STATIC void BIT_flushBitsFast(BIT_CStream_t* bitC)
{
    size_t nbBytes = bitC->bitPos >> 3;
    MEM_writeLEST(bitC->ptr, bitC->bitContainer);
    bitC->ptr += nbBytes;
    bitC->bitPos &= 7;
    bitC->bitContainer >>= nbBytes*8;   /* if bitPos >= sizeof(bitContainer)*8 --> undefined behavior */
}

MEM_STATIC void BIT_flushBits(BIT_CStream_t* bitC)
{
    size_t nbBytes = bitC->bitPos >> 3;
    MEM_writeLEST(bitC->ptr, bitC->bitContainer);
    bitC->ptr += nbBytes;
    if (bitC->ptr > bitC->endPtr) bitC->ptr = bitC->endPtr;
    bitC->bitPos &= 7;
    bitC->bitContainer >>= nbBytes*8;   /* if bitPos >= sizeof(bitContainer)*8 --> undefined behavior */
}

/*! BIT_closeCStream
 *  @result : size of CStream, in bytes, or 0 if it cannot fit into dstBuffer */
MEM_STATIC size_t BIT_closeCStream(BIT_CStream_t* bitC)
{
    char* endPtr;

    BIT_addBitsFast(bitC, 1, 1);   /* endMark */
    BIT_flushBits(bitC);

    if (bitC->ptr >= bitC->endPtr) /* too close to buffer's end */
        return 0;   /* not storable */

    endPtr = bitC->ptr;
    endPtr += bitC->bitPos > 0;    /* remaining bits (incomplete byte) */

    return (endPtr - bitC->startPtr);
}


/*-********************************************************
* bitStream decoding
**********************************************************/
/*!BIT_initDStream
*  Initialize a BIT_DStream_t.
*  @bitD : a pointer to an already allocated BIT_DStream_t structure
*  @srcBuffer must point at the beginning of a bitStream
*  @srcSize must be the exact size of the bitStream
*  @result : size of stream (== srcSize) or an errorCode if a problem is detected
*/
MEM_STATIC size_t BIT_initDStream(BIT_DStream_t* bitD, const void* srcBuffer, size_t srcSize)
{
    if (srcSize < 1) { memset(bitD, 0, sizeof(*bitD)); return ERROR(srcSize_wrong); }

    if (srcSize >=  sizeof(size_t)) {  /* normal case */
        U32 contain32;
        bitD->start = (const char*)srcBuffer;
        bitD->ptr   = (const char*)srcBuffer + srcSize - sizeof(size_t);
        bitD->bitContainer = MEM_readLEST(bitD->ptr);
        contain32 = ((const BYTE*)srcBuffer)[srcSize-1];
        if (contain32 == 0) return ERROR(GENERIC);   /* endMark not present */
        bitD->bitsConsumed = 8 - BIT_highbit32(contain32);
    } else {
        U32 contain32;
        bitD->start = (const char*)srcBuffer;
        bitD->ptr   = bitD->start;
        bitD->bitContainer = *(const BYTE*)(bitD->start);
        switch(srcSize)
        {
            case 7: bitD->bitContainer += (size_t)(((const BYTE*)(bitD->start))[6]) << (sizeof(size_t)*8 - 16);
            case 6: bitD->bitContainer += (size_t)(((const BYTE*)(bitD->start))[5]) << (sizeof(size_t)*8 - 24);
            case 5: bitD->bitContainer += (size_t)(((const BYTE*)(bitD->start))[4]) << (sizeof(size_t)*8 - 32);
            case 4: bitD->bitContainer += (size_t)(((const BYTE*)(bitD->start))[3]) << 24;
            case 3: bitD->bitContainer += (size_t)(((const BYTE*)(bitD->start))[2]) << 16;
            case 2: bitD->bitContainer += (size_t)(((const BYTE*)(bitD->start))[1]) <<  8;
            default:;
        }
        contain32 = ((const BYTE*)srcBuffer)[srcSize-1];
        if (contain32 == 0) return ERROR(GENERIC);   /* endMark not present */
        bitD->bitsConsumed = 8 - BIT_highbit32(contain32);
        bitD->bitsConsumed += (U32)(sizeof(size_t) - srcSize)*8;
    }

    return srcSize;
}

/*!BIT_lookBits
 * Provides next n bits from local register
 * local register is not modified (bits are still present for next read/look)
 * On 32-bits, maxNbBits==25
 * On 64-bits, maxNbBits==57
 * @return : value extracted
 */
MEM_STATIC size_t BIT_lookBits(BIT_DStream_t* bitD, U32 nbBits)
{
    const U32 bitMask = sizeof(bitD->bitContainer)*8 - 1;
    return ((bitD->bitContainer << (bitD->bitsConsumed & bitMask)) >> 1) >> ((bitMask-nbBits) & bitMask);
}

/*! BIT_lookBitsFast :
*   unsafe version; only works only if nbBits >= 1 */
MEM_STATIC size_t BIT_lookBitsFast(BIT_DStream_t* bitD, U32 nbBits)
{
    const U32 bitMask = sizeof(bitD->bitContainer)*8 - 1;
    return (bitD->bitContainer << (bitD->bitsConsumed & bitMask)) >> (((bitMask+1)-nbBits) & bitMask);
}

MEM_STATIC void BIT_skipBits(BIT_DStream_t* bitD, U32 nbBits)
{
    bitD->bitsConsumed += nbBits;
}

/*!BIT_readBits
 * Read next n bits from local register.
 * pay attention to not read more than nbBits contained into local register.
 * @return : extracted value.
 */
MEM_STATIC size_t BIT_readBits(BIT_DStream_t* bitD, U32 nbBits)
{
    size_t value = BIT_lookBits(bitD, nbBits);
    BIT_skipBits(bitD, nbBits);
    return value;
}

/*!BIT_readBitsFast :
*  unsafe version; only works only if nbBits >= 1 */
MEM_STATIC size_t BIT_readBitsFast(BIT_DStream_t* bitD, U32 nbBits)
{
    size_t value = BIT_lookBitsFast(bitD, nbBits);
    BIT_skipBits(bitD, nbBits);
    return value;
}

MEM_STATIC BIT_DStream_status BIT_reloadDStream(BIT_DStream_t* bitD)
{
	if (bitD->bitsConsumed > (sizeof(bitD->bitContainer)*8))  /* should never happen */
		return BIT_DStream_overflow;

    if (bitD->ptr >= bitD->start + sizeof(bitD->bitContainer)) {
        bitD->ptr -= bitD->bitsConsumed >> 3;
        bitD->bitsConsumed &= 7;
        bitD->bitContainer = MEM_readLEST(bitD->ptr);
        return BIT_DStream_unfinished;
    }
    if (bitD->ptr == bitD->start) {
        if (bitD->bitsConsumed < sizeof(bitD->bitContainer)*8) return BIT_DStream_endOfBuffer;
        return BIT_DStream_completed;
    }
    {
        U32 nbBytes = bitD->bitsConsumed >> 3;
        BIT_DStream_status result = BIT_DStream_unfinished;
        if (bitD->ptr - nbBytes < bitD->start) {
            nbBytes = (U32)(bitD->ptr - bitD->start);  /* ptr > start */
            result = BIT_DStream_endOfBuffer;
        }
        bitD->ptr -= nbBytes;
        bitD->bitsConsumed -= nbBytes*8;
        bitD->bitContainer = MEM_readLEST(bitD->ptr);   /* reminder : srcSize > sizeof(bitD) */
        return result;
    }
}

/*! BIT_endOfDStream
*   @return Tells if DStream has reached its exact end
*/
MEM_STATIC unsigned BIT_endOfDStream(const BIT_DStream_t* DStream)
{
    return ((DStream->ptr == DStream->start) && (DStream->bitsConsumed == sizeof(DStream->bitContainer)*8));
}

#if defined (__cplusplus)
}
#endif

#endif /* BITSTREAM_H_MODULE */
