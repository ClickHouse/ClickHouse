/*
    datagen.c - compressible data generator test tool
    Copyright (C) Yann Collet 2012-2016

    GPL v2 License

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

    You can contact the author at :
       - Lizard source repository : https://github.com/inikep/lizard
*/

/**************************************
*  Includes
**************************************/
#include "platform.h"  /* Compiler options, SET_BINARY_MODE */
#include "util.h"      /* U32 */
#include <stdlib.h>    /* malloc */
#include <stdio.h>     /* FILE, fwrite */
#include <string.h>    /* memcpy */


/**************************************
*  Constants
**************************************/
#define KB *(1 <<10)

#define PRIME1   2654435761U
#define PRIME2   2246822519U


/**************************************
*  Local types
**************************************/
#define LTLOG 13
#define LTSIZE (1<<LTLOG)
#define LTMASK (LTSIZE-1)
typedef BYTE litDistribTable[LTSIZE];



/*********************************************************
*  Local Functions
*********************************************************/
#define MIN(a,b)   ( (a) < (b) ? (a) :(b) )
#define RDG_rotl32(x,r) ((x << r) | (x >> (32 - r)))
static unsigned int RDG_rand(U32* src)
{
    U32 rand32 = *src;
    rand32 *= PRIME1;
    rand32 ^= PRIME2;
    rand32  = RDG_rotl32(rand32, 13);
    *src = rand32;
    return rand32;
}


static void RDG_fillLiteralDistrib(litDistribTable lt, double ld)
{
    BYTE const firstChar = ld <= 0.0 ? 0 : '(';
    BYTE const lastChar  = ld <= 0.0 ? 255 : '}';
    BYTE character = ld <= 0.0 ? 0 : '0';
    U32 u = 0;

    while (u<LTSIZE) {
        U32 const weight = (U32)((double)(LTSIZE - u) * ld) + 1;
        U32 const end = MIN(u+weight, LTSIZE);
        while (u < end) lt[u++] = character;
        character++;
        if (character > lastChar) character = firstChar;
    }
}


static BYTE RDG_genChar(U32* seed, const litDistribTable lt)
{
    U32 id = RDG_rand(seed) & LTMASK;
    return (lt[id]);
}


#define RDG_DICTSIZE    (32 KB)
#define RDG_RAND15BITS  ((RDG_rand(seed) >> 3) & 32767)
#define RDG_RANDLENGTH  ( ((RDG_rand(seed) >> 7) & 7) ? (RDG_rand(seed) & 15) : (RDG_rand(seed) & 511) + 15)
void RDG_genBlock(void* buffer, size_t buffSize, size_t prefixSize, double matchProba, litDistribTable lt, unsigned* seedPtr)
{
    BYTE* buffPtr = (BYTE*)buffer;
    const U32 matchProba32 = (U32)(32768 * matchProba);
    size_t pos = prefixSize;
    U32* seed = seedPtr;

    /* special case */
    while (matchProba >= 1.0)
    {
        size_t size0 = RDG_rand(seed) & 3;
        size0  = (size_t)1 << (16 + size0 * 2);
        size0 += RDG_rand(seed) & (size0-1);   /* because size0 is power of 2*/
        if (buffSize < pos + size0)
        {
            memset(buffPtr+pos, 0, buffSize-pos);
            return;
        }
        memset(buffPtr+pos, 0, size0);
        pos += size0;
        buffPtr[pos-1] = RDG_genChar(seed, lt);
    }

    /* init */
    if (pos==0) buffPtr[0] = RDG_genChar(seed, lt), pos=1;

    /* Generate compressible data */
    while (pos < buffSize)
    {
        /* Select : Literal (char) or Match (within 32K) */
        if (RDG_RAND15BITS < matchProba32)
        {
            /* Copy (within 32K) */
            size_t match;
            size_t d;
            int length = RDG_RANDLENGTH + 4;
            U32 offset = RDG_RAND15BITS + 1;
            if (offset > pos) offset = (U32)pos;
            match = pos - offset;
            d = pos + length;
            if (d > buffSize) d = buffSize;
            while (pos < d) buffPtr[pos++] = buffPtr[match++];
        }
        else
        {
            /* Literal (noise) */
            size_t d;
            size_t length = RDG_RANDLENGTH;
            d = pos + length;
            if (d > buffSize) d = buffSize;
            while (pos < d) buffPtr[pos++] = RDG_genChar(seed, lt);
        }
    }
}


void RDG_genBuffer(void* buffer, size_t size, double matchProba, double litProba, unsigned seed)
{
    litDistribTable lt;
    if (litProba==0.0) litProba = matchProba / 4.5;
    RDG_fillLiteralDistrib(lt, litProba);
    RDG_genBlock(buffer, size, 0, matchProba, lt, &seed);
}


#define RDG_BLOCKSIZE (128 KB)
void RDG_genOut(unsigned long long size, double matchProba, double litProba, unsigned seed)
{
    BYTE buff[RDG_DICTSIZE + RDG_BLOCKSIZE];
    U64 total = 0;
    size_t genBlockSize = RDG_BLOCKSIZE;
    litDistribTable lt;

    /* init */
    if (litProba==0.0) litProba = matchProba / 4.5;
    RDG_fillLiteralDistrib(lt, litProba);
    SET_BINARY_MODE(stdout);

    /* Generate dict */
    RDG_genBlock(buff, RDG_DICTSIZE, 0, matchProba, lt, &seed);

    /* Generate compressible data */
    while (total < size)
    {
        RDG_genBlock(buff, RDG_DICTSIZE+RDG_BLOCKSIZE, RDG_DICTSIZE, matchProba, lt, &seed);
        if (size-total < RDG_BLOCKSIZE) genBlockSize = (size_t)(size-total);
        total += genBlockSize;
        fwrite(buff, 1, genBlockSize, stdout);
        /* update dict */
        memcpy(buff, buff + RDG_BLOCKSIZE, RDG_DICTSIZE);
    }
}
